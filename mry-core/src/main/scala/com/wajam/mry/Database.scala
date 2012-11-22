package com.wajam.mry

import execution._
import storage.Storage
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.service.{ActionMethod, Resolver, Action, Service}
import com.wajam.scn.client.ScnClient
import com.wajam.scn.Timestamp
import com.wajam.nrv.tracing.Traced
import java.util.concurrent.atomic.AtomicReference
import com.wajam.nrv.data.InMessage


/**
 * MRY database
 */
class Database(var serviceName: String = "database", val scn: ScnClient)
  extends Service(serviceName) with Logging with Instrumented with Traced {

  var storages = Map[String, Storage]()
  def responseTimeout = 1000 //TODO hardcoded while timeout implementation is not in action support

  private val metricExecuteLocal = tracedTimer("execute-local")
  private val lastWriteTimestamp = new AtomicReference[Option[Timestamp]](None)

  def analyseTransaction(transaction: Transaction): ExecutionContext = {
    val context = new ExecutionContext(storages)
    context.dryMode = true
    transaction.execute(context)
    context
  }

  def execute(blockCreator: (Block with OperationApi) => Unit) {
    this.execute(blockCreator, null)
  }

  def execute(blockCreator: (Block with OperationApi) => Unit, ret: ((Seq[Value], Option[Exception]) => Unit)) {
    try {
      this.execute(new Transaction(blockCreator), ret)
    } catch {
      case ex: Exception =>
        debug("Got an exception executing transaction", ex)

        if (ret != null) {
          ret(null, Some(ex))
        }
    }
  }

  def execute(transaction: Transaction, ret: ((Seq[Value], Option[Exception]) => Unit)) {
    try {

      // get token
      val context = this.analyseTransaction(transaction)
      if (context.tokens.size != 1)
        throw new ExecutionException("Only single destination transaction are supported right now.")

      // reset transaction before sending it
      transaction.reset()

      // send transaction to node in charge of that token
      val remoteAction = if (context.isMutation) {
        remoteWriteExecuteToken
      } else {
        remoteReadExecuteToken
      }

      remoteAction.call(Map(Database.TOKEN_KEY -> context.tokens(0), "trx" -> transaction), onReply = (resp, optException) => {
        if (ret != null) {
          if (optException.isEmpty)
            ret(resp.parameters("values").asInstanceOf[Seq[Value]], None)
          else
            ret(Seq(), optException)
        }
      }, responseTimeout = responseTimeout)

    } catch {
      case ex: Exception =>
        debug("Got an exception executing transaction", ex)

        if (ret != null) {
          ret(null, Some(ex))
        }
    }
  }

  def registerStorage(storage: Storage) {
    this.storages += (storage.name -> storage)
    storage.start()
  }

  def getStorage(name: String) = this.storages.get(name).get

  private val remoteWriteExecuteToken = this.registerAction(new Action("/execute/:" + Database.TOKEN_KEY, req => {
    fetchTimestampAndExecute(req)
  }, ActionMethod.POST))
  remoteWriteExecuteToken.applySupport(resolver = Some(Database.TOKEN_RESOLVER))

  private val remoteReadExecuteToken = this.registerAction(new Action("/execute/:" + Database.TOKEN_KEY, req => {
    lastWriteTimestamp.get() match {
      case Some(timestamp) => metricExecuteLocal.time {execute(timestamp, req)}
      case None => fetchTimestampAndExecute(req)
    }
  }, ActionMethod.GET))
  remoteReadExecuteToken.applySupport(resolver = Some(Database.TOKEN_RESOLVER))

  private def fetchTimestampAndExecute(req: InMessage) {
    val timerContext = this.metricExecuteLocal.timerContext()
    scn.fetchTimestamps(serviceName, (timestamps: Seq[Timestamp], optException) => {
      try {
        if (optException.isDefined) {
          info("Exception while fetching timestamps from SCN.", optException.get)
          throw optException.get
        }
        val timestamp = timestamps(0)
        updateLastTimestamp(timestamp)
        execute(timestamp, req)
      } catch {
        case e: Exception => req.replyWithError(e)
      } finally {
        timerContext.stop()
      }
    }, 1, req.token)
  }

  /**
   * Update last timestamp only if greater than the currently saved timestamp. SCN should not give non increasing
   * timestamps, but concurrent execution could be executed in a different order. Since we need to be certain that
   * the last timestamp is effectively the last fetched timestamp, this method must try to set the value as long as
   * either the new value was atomically set or is obsolete.
   */
  private def updateLastTimestamp(timestamp: Timestamp) {
    var updateSuccessful = false
    do {
      val savedTimestamp = lastWriteTimestamp.get()
      updateSuccessful = savedTimestamp match {
        case Some(prevTimestamp) => if (timestamp > prevTimestamp) {
          lastWriteTimestamp.compareAndSet(savedTimestamp, Some(timestamp))
        } else {
          true //the timestamp is less than the last timestamp so our value is obsolete
        }
        case None => lastWriteTimestamp.compareAndSet(savedTimestamp, Some(timestamp))
      }
    } while (!updateSuccessful)
  }

  private def execute(timestamp: Timestamp, req: InMessage) {
    var values: Seq[Value] = null
    val context = new ExecutionContext(storages, Some(timestamp))
    context.cluster = Database.this.cluster

    try {
      val transaction = req.parameters("trx").asInstanceOf[Transaction]
      transaction.execute(context)
      values = context.returnValues
      context.commit()

    } catch {
      case e: Exception => {
        context.rollback()
        throw e
      }
    }

    req.reply(
      Seq("values" -> values)
    )
  }
}

object Database {
  val TOKEN_KEY = "token"
  val TOKEN_RESOLVER = new Resolver(tokenExtractor = Resolver.TOKEN_PARAM(TOKEN_KEY))
}
