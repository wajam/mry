package com.wajam.mry

import execution._
import storage.Storage
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.service.{Resolver, Action, Service}
import com.wajam.scn.{ScnClient, Timestamp}
import com.wajam.nrv.tracing.Traced


/**
 * MRY database
 */
class Database(var serviceName: String = "database", val scn: ScnClient)
  extends Service(serviceName) with Logging with Instrumented with Traced {

  var storages = Map[String, Storage]()

  private val metricExecuteLocal = tracedTimer("execute-local")

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
      remoteExecuteToken.call(Map(Database.TOKEN_KEY -> context.tokens(0), "trx" -> transaction), onReply = (resp, optException) => {
        if (ret != null) {
          if (optException.isEmpty)
            ret(resp.parameters("values").asInstanceOf[Seq[Value]], None)
          else
            ret(Seq(), optException)
        }
      })

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


  private val remoteExecuteToken = this.registerAction(new Action("/execute/:" + Database.TOKEN_KEY, req => {
    val timerContext = this.metricExecuteLocal.timerContext()
    scn.fetchTimestamps(serviceName, (timestamps: Seq[Timestamp], optException) => {
      try {
        if (optException.isDefined) {
          info("Exception while fetching timestamps from SCN.", optException.get)
          throw optException.get
        }

        var values: Seq[Value] = null
        val context = new ExecutionContext(storages, Some(timestamps(0)))
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
      } catch {
        case e: Exception => req.replyWithError(e)
      } finally {
         timerContext.stop()
      }
    }, 1)
  }))

  remoteExecuteToken.applySupport(resolver = Some(Database.TOKEN_RESOLVER))
}

object Database {
  val TOKEN_KEY = "token"
  val TOKEN_RESOLVER = new Resolver(tokenExtractor = Resolver.TOKEN_PARAM(TOKEN_KEY))
}
