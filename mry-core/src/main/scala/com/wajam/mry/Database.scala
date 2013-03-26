package com.wajam.mry

import api.MryCodec
import execution._
import storage.Storage
import com.wajam.nrv.{TimeoutException, Logging}
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.service._
import com.wajam.nrv.tracing.Traced
import com.wajam.nrv.data.InMessage
import com.wajam.nrv.utils.{CurrentTime, Promise, Future}
import com.wajam.nrv.consistency.Consistency

/**
 * MRY database
 */
class Database[T <: Storage](serviceName: String = "database")
  extends Service(serviceName) with CurrentTime with Logging with Instrumented with Traced {

  var storages = Map[String, T]()

  // Set specific messageData codec for nrv
  applySupport(nrvCodec = Some(new MryCodec))

  def analyseTransaction(transaction: Transaction): ExecutionContext = {
    val context = new ExecutionContext(storages)
    context.dryMode = true
    transaction.execute(context)
    context
  }

  def execute(blockCreator: (Block with OperationApi) => Unit): Future[Seq[Value]] = {
    val p = Promise[Seq[Value]]
    this.execute(blockCreator(_), (v, optExp) => {
      optExp match {
        case Some(e) => p.failure(e)
        case None => p.success(v)
      }
    })
    p.future
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

      //System.out.println(transaction.printTree("Before call"))

      remoteAction.call(Map(Database.TOKEN_KEY -> context.tokens(0)),
        data = transaction,
        onReply = (resp, optException) => {
          if (ret != null) {
            if (optException.isEmpty)
              ret(resp.getData[Seq[Value]], None)
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

  def registerStorage(storage: T) {
    this.storages += (storage.name -> storage)
    storage.start()
  }

  def getStorage(name: String): T = this.storages.get(name).get

  protected val remoteWriteExecuteToken = this.registerAction(new Action("/execute/:" + Database.TOKEN_KEY, req => {
    execute(req)
  }, ActionMethod.POST))
  remoteWriteExecuteToken.applySupport(resolver = Some(Database.TOKEN_RESOLVER))

  protected val remoteReadExecuteToken = this.registerAction(new Action("/execute/:" + Database.TOKEN_KEY, req => {
    execute(req)
  }, ActionMethod.GET))
  remoteReadExecuteToken.applySupport(resolver = Some(Database.TOKEN_RESOLVER))

  private def transactionTimeout = math.max(responseTimeout * 0.75, responseTimeout - 500)

  private def execute(req: InMessage) {
    var values: Seq[Value] = null
    val context = new ExecutionContext(storages, Consistency.getMessageTimestamp(req))
    context.cluster = Database.this.cluster

    try {
      val transaction = req.getData[Transaction]

      //System.out.println(transaction.printTree("Before execute"))

      val startTime = currentTime
      transaction.execute(context)
      val elapsedTime = currentTime - startTime
      if (elapsedTime > transactionTimeout) {
        throw new TimeoutException("Database transaction took too much time to execute", Some(elapsedTime))
      }
      values = context.returnValues
      context.commit()
      transaction.reset()

    } catch {
      case e: Exception => {
        debug("Got an exception executing transaction", e)
        context.rollback()
        throw e
      }
    }

    req.reply(
      null,
      data = values
    )
  }
}

object Database {
  val TOKEN_KEY = "token"
  val TOKEN_RESOLVER = new Resolver(tokenExtractor = Resolver.TOKEN_PARAM(TOKEN_KEY))
}
