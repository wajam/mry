package com.wajam.mry

import execution._
import storage.Storage
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.service.{Resolver, Action, Service}

/**
 * MRY database
 */
class Database(var serviceName: String = "database") extends Service(serviceName) with Logging with Instrumented {
  var storages = Map[String, Storage]()

  private val metricExecuteLocal = metrics.timer("execute-local")

  def analyseTransaction(transaction: Transaction): ExecutionContext = {
    val context = new ExecutionContext(storages)
    context.dryMode = true
    context.timestamp = Timestamp.now
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
      remoteExecuteToken.call(Map("token" -> context.tokens(0), "trx" -> transaction), onReply = (resp, optException) => {
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


  private val remoteExecuteToken = this.registerAction(new Action("/execute/:token", req => {
    this.metricExecuteLocal.time {
      var values: Seq[Value] = null
      val context = new ExecutionContext(storages)
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
  }))

  remoteExecuteToken.applySupport(resolver = Some(Database.tokenResolver))
}

object Database {
  val tokenKey = "token"
  val tokenResolver = new Resolver(tokenExtractor = Resolver.TOKEN_PARAM(tokenKey))
}
