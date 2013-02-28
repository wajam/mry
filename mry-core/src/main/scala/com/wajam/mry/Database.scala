package com.wajam.mry

import execution._
import storage.Storage
import com.wajam.nrv.Logging
import com.yammer.metrics.scala.Instrumented
import com.wajam.nrv.service.{ActionMethod, Resolver, Action, Service}
import com.wajam.nrv.tracing.Traced
import com.wajam.nrv.data.{MMigrationCatchAll, Message, InMessage}
import com.wajam.nrv.utils.{Promise, Future}
import com.wajam.nrv.consistency.{Consistency, ConsistentStore}


/**
 * MRY database
 */
class Database(var serviceName: String = "database")
  extends Service(serviceName) with ConsistentStore with Logging with Instrumented with Traced {

  var storages = Map[String, Storage]()

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

      remoteAction.call(Map(Database.TOKEN_KEY -> context.tokens(0), "trx" -> MMigrationCatchAll(transaction)), // TODO: MigrationDuplicate: Remove trx
                        data = transaction,
                        onReply = (resp, optException) => {
        if (ret != null) {
          if (optException.isEmpty)
            if (resp.hasData)
              ret(resp.getData[Seq[Value]], None)
            else
              ret(resp.parameters("values").asInstanceOf[MMigrationCatchAll].value.asInstanceOf[Seq[Value]], None) // TODO: MigrationDuplicate: Remove
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

  protected val remoteWriteExecuteToken = this.registerAction(new Action("/execute/:" + Database.TOKEN_KEY, req => {
    execute(req)
  }, ActionMethod.POST))
  remoteWriteExecuteToken.applySupport(resolver = Some(Database.TOKEN_RESOLVER))

  protected val remoteReadExecuteToken = this.registerAction(new Action("/execute/:" + Database.TOKEN_KEY, req => {
    execute(req)
  }, ActionMethod.GET))
  remoteReadExecuteToken.applySupport(resolver = Some(Database.TOKEN_RESOLVER))

  private def execute(req: InMessage) {
    var values: Seq[Value] = null
    val context = new ExecutionContext(storages, Some(Consistency.getMessageTimestamp(req).get))
    context.cluster = Database.this.cluster

    try {
      val transaction =
        if (req.hasData)
          req.getData[Transaction]
        else
          req.parameters("trx").asInstanceOf[MMigrationCatchAll].value.asInstanceOf[Transaction]  // TODO: MigrationDuplicate: Remove

      transaction.execute(context)
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
      Seq("values" -> MMigrationCatchAll(values)), // TODO: MigrationDuplicate: Remove
      data = values
    )
  }

  def requiresConsistency(message: Message) : Boolean = {
    findAction(message.path, message.method) match {
      case Some(action) => {
        action == remoteWriteExecuteToken || action == remoteReadExecuteToken
      }
      case _ => false
    }
  }
}

object Database {
  val TOKEN_KEY = "token"
  val TOKEN_RESOLVER = new Resolver(tokenExtractor = Resolver.TOKEN_PARAM(TOKEN_KEY))
}
