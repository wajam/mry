package com.wajam.mry

import com.wajam.nrv.service.{Action, Service}
import execution._
import storage.Storage
import com.wajam.nrv.data.OutRequest

/**
 * MRY database
 */
class Database(var serviceName: String = "database") extends Service(serviceName) {
  var storages = Map[String, Storage]()

  def analyseTransaction(transaction:Transaction):ExecutionContext = {
    val context = new ExecutionContext(storages)
    context.dryMode = true
    context.timestamp = Timestamp.now
    transaction.execute(context)
    context
  }

  def execute(blockCreator:(Block with OperationApi)=>Unit) { this.execute(blockCreator, null) }

  def execute(blockCreator:(Block with OperationApi)=>Unit, ret:((Seq[Value], Option[Exception])=>Unit)) { this.execute(new Transaction(blockCreator), ret) }

  def execute(transaction: Transaction, ret:((Seq[Value], Option[Exception])=>Unit)) {
    val context = this.analyseTransaction(transaction)

    // TODO: support multiple token on read transactions
    if (context.tokens.size != 1)
      throw new ExecutionException("Only single destination transaction are supported right now.")

    // TODO: split write and read transactions so that write gets through the consistency manager
    // TODO: support for protocol translator

    // reset transaction before sending it
    transaction.reset()

    remoteExecuteToken.call(Map("token"->context.tokens(0), "trx" -> transaction), (resp, exception) => {
      if (ret != null) {
        ret(resp("values").asInstanceOf[Seq[Value]], exception)
      }
    })
  }

  private val remoteExecuteToken = this.registerAction(new Action("/execute/:token", req => {
    var values:Seq[Value] = null

    var context = new ExecutionContext(storages)

    try {
      val transaction = req("trx").asInstanceOf[Transaction]
      transaction.execute(context)
      values = context.returnValues
      context.commit()

    } catch {
      case e:Exception => {
        context.rollback()
        throw e
      }
    }

    req.reply(
      "values" -> values
    )
  }))


  def registerStorage(storage:Storage) {
    this.storages += (storage.name -> storage)
  }

  def getStorage(name:String) = this.storages.get(name).get
}
