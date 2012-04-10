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

  def execute(transaction: Transaction):Seq[Value] = {
    val context = this.analyseTransaction(transaction)

    // TODO: support multiple token on read transactions
    if (context.tokens.size != 1)
      throw new ExecutionException("Only single destination transaction are supported right now.")

    // TODO: split write and read transactions so that write gets through the consistency manager
    // TODO: support for protocol translator

    // make sure we don't have any non serializable value
    transaction.reset()

    // TODO: replace by future?
    var ret:Seq[Value] = null
    val notif = new java.lang.Object
    remoteExecuteToken.call("token"->context.tokens(0), "trx" -> transaction)(resp => {
      ret = resp("values").asInstanceOf[Seq[Value]]

      notif.synchronized {
       notif.notify()
      }
    })

    notif.synchronized {
      notif.wait(10000)
    }

    ret
  }

  private val remoteExecuteToken = this.bind(new Action("/execute/:token", req => {

    println("Got call")
    var error:String = ""
    var values:Seq[Value] = null

    var context = new ExecutionContext(storages)

    try {
      val transaction = req("trx").asInstanceOf[Transaction]
      transaction.execute(context)
      values = context.returnValues
      context.commit()

    } catch {
      case e:Exception => {
        error = e.getMessage
        context.rollback()
      }
    }

    req.reply(
      "error" -> error,
      "values" -> values
    )
  }))


  def registerStorage(storage:Storage) {
    this.storages += (storage.name -> storage)
  }

  def getStorage(name:String) = this.storages.get(name).get
}
