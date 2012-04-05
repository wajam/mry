package com.wajam.mry

import com.wajam.nrv.service.{Action, Service}
import execution.Transaction
import storage.Storage

/**
 * MRY database
 */
class Database(var serviceName: String = "database") extends Service(serviceName) {
  var storages = Map[String, Storage]()

  private val remoteExecute = this.bind(new Action("/execute/:token/write", req => {
  }))

  def execute(transaction: Transaction) {
    remoteExecute.call("token"->1234, "trx" -> transaction)()
  }

  def registerStorage(storage:Storage) {
    this.storages += (storage.name -> storage)
  }

  def getStorage(name:String) = this.storages.get(name).get
}
