package com.wajam.mry

import com.wajam.nrv.service.{Action, Service}
import com.wajam.nrv.cluster.Cluster
import execution.Transaction
import storage.Storage

/**
 * MRY database
 */
class Database(var cluster: Cluster, var serviceName: String = "database") {
  val dbService = new Service(serviceName)
  var storages = Map[String, Storage]()

  cluster.addService(dbService)

  private val remoteExecute = dbService.bind(path = "/execute/:token/write", action = new Action(req => {
  }))

  def execute(transaction: Transaction) {
    remoteExecute.call("token"->1234)()
  }

  def registerStorage(storage:Storage) {
    this.storages += (storage.name -> storage)
  }

  def getStorage(name:String) = this.storages.get(name).get
}
