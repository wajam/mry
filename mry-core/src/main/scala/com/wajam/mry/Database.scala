package com.wajam.mry

import com.wajam.nrv.service.{Action, Service}
import com.wajam.nrv.cluster.Cluster
import execution.Transaction

/**
 * MRY database
 */
class Database(var cluster: Cluster, var serviceName: String = "database") {
  val dbService = new Service(serviceName)

  cluster.addService(dbService)

  private val remoteExecute = dbService.bind(path = "/execute", action = new Action(req => {

  }))

  def execute(transaction: Transaction) {
  }
}
