package com.appaquet.mry

import com.appaquet.nrv.service.{Action, Service}
import com.appaquet.nrv.cluster.Cluster
import execution.Transaction

/**
 * DESCRIPTION HERE
 *
 * Author: Andre-Philippe Paquet < app@quet.ca >
 */

class Database(var cluster: Cluster, var serviceName: String = "database") {
  val dbService = new Service(serviceName)

  cluster.addService(dbService)

  private val remoteExecute = dbService.bind(path = "/execute", action = new Action(req => {

  }))

  def execute(transaction: Transaction) {
  }
}
