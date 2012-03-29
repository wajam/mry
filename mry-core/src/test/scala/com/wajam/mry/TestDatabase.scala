package com.wajam.mry

import org.scalatest.FunSuite
import com.wajam.nrv.cluster.{Node, Cluster}

class TestDatabase extends FunSuite {
  val cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> 12345)))
  val db = new Database(cluster)
  cluster.start()

  ignore("execute") {
  }

  cluster.stop()
}
