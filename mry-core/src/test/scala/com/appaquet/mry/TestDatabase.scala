package com.appaquet.mry

import org.scalatest.FunSuite
import com.appaquet.nrv.cluster.{Node, Cluster}

/**
 * DESCRIPTION HERE
 *
 * Author: Andre-Philippe Paquet < app@quet.ca >
 */

class TestDatabase extends FunSuite {
  val cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> 12345)))
  val db = new Database(cluster)
  cluster.start()

  test("execute") {
  }

  cluster.stop()
}
