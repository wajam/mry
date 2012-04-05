package com.wajam.mry

import execution.Transaction
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class TestDatabase extends FunSuite with BeforeAndAfterAll {
  val driver = new TestingClusterDriver((n, manager) => {
    val cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> (50000 + 10*n), "mry" -> (50001 + 10*n))), manager)
    val db = cluster.addService(new Database("mry"))
    db.addMember((n*500), cluster.localNode)
    new TestingClusterInstance(cluster, db)
  })

  test("exec") {
    driver.execute((driver, oneInstance) => {
      val db = oneInstance.data.asInstanceOf[Database]

      db.execute(new Transaction())
    })
  }

  override protected def afterAll() {
    driver.destroy()
  }
}
