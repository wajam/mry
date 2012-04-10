package com.wajam.mry

import execution.Operation.From
import execution.{Operation, Transaction}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster._
import com.wajam.mry.execution.Implicits._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.wajam.nrv.codec.JavaSerializeCodec
import com.wajam.nrv.data.OutRequest
import storage.MemoryStorage

@RunWith(classOf[JUnitRunner])
class TestDatabase extends FunSuite with BeforeAndAfterAll {
  val driver = new TestingClusterDriver((n, manager) => {
    val cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> (50000 + 10 * n), "mry" -> (50001 + 10 * n))), manager)

    val db = new Database("mry")
    cluster.addService(db)
    db.registerStorage(new MemoryStorage("memory"))
    db.addMember((n * 500), cluster.localNode)

    new TestingClusterInstance(cluster, db)
  })

  test("exec") {
    val obj = new Object

    driver.execute((driver, oneInstance) => {
      val db = oneInstance.data.asInstanceOf[Database]
      db.execute(new Transaction(b => {
        b.from("memory").set("value1", "key1")
      }))

      val ret = db.execute(new Transaction(b => {
        b.ret(b.from("memory").get("key1"))
      }))

      assert(ret.size == 1)
      assert(ret(0).equalsValue("value1"))

    }, 1, 1)
  }

  override protected def afterAll() {
    driver.destroy()
  }
}
