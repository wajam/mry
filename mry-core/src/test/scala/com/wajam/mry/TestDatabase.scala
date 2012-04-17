package com.wajam.mry

import execution.Value
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster._
import com.wajam.mry.execution.Implicits._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import storage.MemoryStorage
import com.wajam.nrv.utils.Sync

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

      for (i <- 0 to 50) {
        val sync = new Sync[Seq[Value]]
        db.execute(b => {
          b.from("memory").set("value%d".format(i), "key%d".format(i))
        })

        db.execute(b => {
          b.returns(b.from("memory").get("key%d".format(i)))
        }, sync.done(_, _))

        sync.thenWait(ret => {
          assert(ret != null)
          assert(ret.size == 1)
          assert(ret(0).equalsValue("value%d".format(i)))
        }, 3000)
      }
    }, 1, 6)
  }

  override protected def afterAll() {
    driver.destroy()
  }
}
