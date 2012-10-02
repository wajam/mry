package com.wajam.mry

import execution.{StringValue, IntValue, ListValue, Value}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster._
import com.wajam.mry.execution.Implicits._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import storage.MemoryStorage
import com.wajam.nrv.service.Resolver
import com.wajam.nrv.utils.{Future, Promise}

@RunWith(classOf[JUnitRunner])
class TestDatabase extends FunSuite with BeforeAndAfterAll {
  val driver = new TestingClusterDriver((size, i, manager) => {
    val cluster = new Cluster(new Node("127.0.0.1", Map("nrv" -> (50000 + 10 * i), "mry" -> (50001 + 10 * i))), manager)

    val db = new Database("mry")
    cluster.registerService(db)
    db.registerStorage(new MemoryStorage("memory"))

    val token = (Int.MaxValue / size) * i
    db.addMember(token, cluster.localNode)

    new TestingClusterInstance(cluster, db)
  })

  test("exec") {
    val obj = new Object

    driver.execute((driver, oneInstance) => {
      val db = oneInstance.data.asInstanceOf[Database]

      for (i <- 0 to 100) {
        val key = "key%d".format(i)

        val p = Promise[Seq[Value]]
        db.execute(b => {
          b.from("memory").set(key, "value%d".format(i))
        })

        db.execute(b => {
          val context = b.from("context")
          b.returns(b.from("memory").get(key), context.get("tokens"), context.get("local_node"))
        }, p.complete(_, _))

        Future.blocking(p.future map {
          case ret =>
            assert(ret != null)
            assert(ret.size == 3)
            assert(ret(0).equalsValue("value%d".format(i)))

            // make sure it has the expected token
            val tok = ret(1).asInstanceOf[ListValue].listValue(0).asInstanceOf[IntValue]
            val expectedToken = Resolver.hashData(key)
            assert(tok.intValue == expectedToken)

            // make sure it's on the expected node
            val members = db.resolveMembers(expectedToken, 1)
            assert(members(0).node.uniqueKey == ret(2).asInstanceOf[StringValue].strValue, "%s!=%s".format(members(0).node.uniqueKey, ret(2).asInstanceOf[StringValue].strValue))
        })
      }
    }, 1, 6)
  }

  override protected def afterAll() {
    driver.destroy()
  }
}
