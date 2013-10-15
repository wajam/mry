package com.wajam.mry

import execution.{StringValue, IntValue, ListValue}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster._
import com.wajam.mry.execution.Implicits._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import storage.MemoryStorage
import com.wajam.nrv.service.Resolver
import com.wajam.tracing.Tracer
import java.util.UUID
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.zookeeper.cluster.ZookeeperTestingClusterDriver
import com.wajam.nrv.scribe.ScribeTraceRecorder
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import scala.concurrent.Await
import scala.concurrent.duration._
import com.wajam.nrv.TimeoutException
import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class TestDatabase extends FunSuite with BeforeAndAfterAll {

  def createClusterInstance(size: Int, i: Int, manager: ClusterManager): TestingClusterInstance = {

    val tracer = new Tracer(new ScribeTraceRecorder("127.0.0.1", 1463))
    val node = new LocalNode("127.0.0.1", Map("nrv" -> (50000 + 10 * i), "mry" -> (50001 + 10 * i), "scn" -> (50002 + 10 * i)))
    val cluster = new Cluster(node, manager /*, new ActionSupportOptions(tracer = Option(tracer))*/)

    val token = Resolver.MAX_TOKEN / size * i

    val db = new Database("mry")
    cluster.registerService(db)
    db.registerStorage(spy(new MemoryStorage("memory")))

    db.addMember(token, cluster.localNode)

    new TestingClusterInstance(cluster, db)
  }

  def testDatabaseInstance(instance: TestingClusterInstance) {
    val db = instance.data.asInstanceOf[Database]

    for (i <- 0 to 100) {
      val key = UUID.randomUUID().toString

      val s = db.execute(b => {
        b.from("memory").set(key, "value%s".format(key))
      })

      Await.result(s, 2 seconds)

      val f = db.execute(b => {
        val context = b.from("context")
        b.returns(b.from("memory").get(key), context.get("tokens"), context.get("local_node"))
      })

      Await.result(f map {
        case ret =>
          assert(ret != null)
          assert(ret.size == 3)
          assert(ret(0).equalsValue("value%s".format(key)))

          // make sure it has the expected token
          val tok = ret(1).asInstanceOf[ListValue].listValue(0).asInstanceOf[IntValue]
          val expectedToken = Resolver.hashData(key)
          assert(tok.intValue == expectedToken)

          // make sure it's on the expected node
          val members = db.resolveMembers(expectedToken, 1)

          members.map(_.node.uniqueKey) should contain(ret(2).asInstanceOf[StringValue].strValue)
      }, 30.seconds)
    }
  }

  test("database with static cluster") {
    val driver = new TestingClusterDriver((size, i, manager) => createClusterInstance(size, i, manager))
    driver.execute((driver, instance) => testDatabaseInstance(instance), 1, 6)
    driver.destroy()
  }

  ignore("database with dynamic cluster") {
    ZookeeperTestingClusterDriver.cleanupZookeeper()
    val driver = new ZookeeperTestingClusterDriver((size, i, manager) => createClusterInstance(size, i, manager))
    driver.execute((driver, instance) => testDatabaseInstance(instance), 6)
    driver.destroy()
  }

  test("database execute rollback on timeout") {
    val instance = createClusterInstance(1, 1, new StaticClusterManager)

    try {
      val db = instance.data.asInstanceOf[Database]
      db.applySupport(responseTimeout = Some(1000))
      instance.cluster.start()

      // Set a first value
      val set1 = db.execute(b => {
        b.from("memory").set("key", "value1")
        b.returns(b.from("memory").get("key"))
      })
      val Seq(set1Value) = Await.result(set1, 5.seconds)
      assert(set1Value.equalsValue("value1"))

      // Add a artificial delay greater than the service response timeout to storage execution.
      val spyStorage = db.getStorage("memory").asInstanceOf[MemoryStorage]
      when(spyStorage.createStorageTransaction(anyObject())).thenAnswer(new Answer[spyStorage.MemoryTransaction]() {
        def answer(invocation: InvocationOnMock) = {
          Thread.sleep(db.responseTimeout * 2)
          invocation.callRealMethod().asInstanceOf[spyStorage.MemoryTransaction]
        }
      })

      // Set a second value with the delay. Should timeout and the second value should not be set to the storage
      val set2 = db.execute(b => {
        b.from("memory").set("key", "value2")
        b.returns(b.from("memory").get("key"))
      })
      evaluating {
        Await.result(set2, 5.seconds)

      } should produce[TimeoutException]

      // Remove the delay and fetch the current storage value. Should be the inital value since the second
      // transaction should had rolledback
      reset(spyStorage)
      val get = db.execute(b => {
        b.returns(b.from("memory").get("key"))
      })
      val Seq(set2Value) = Await.result(get, 5.seconds)
      assert(set2Value.equalsValue("value1"))
    } finally {
      instance.cluster.stop()
    }
  }
}
