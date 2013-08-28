package com.wajam.mry

import execution.{StringValue, IntValue, ListValue, Value}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.nrv.cluster._
import com.wajam.mry.execution.Implicits._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import storage.{Storage, MemoryStorage}
import com.wajam.nrv.service.Resolver
import com.wajam.nrv.utils.{Future, Promise}
import com.wajam.nrv.tracing.Tracer
import java.util.UUID
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.zookeeper.cluster.ZookeeperTestingClusterDriver
import com.wajam.nrv.scribe.ScribeTraceRecorder
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import com.wajam.nrv.TimeoutException

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

      val p = Promise[Seq[Value]]

      db.execute(b => {
        b.from("memory").set(key, "value%s".format(key))
      })

      db.execute(b => {
        val context = b.from("context")
        b.returns(b.from("memory").get(key), context.get("tokens"), context.get("local_node"))
      }, p.complete(_, _))

      Future.blocking(p.future map {
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
      }, 30000)
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
      val set1 = Promise[Seq[Value]]
      db.execute(b => {
        b.from("memory").set("key", "value1")
        b.returns(b.from("memory").get("key"))
      }, set1.complete(_, _))
      Future.blocking(set1.future, 5000)
      val Some(Right(Seq(set1Value))) = set1.future.value
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
      val set2 = Promise[Seq[Value]]
      db.execute(b => {
        b.from("memory").set("key", "value2")
        b.returns(b.from("memory").get("key"))
      }, set2.complete(_, _))
      evaluating {
        Future.blocking(set2.future, 5000)
      } should produce[TimeoutException]

      // Remove the delay and fetch the current storage value. Should be the inital value since the second
      // transaction should had rolledback
      reset(spyStorage)
      val get = Promise[Seq[Value]]
      db.execute(b => {
        b.returns(b.from("memory").get("key"))
      }, get.complete(_, _))
      Future.blocking(get.future, 5000)
      val Some(Right(Seq(set2Value))) = get.future.value
      assert(set2Value.equalsValue("value1"))
    } finally {
      instance.cluster.stop()
    }
  }
}
