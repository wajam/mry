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
import com.wajam.scn.client.{ScnClientConfig, ScnClient}
import com.wajam.scn.{ScnConfig, Scn}
import com.wajam.scn.storage.StorageType
import com.wajam.nrv.tracing.Tracer
import java.util.UUID
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.zookeeper.cluster.ZookeeperTestingClusterDriver
import com.wajam.nrv.consistency.ConsistencyMasterSlave
import com.wajam.nrv.scribe.ScribeTraceRecorder

@RunWith(classOf[JUnitRunner])
class TestDatabase extends FunSuite with BeforeAndAfterAll {

  def createClusterInstance(size: Int, i: Int, manager: ClusterManager): TestingClusterInstance = {

    val tracer = new Tracer(new ScribeTraceRecorder("127.0.0.1", 1463, 1))
    val node = new LocalNode("127.0.0.1", Map("nrv" -> (50000 + 10 * i), "mry" -> (50001 + 10 * i), "scn" -> (50002 + 10 * i)))
    val cluster = new Cluster(node, manager/*, new ActionSupportOptions(tracer = Option(tracer))*/)

    val token = Resolver.MAX_TOKEN / size * i

    val scn = new Scn("scn", ScnConfig(), StorageType.MEMORY)
    cluster.registerService(scn)
    scn.addMember(token, cluster.localNode)

    val scnClient = new ScnClient(scn, ScnClientConfig(100)).start()
    val consistency = new ConsistencyMasterSlave(scnClient)

    val db = new Database("mry")
    cluster.registerService(db)
    db.registerStorage(new MemoryStorage("memory"))
    db.applySupport(consistency = Some(consistency))
    consistency.bindService(db)

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

          members.map(_.node.uniqueKey) should contain (ret(2).asInstanceOf[StringValue].strValue)
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
}
