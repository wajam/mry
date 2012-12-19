package com.wajam.mry.storage.mysql

import com.yammer.metrics.scala.Instrumented
import com.wajam.mry.execution.Implicits._
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.Help
import com.wajam.mry.Database
import com.wajam.nrv.cluster.{StaticClusterManager, LocalNode, Cluster}
import com.wajam.scn.{Scn, ScnConfig}
import com.wajam.scn.storage.StorageType
import com.wajam.scn.client.{ScnClientConfig, ScnClient}
import com.wajam.nrv.utils.Future
import com.wajam.nrv.service.Switchboard
import com.wajam.nrv.zookeeper.cluster.ZookeeperClusterManager
import com.wajam.nrv.zookeeper.ZookeeperClient

/**
 * MySQL storage benchmarking tool
 *
 * TODO: implement distributed testing, only local is supported
 */
object Benchmark extends App with Instrumented {
  val inserts = metrics.timer("insert")
  val gets = metrics.timer("gets")


  object Conf extends ScallopConf(args) {
    banner(
      """Usage: Benchmark [OPTIONS]
        |Benchmarks the MRY MySQL storage
        |Examples: ...Benchmark
        | """.stripMargin)

    val local = opt[Boolean]("local", default = Some(true),
      descr = "benchmark in local mode")

    val distributedZookeeper = opt[String]("distributed-zookeeper",
      descr = "benchmark in distributed mode using given zookeeper server for cluster discovery (ex: --distributed-zookeeper zook1.example.com/root)")

    mutuallyExclusive(local, distributedZookeeper)


    val benchmarkNode = opt[Boolean]("benchmark-node", default = Some(true),
      descr = "start a benchmark node (only option for local)")

    val nodeMode = opt[Boolean]("data-node", default = Some(false),
      descr = "start a data node (only receives data and calls)")

    mutuallyExclusive(benchmarkNode, nodeMode)


    val gcDisabled = opt[Boolean]("gc-disable", default = Some(false),
      descr = "don't garbage collect")

    val gcTokenStep = opt[Int]("gc-token-step", default = Some(100000),
      descr = "token range step used to find records to collect")

    var setThreads = opt[Int]("set-threads", default = Some(5),
      descr = "how many threads in parallel we use to set")

    var setCount = opt[Int]("set-count", default = Some(10000),
      descr = "how many records to create")

    var noNukeModel = opt[Boolean]("no-nuke", default = Some(false),
      descr = "don't nuke the model")

    var getThreads = opt[Int]("get-threads", default = Some(5),
      descr = "how many threads in parallel we use to get")

    var getCount = opt[Int]("get-count", default = Some(10000),
      descr = "how many records to get")

    var seed = opt[Long]("seed", default = Some(529948800),
      descr = "seed to use in random generator")

    var keys = opt[Int]("keys", default = Some(1000),
      descr = "numbers of different keys to set")


    var mysqlHost = opt[String]("mysql-host", default = Some("127.0.0.1"),
      descr = "host of the mysql server")

    var mysqlDatabase = opt[String]("mysql-database", default = Some("mry"),
      descr = "database of the mysql server")

    var mysqlUser = opt[String]("mysql-user", default = Some("mry"),
      descr = "username of the mysql server")

    var mysqlPassword = opt[String]("mysql-password", default = Some("mry"),
      descr = "password of the mysql server")


    override protected def onError(e: Throwable) = e match {
      case Help =>
        builder.printHelp
        sys.exit(0)
      case _ =>
        println("Error: %s".format(e.getMessage))
        println()
        builder.printHelp
        sys.exit(1)
    }

    verify
  }

  val db: Database = if (Conf.local()) {
    val node = new LocalNode(Map("nrv" -> 1234))
    val cluster = new Cluster(node, new StaticClusterManager())

    val scn = new Scn("scn", ScnConfig(), StorageType.MEMORY)
    val scnClient = new ScnClient(scn, ScnClientConfig()).start()
    cluster.registerService(scn)
    scn.addMember(0, cluster.localNode)

    val db = new Database(scn = scnClient)
    db.applySupport(switchboard = Some(new Switchboard("mry", 10, 50)))
    cluster.registerService(db)
    db.addMember(0, cluster.localNode)

    val mysql = new MysqlStorage(MysqlStorageConfiguration("mysql", Conf.mysqlHost(), Conf.mysqlDatabase(),
      Conf.mysqlUser(), Conf.mysqlPassword(), gcTokenStep = Conf.gcTokenStep()), garbageCollection = !Conf.gcDisabled())
    val model = new Model
    val table1 = model.addTable(new Table("table1"))
    val table1_1 = table1.addTable(new Table("table1_1"))
    table1_1.addTable(new Table("table1_1_1"))
    val table2 = model.addTable(new Table("table2"))
    val table2_1 = table2.addTable(new Table("table2_1"))
    table2_1.addTable(new Table("table2_1_1"))

    if (!Conf.noNukeModel()) {
      mysql.nuke()
    }

    mysql.syncModel(model)
    db.registerStorage(mysql)

    cluster.start()

    db

  } else {
    val zkClient = new ZookeeperClient(Conf.distributedZookeeper())
    val node = new LocalNode(Map("nrv" -> 1234))
    val cluster = new Cluster(node, new ZookeeperClusterManager(zkClient))


    // TODO: implement distributed testing
    new Database(scn = null)
  }

  def getRandomGenerator(id: Int = 0) = new Object {
    val rand = new util.Random(Conf.seed() + id)

    def nextKey() = rand.nextInt(Conf.keys())
  }

  def benchmarkSet(threadId: Int, count: Int) {
    val rand = getRandomGenerator(1 + threadId)
    Iterator.range(0, count).foreach(i => {
      inserts.time {
        try {
          Future.blocking(db.execute(b => {
            val data = rand.rand.nextString(rand.rand.nextInt(2048))
            b.from("mysql").from("table1").set("key%d".format(rand.nextKey()), Map("k" -> data))
          }))
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    })
  }

  def benchmarkGet(threadId: Int, count: Int) {
    val rand = getRandomGenerator(2 + threadId)
    Iterator.range(0, count).foreach(i => {
      try {
        gets.time {
          try {
            Future.blocking(db.execute(b => {
              b.ret(b.from("mysql").from("table1").get("key%d".format(rand.nextKey())))
            }))
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      }
    })
  }

  if (Conf.benchmarkNode()) {
    Iterable.range(0, Conf.setThreads()).foreach(i => {
      Future {
        benchmarkSet(i, Conf.setCount())
      }
    })

    Iterable.range(0, Conf.getThreads()).foreach(i => {
      Future {
        benchmarkGet(i, Conf.getCount())
      }
    })
  }

  Thread.currentThread.join()
}
