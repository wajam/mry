package com.wajam.mry.storage.mysql

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.wajam.nrv.Logging
import java.sql.{PreparedStatement, ResultSet, SQLException, Connection}
import com.wajam.mry.execution._
import com.wajam.mry.api.protobuf.ProtobufTranslator
import com.wajam.mry.storage._
import com.yammer.metrics.scala.Instrumented
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import collection.mutable
import java.util.concurrent.{TimeUnit, ScheduledThreadPoolExecutor}

/**
 * MySQL backed storage
 */
class MysqlStorage(name: String, host: String, database: String, username: String, password: String, garbageCollection: Boolean = true) extends Storage(name) with Logging with Value with Instrumented {
  var model: Model = new Model
  var mutationsCount = new AtomicInteger(0)
  val valueSerializer = new ProtobufTranslator

  val datasource = new ComboPooledDataSource()
  datasource.setDriverClass("com.mysql.jdbc.Driver")
  datasource.setJdbcUrl(String.format("jdbc:mysql://%s/%s?zeroDateTimeBehavior=convertToNull", host, database))
  datasource.setUser(username)
  datasource.setPassword(password)

  def getStorageTransaction(context: ExecutionContext) = this.getStorageTransaction

  def getStorageTransaction = new MysqlTransaction(this)

  def returnTransaction(trx: MysqlTransaction) {
    this.mutationsCount.getAndAdd(trx.mutationsCount)
  }

  def getStorageValue(context: ExecutionContext): Value = this

  def syncModel(model: Model, deleteOld: Boolean = false) {
    this.model = model

    val mysqlTables = this.getTables
    var modelTablesNameMap = Map[String, Boolean]()

    val allTables = model.allHierarchyTables
    for (table <- allTables) {
      val fullName = table.depthName("_")
      modelTablesNameMap += (fullName -> true)

      if (!mysqlTables.contains(fullName)) {
        createTable(table, fullName)
      }
    }

    if (deleteOld) {
      for (table <- mysqlTables) {
        if (!modelTablesNameMap.contains(table))
          this.dropTable(table)
      }
    }
  }

  def getConnection = datasource.getConnection

  def nuke() {
    val tables = this.getTables

    for (table <- tables) {
      this.dropTable(table)
    }
  }

  def start() {
    assert(this.model != null, "No model has been synced")

    if (garbageCollection) {
      GarbageCollector.start()
    }
  }

  def stop() {
    GarbageCollector.kill()
    this.datasource.close()
  }

  @throws(classOf[SQLException])
  def executeSql(connection: Connection, update: Boolean, sql: String, params: Any*): SqlResults = {
    val results = new SqlResults

    try {
      results.statement = connection.prepareStatement(sql)

      var p = 0
      for (param <- params) {
        p += 1
        results.statement.setObject(p, param)
      }

      if (update)
        results.statement.executeUpdate()
      else
        results.resultset = results.statement.executeQuery()


      results

    } catch {
      case e: Exception => {
        error("Couldn't execute SQL query {}", sql, e)
        results.close()
        throw e
      }
    }
  }

  private def getTables: Seq[String] = {
    var results: SqlResults = null
    val connection = this.getConnection

    try {
      results = executeSql(connection, false, "SHOW TABLES")
      val rs = results.resultset

      var tables = List[String]()
      while (rs.next()) {
        tables ::= rs.getString(1)
      }

      tables
    } catch {
      case e: Exception => {
        error("Couldn't get tables from database", e)
        throw e
      }
    } finally {
      if (connection != null)
        connection.close()

      if (results != null)
        results.close()
    }
  }

  private def createTable(table: Table, fullTableName: String) {
    var sql = "CREATE TABLE `%s` ( ".format(fullTableName) +
      "`ts` bigint(20) NOT NULL AUTO_INCREMENT, " +
      "`tk` bigint(20) NOT NULL, "

    sql += ((for (i <- 1 to table.depth) yield " k%d varchar(128) NOT NULL ".format(i)) ++ (for (i <- 1 to table.depth) yield " g%d varchar(128) NOT NULL ".format(i))).mkString(",") + ","

    val keyList = (for (i <- 1 to table.depth) yield "k%d".format(i)).mkString(",")
    sql += " `d` blob NULL, " +
      " PRIMARY KEY (`ts`,`tk`," + keyList + "), " +
      "	UNIQUE KEY `revkey` (`tk`, " + keyList + ",`ts`) " +
      ") ENGINE=InnoDB  DEFAULT CHARSET=utf8;"


    var results: SqlResults = null
    val connection = this.getConnection

    try {
      results = this.executeSql(connection, true, sql)

    } catch {
      case e: Exception =>
        error("Couldn't create table {}", fullTableName, e)
        throw e

    } finally {
      if (connection != null)
        connection.close()

      if (results != null)
        results.close()
    }
  }

  private def dropTable(tableName: String) {
    val sql = "DROP TABLE `%s`".format(tableName)

    var results: SqlResults = null
    val connection = this.getConnection

    try {
      results = this.executeSql(connection, true, sql)

    } catch {
      case e: Exception =>
        error("Couldn't drop table {}", tableName, e)
        throw e

    } finally {
      if (connection != null)
        connection.close()

      if (results != null)
        results.close()
    }
  }

  override def execFrom(context: ExecutionContext, into: Variable, keys: Object*) {
    val tableName = param[StringValue](keys, 0).strValue
    val optTable = model.getTable(tableName)

    optTable match {
      case Some(t) =>
        into.value = new TableValue(this, t)

      case None =>
        throw new StorageException("Non existing table %s".format(tableName))

    }
  }

  object GarbageCollector extends Instrumented {
    private val metricCollect = metrics.timer("gc-collect")
    private val metricCollected = metrics.counter("gc-collected")

    val VERSIONS_BATCH_COUNT = 100
    val MIN_COLLECTION = 10
    val GC_DELAY = 1000

    val killed = new AtomicBoolean(false)
    val active = new AtomicBoolean(true)
    var lastTokens = mutable.Map[Table, Long]()
    var versionsCache = mutable.Map[Table, mutable.Queue[VersionRecord]]()
    var allTables: Seq[Table] = model.allHierarchyTables

    var lastCollectMutationsCount = 0
    var lastCollected = 0 // always force first collection
    var nbEmptyCollect = 0 // succeeding empty garbage collection

    val scheduledExecutor = new ScheduledThreadPoolExecutor(1)
    scheduledExecutor.scheduleWithFixedDelay(new Runnable {
      def run() {
        if (killed.get()) {
          info("Garbage collector shouldn't be running anymore. Stopping!")
          throw new InterruptedException()
        }

        if (active.get()) {
          val curCount = mutationsCount.get()
          val mutationsDiff = curCount - lastCollectMutationsCount
          lastCollectMutationsCount = curCount

          if (mutationsDiff > 0 || lastCollected > 0 || nbEmptyCollect < 5) {
            val toCollect = math.min(mutationsDiff, MIN_COLLECTION)
            lastCollected = collect(toCollect)

            if (lastCollected == 0)
              nbEmptyCollect += 1
            else
              nbEmptyCollect = 0
          }
        }
      }
    }, GC_DELAY, GC_DELAY, TimeUnit.MILLISECONDS)

    def start() {
      this.active.set(true)
    }

    def stop() {
      this.active.set(false)
    }

    def kill() {
      this.stop()
      killed.set(true)
    }

    /**
     * Garbage collect at least specified versions (if any can be collected)
     * Not thread safe! Call from 1 thread at the time!
     *
     * @param toCollect Number of versions to collect
     * @return Collected versions
     */
    def collect(toCollect: Int): Int = {
      debug("GCing iteration starting, need {} to be collected", toCollect)

      var trx: MysqlTransaction = null
      var collectedTotal = 0

      metricCollect.time({
        try {
          trx = getStorageTransaction
          val collectPerTable = (toCollect / allTables.size) + 1

          for (table <- allTables) {
            var lastToken = lastTokens.getOrElse(table, 0l)
            val versions: mutable.Queue[VersionRecord] = versionsCache.getOrElse(table, mutable.Queue())

            // no more versions in cache, fetch new batch
            if (versions.size == 0) {
              val fetched = trx.getTopMostVersions(table, lastToken, VERSIONS_BATCH_COUNT)
              versions ++= fetched

              // didn't fetch anything, no more versions after this token. rewind to beginning
              if (fetched.size == 0)
                lastToken = 0
            }

            var collectedVersions = 0
            while (versions.size > 0 && collectedVersions < collectPerTable) {
              val version = versions.dequeue()
              val toDelete = (version.generations - table.maxVersions)

              trx.truncateVersions(table, version.token, version.accessPath, toDelete)

              collectedVersions += toDelete
              lastToken = version.token
            }

            lastTokens += (table -> lastToken)
            versionsCache += (table -> versions)
            collectedTotal += collectedVersions
          }


          metricCollected += collectedTotal
          trx.commit()
        } catch {
          case e: Exception =>
            try {
              if (trx != null)
                trx.rollback()
            } catch {
              case _ =>
            }

            error("Catched an exception in garbage collector!", e)
        }
      })

      info("GCing iteration done! Collected {} versions", collectedTotal)
      collectedTotal
    }
  }

}

class SqlResults {
  var statement: PreparedStatement = null
  var resultset: ResultSet = null

  def close() {
    if (resultset != null)
      resultset.close()

    if (statement != null)
      statement.close()
  }
}


