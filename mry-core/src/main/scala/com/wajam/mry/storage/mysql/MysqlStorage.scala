package com.wajam.mry.storage.mysql

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.wajam.nrv.Logging
import java.sql.{PreparedStatement, ResultSet, SQLException, Connection}
import com.wajam.mry.execution._
import com.wajam.mry.api.protobuf.ProtobufTranslator
import com.wajam.mry.storage._
import com.yammer.metrics.scala.Instrumented
import java.util.concurrent.atomic.AtomicInteger
import collection.mutable
import java.util.concurrent.{TimeUnit, ScheduledThreadPoolExecutor}

/**
 * MySQL backed storage
 */
class MysqlStorage(config: MysqlStorageConfiguration, garbageCollection: Boolean = true)
  extends Storage(config.name) with Logging with Value with Instrumented {
  var model: Model = _
  var tablesMutationsCount = Map[Table, AtomicInteger]()
  val valueSerializer = new ProtobufTranslator

  val datasource = new ComboPooledDataSource()
  datasource.setDriverClass("com.mysql.jdbc.Driver")
  datasource.setJdbcUrl(String.format("jdbc:mysql://%s/%s?zeroDateTimeBehavior=convertToNull", config.host, config.database))
  datasource.setUser(config.username)
  datasource.setPassword(config.password)
  datasource.setInitialPoolSize(config.initPoolSize)
  datasource.setMaxPoolSize(config.maxPoolSize)
  datasource.setNumHelperThreads(config.numhelperThread)

  def createStorageTransaction(context: ExecutionContext) = new MysqlTransaction(this, Some(context))

  def createStorageTransaction = new MysqlTransaction(this, None)

  def closeStorageTransaction(trx: MysqlTransaction) {
    model.allHierarchyTables.map(table => {
      this.tablesMutationsCount(table).getAndAdd(trx.tableMutationsCount(table).get)
    })
  }

  def getStorageValue(context: ExecutionContext): Value = this

  /**
   * Create and delete tables from MySQL based on the given model.
   * *WARNING*: This method should only be called ONCE before starting the storage
   *
   * @param model Model to sync
   * @param deleteOld Also delete the old tables
   */
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

    tablesMutationsCount = allTables.map(table => (table, new AtomicInteger(0))).toMap
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
      trace("Executing SQL '{}', with params {}", sql, params)

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
      "`ts` bigint(20) NOT NULL, " +
      "`tk` bigint(20) NOT NULL, " +
      "`ec` tinyint(1) NOT NULL DEFAULT '0', "

    sql += (for (i <- 1 to table.depth) yield " k%d varchar(128) NOT NULL ".format(i)).mkString(",") + ","

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
    private val allTables: Iterable[Table] = model.allHierarchyTables

    private val metricCollect = allTables.map(table =>
      (table, metrics.timer("gc-collect", table.uniqueName))).toMap
    private val metricCollected = allTables.map(table =>
      (table, metrics.meter("gc-collected-record", "records", table.uniqueName))).toMap

    @volatile
    private var active = false
    private val tableNextToken = mutable.Map[Table, Long]().withDefaultValue(0)
    private val tableVersionsCache = mutable.Map[Table, mutable.Queue[VersionRecord]]()
    private val tableLastMutationsCount = mutable.Map[Table, Int]().withDefaultValue(0)
    private val tableLastCollection = mutable.Map[Table, Int]().withDefaultValue(0) // always force first collection

    private val scheduledExecutor = new ScheduledThreadPoolExecutor(1)
    private val scheduledTask = scheduledExecutor.scheduleWithFixedDelay(new Runnable {
      def run() {
        try {
          if (active) {
            allTables.foreach(table => {
              val currentMutations = tablesMutationsCount(table).get()
              val lastMutations = tableLastMutationsCount(table)
              val mutationsDiff = currentMutations - lastMutations
              tableLastMutationsCount += (table -> currentMutations)

              val lastCollection = tableLastCollection(table)
              if (mutationsDiff > 0 || lastCollection > 0) {
                val toCollect = math.max(math.max(mutationsDiff, config.gcMinimumCollection), lastCollection) * config.gcCollectionFactor
                log.info("Collecting {} from table {}", toCollect, table.name)
                val collected = collect(table, toCollect.toInt)
                tableLastCollection += (table -> collected)
              }
            })
          }
        } catch {
          case e: Exception => log.error("Got an error in GC scheduler", e)
        }
      }
    }, config.gcDelayMs, config.gcDelayMs, TimeUnit.MILLISECONDS)

    def start() {
      this.active = true
    }

    def stop() {
      this.active = false
    }

    private[mysql] def kill() {
      this.stop()
      this.scheduledTask.cancel(true)
    }

    private[mysql] def collectAll(toCollect: Int): Int = {
      allTables.foldLeft(0)((sum, table) => sum + collect(table, toCollect))
    }

    /**
     * Garbage collect at least specified versions (if any can be collected) from a table
     * Not thread safe! Call from 1 thread at the time!
     *
     * @param table Table to collect
     * @param toCollect Number of versions to collect
     * @return Collected versions
     */
    private[mysql] def collect(table: Table, toCollect: Int): Int = {
      debug("GCing iteration starting, need {} to be collected", toCollect)

      var trx: MysqlTransaction = null
      var collectedTotal = 0

      metricCollect(table).time({
        try {
          trx = createStorageTransaction

          val lastToken = tableNextToken(table)
          val recordsVersions: mutable.Queue[VersionRecord] = tableVersionsCache.getOrElse(table, mutable.Queue())
          val toToken = lastToken + config.gcTokenStep

          // no more versions in cache, fetch new batch
          var nextToken: Long = 0
          if (recordsVersions.size == 0) {
            val fetched = trx.getTopMostVersions(table, lastToken, toToken, config.gcVersionsBatch)
            recordsVersions ++= fetched

            if (fetched.size < config.gcVersionsBatch) {
              nextToken = toToken
            }
          }

          var collectedVersions = 0
          while (recordsVersions.size > 0 && collectedVersions < toCollect) {
            val record = recordsVersions.dequeue()
            val versions = record.versions
            val toDeleteVersions = versions.sortBy(_.value).slice(0, versions.size - table.maxVersions)

            if (toDeleteVersions.size > 0) {
              trx.truncateVersions(table, record.token, record.accessPath, toDeleteVersions)
              collectedVersions += toDeleteVersions.size
            }
            if (record.token > nextToken)
              nextToken = record.token
          }

          if (nextToken >= Int.MaxValue.toLong * 2)
            nextToken = 0

          tableNextToken += (table -> nextToken)
          tableVersionsCache += (table -> recordsVersions)
          collectedTotal += collectedVersions

          metricCollected(table).mark(collectedTotal)
          trx.commit()
        } catch {
          case e: Exception =>
            try {
              if (trx != null)
                trx.rollback()
            } catch {
              case _ =>
            }

            error("Caught an exception in garbage collector!", e)
        }
      })

      debug("GCing iteration done! Collected {} versions", collectedTotal)
      collectedTotal
    }
  }

}

case class MysqlStorageConfiguration(name: String,
                                     host: String,
                                     database: String,
                                     username: String,
                                     password: String,
                                     initPoolSize: Int = 3,
                                     maxPoolSize: Int = 15,
                                     numhelperThread: Int = 3,
                                     gcMinimumCollection: Int = 20,
                                     gcCollectionFactor: Double = 1.2,
                                     gcTokenStep: Long = 10000,
                                     gcDelayMs: Int = 1000,
                                     gcVersionsBatch: Int = 100)


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


