package com.wajam.mry.storage.mysql

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.wajam.nrv.Logging
import java.sql.{PreparedStatement, ResultSet, SQLException, Connection}
import com.wajam.mry.execution._
import com.wajam.mry.api.protobuf.ProtobufTranslator
import com.wajam.mry.storage._
import com.yammer.metrics.scala.{Meter, Timer, Instrumented}
import java.util.concurrent.atomic.AtomicInteger
import collection.mutable
import java.util.concurrent.{TimeUnit, ScheduledThreadPoolExecutor}
import com.wajam.nrv.service.TokenRange
import com.yammer.metrics.core.Gauge
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * MySQL backed storage
 */
class MysqlStorage(config: MysqlStorageConfiguration, garbageCollection: Boolean = true)
  extends Storage with ConsistentStorage with Logging with Value with Instrumented {
  val name = config.name
  var model: Model = _
  var transactionMetrics: MysqlTransaction.Metrics = _
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

  def createStorageTransaction(context: ExecutionContext) = new MysqlTransaction(this, Some(context), transactionMetrics)

  def createStorageTransaction = new MysqlTransaction(this, None, transactionMetrics)

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

    transactionMetrics = new MysqlTransaction.Metrics(this)
    tablesMutationsCount = allTables.map(table => (table, new AtomicInteger(0))).toMap
  }

  def getConnection = datasource.getConnection

  def nuke() {
    this.getTables.foreach(table => {
      this.dropTable(table)
    })
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
  def executeSqlUpdate(connection: Connection, sql: String, params: Any*) {
    var results: SqlResults = null
    try {
      results = this.executeSql(connection, true, sql, params: _*)
    } finally {
      if (results != null)
        results.close()
    }
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

      if (update) {
        results.statement.executeUpdate()
        results.close()
      } else {
        results.resultset = results.statement.executeQuery()
      }

      results

    } catch {
      case e: Exception => {
        error("Couldn't execute SQL query {}", sql, e)
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
        val name = rs.getString(1)
        if (name.contains("_index")) {
          tables ::= name.stripSuffix("_index")
        }
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
    val connection = this.getConnection

    try {
      val keyList = (for (i <- 1 to table.depth) yield "k%d".format(i)).mkString(",")

      // create the index table
      var indexSql = "CREATE TABLE IF NOT EXISTS `%s_index` ( ".format(fullTableName) +
        "`ts` bigint(20) NOT NULL, " +
        "`tk` bigint(20) NOT NULL, "
      indexSql += (for (i <- 1 to table.depth) yield " k%d varchar(128) NOT NULL ".format(i)).mkString(",") + "," +
        " PRIMARY KEY (`ts`,`tk`," + keyList + "), " +
        " UNIQUE KEY `revkey` (`tk`," + keyList + ",`ts`) " +
        ") ENGINE=InnoDB  DEFAULT CHARSET=utf8;"
      this.executeSqlUpdate(connection, indexSql)

      // create the data table
      var dataSql = "CREATE TABLE IF NOT EXISTS `%s_data` ( ".format(fullTableName) +
        "`tk` bigint(20) NOT NULL, "
      dataSql += (for (i <- 1 to table.depth) yield " k%d varchar(128) NOT NULL ".format(i)).mkString(",") + "," +
        "`ts` bigint(20) NOT NULL, " +
        "`ec` tinyint(1) NOT NULL DEFAULT '0', " +
        " `d` blob NULL, " +
        " PRIMARY KEY (`tk`," + keyList + ",`ts`) " +
        ") ENGINE=InnoDB  DEFAULT CHARSET=utf8;"
      this.executeSqlUpdate(connection, dataSql)

    } catch {
      case e: Exception =>
        error("Couldn't create table {}", fullTableName, e)
        throw e

    } finally {
      if (connection != null)
        connection.close()
    }
  }

  private def dropTable(tableName: String) {
    val connection = this.getConnection

    try {
      this.executeSqlUpdate(connection, "DROP TABLE `%s_index`".format(tableName))
      this.executeSqlUpdate(connection, "DROP TABLE `%s_data`".format(tableName))

    } catch {
      case e: Exception =>
        error("Couldn't drop table {}", tableName, e)
        throw e

    } finally {
      if (connection != null)
        connection.close()
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

  /**
   * Returns the latest record timestamp for the specified token ranges
   */
  def getLastTimestamp(ranges: Seq[TokenRange]): Option[Timestamp] = {
    var trx: MysqlTransaction = null
    try {
      trx = createStorageTransaction
      model.allHierarchyTables.map(table => trx.getLastTimestamp(table, ranges)).max
    } finally {
      if (trx != null) {
        trx.rollback()
      }
    }
  }

  /**
   * Truncate all records at the given timestamp for the specified token.
   */
  def truncateAt(timestamp: Timestamp, token: Long) {
    var trx: MysqlTransaction = null
    try {
      trx = createStorageTransaction
      for (table <- model.allHierarchyTables) {
        trx.truncateVersion(table, token, timestamp)
      }
      trx.commit()
    } catch {
      case e: Exception => {
        try {
          if (trx != null)
            trx.rollback()
        } catch {
          case _ =>
        }

        error("Caught an exception while truncating records storage (tk={}, ts={})", token, timestamp, e)
        throw e
      }
    }
  }

  /**
   * A group of records from different tables which been mutated at the same timestamp.
   */
  case class MutationGroup(token: Long, timestamp: Timestamp, var records: List[Record] = Nil)

  /**
   * Iterate group of mutation records <b>from</b> the specified timestamp up <b>to</b> specified timestamp.
   * @param from start timestamp (inclusive)
   * @param to end timestamp (inclusive)
   * @param ranges token ranges
   * @param recordCacheSize maximum number of records cached in iterator
   * @param indexBatchSize maximum number of indexes per table loaded from database
   */
  class MutationGroupIterator(from: Timestamp, to: Timestamp, ranges: Seq[TokenRange], recordCacheSize: Int = 100,
                              indexBatchSize: Int = 50) extends Traversable[MutationGroup] {

    private case class MutationGroupIndex(token: Long, timestamp: Timestamp, var indexes: List[RecordIndex] = Nil)

    private case class TableIndex(table: Table, var lastTimestamp: Timestamp, var indexes: List[RecordIndex] = Nil) {
      def hasMore: Boolean = lastTimestamp < to

      def takeIndexesTo(timestamp: Timestamp): List[RecordIndex] = {
        val (taken, remaining) = indexes.span(_.timestamp <= timestamp)
        indexes = remaining
        taken
      }
    }

    // Mutation group cache ordered by timestamp.
    private var groups: List[MutationGroup] = Nil

    // Mutation group index ordered by timestamp. Very similar to the mutation group cache but without data loaded.
    private var groupIndexes: List[MutationGroupIndex] = Nil

    // Record indexes cached per table
    private val tableIndexes: Seq[TableIndex] = {
      val initialTimestamp = Timestamp(from.value - 1)
      model.allHierarchyTables.map(table => loadTableIndex(TableIndex(table, initialTimestamp))).toSeq
    }

    private var error: Option[Exception] = None

    def foreach[U](f: (MutationGroup) => U) {
      while (next()) {
        f(mutationGroup)
      }
    }

    def next(): Boolean = {

      // Do not continue if we had an error before as the iterator state is likely inconsistent.
      error.foreach(e => throw e)

      if (!groups.isEmpty) {
        groups = groups.tail
      }

      try {
        // Load more mutation groups if needed and possible
        if (groups.isEmpty && (!groupIndexes.isEmpty || tableIndexes.exists(ti => ti.hasMore || !ti.indexes.isEmpty))) {

          // Ensure we have a minimum quantity of indexes loaded from each table
          tableIndexes.withFilter(ti => ti.hasMore && ti.indexes.size < indexBatchSize * 0.70).foreach(loadTableIndex(_))

          // Group record indexes by timestamp
          val maxAvailable = tableIndexes.minBy(_.lastTimestamp).lastTimestamp // Can group indexes up to that timestamp
          val grouped: mutable.Map[Timestamp, MutationGroupIndex] = mutable.Map()
          for (tableIndex <- tableIndexes; index <- tableIndex.takeIndexesTo(maxAvailable)) {
            val groupIndex = grouped.getOrElseUpdate(index.timestamp, MutationGroupIndex(index.token, index.timestamp))
            groupIndex.indexes = index :: groupIndex.indexes
          }
          groupIndexes = (groupIndexes ++ grouped.valuesIterator).sortBy(_.timestamp)

          // Load records from database
          if (!groupIndexes.isEmpty) {

            // Compute the timestamp range and from which tables to load data while respecting the cache size as much
            // as possible. A record group is always entirely loaded and cached records can go beyond the cache size
            // (but never more than the last record group size)
            var loadCount = 0
            val loadFrom = groupIndexes.head.timestamp
            var loadTo = loadFrom
            var loadTables: mutable.Set[Table] = mutable.Set()
            while (!groupIndexes.isEmpty && loadCount < recordCacheSize) {
              loadTables ++= groupIndexes.head.indexes.map(_.table)
              loadCount += groupIndexes.head.indexes.size
              loadTo = groupIndexes.head.timestamp
              groupIndexes = groupIndexes.tail
            }

            // Finally load, merge and sort the data grouped by timestamp
            groups = (groups ++ loadTablesData(loadTables, loadFrom, loadTo)).sortBy(_.timestamp)
          }
        }
      } catch {
        case e: Exception => {
          error = Some(e)
          throw e
        }
      }

      !groups.isEmpty
    }

    def mutationGroup: MutationGroup = {
      groups.head
    }

    private def loadTableIndex(index: TableIndex): TableIndex = {
      var trx: MysqlTransaction = null
      try {
        trx = createStorageTransaction
        val indexes = trx.getRecordIndexRange(index.table, Timestamp(index.lastTimestamp.value + 1),
          indexBatchSize, ranges)
        index.indexes = (index.indexes ++ indexes.toList).sortBy(_.timestamp)
        index.lastTimestamp = if (indexes.isEmpty) {
          to
        } else {
          indexes.maxBy(_.timestamp).timestamp
        }
        info("loadTableIndex: {}", index)
        index
      } finally {
        if (trx != null) {
          trx.rollback()
        }
      }
    }

    /**
     * Load all records from the specified tables and timestamp range. The records are grouped by timestamp.
     */
    private def loadTablesData(tables: Iterable[Table], from: Timestamp, to: Timestamp): Seq[MutationGroup] = {
      var trx: MysqlTransaction = null
      try {
        trx = createStorageTransaction
        val grouped: mutable.Map[Timestamp, MutationGroup] = mutable.Map()
        for (table <- tables; record <- trx.getRecordRange(table, from, to, ranges)) {
          val group = grouped.getOrElseUpdate(record.timestamp, MutationGroup(record.token, record.timestamp))
          group.records = record :: group.records
        }
        grouped.values.toSeq
      } finally {
        if (trx != null) {
          trx.rollback()
        }
      }
    }
  }

  object GarbageCollector {
    private val allTables: Iterable[Table] = model.allHierarchyTables

    @volatile
    private var active = false
    private var tokenRanges: List[TokenRange] = List(TokenRange.All)
    private[MysqlStorage] val tableCollectors = mutable.Map[Table, TableCollector]()

    private val scheduledExecutor = new ScheduledThreadPoolExecutor(1)
    private val scheduledTask = scheduledExecutor.scheduleWithFixedDelay(new Runnable {
      def run() {
        try {
          if (active) {
            val collectors = tableCollectors.synchronized {
              if (tokenRanges.size > 0) {
                allTables.map(table => getTableCollector(table))
              } else {
                Seq[TableCollector]()
              }
            }
            collectors.foreach(_.tryCollect())
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

    def setCollectedRanges(collectedRanges: List[TokenRange]) {
      tableCollectors.synchronized {
        tokenRanges = collectedRanges
        tableCollectors.clear()
      }
    }

    private[mysql] def kill() {
      this.stop()
      this.scheduledTask.cancel(true)
    }

    private[mysql] def collectAll(toCollect: Int, versionBatchSize: Int = config.gcVersionsBatch): Int = {
      val collectors = tableCollectors.synchronized {
        if (tokenRanges.size > 0) {
          allTables.map(table => getTableCollector(table))
        } else {
          Seq[TableCollector]()
        }
      }
      collectors.foldLeft(0)((sum, collector) => sum + collector.collect(toCollect, versionBatchSize))
    }

    /**
     * Returns the cached table collector. A new collector is created and put in the cache if none is already cached.
     * The method caller MUST hold a lock on the tableCollectors map prior calling this method.
     */
    private def getTableCollector(table: Table): TableCollector = {
      tableCollectors.getOrElseUpdate(table, new TableCollector(table, tokenRanges))
    }
  }

  class TableCollector(table: Table, tokenRanges: List[TokenRange]) extends Instrumented {

    if (tokenRanges.size == 0) {
      throw new IllegalArgumentException("Requires at least one token range.")
    }

    lazy private val collectTimer = new Timer(metrics.metricsRegistry.newTimer(GarbageCollector.getClass,
      "gc-collect", table.uniqueName))
    lazy private val collectedVersionsMeter = new Meter(metrics.metricsRegistry.newMeter(GarbageCollector.getClass,
      "gc-collected-record", table.uniqueName, "records", TimeUnit.SECONDS))
    lazy private val extraVersionsLoadedMeter = new Meter(metrics.metricsRegistry.newMeter(GarbageCollector.getClass,
      "gc-extra-record-loaded", table.uniqueName, "extra-record-loaded", TimeUnit.SECONDS))
    private val collectedTokenGauge = metrics.metricsRegistry.newGauge(GarbageCollector.getClass,
      "gc-collected-token", table.uniqueName, new Gauge[Long] {
        def value = {
          // Use token from the current table collector
          GarbageCollector.tableCollectors.get(table) match {
            case Some(collector) => collector.tableNextToken
            case _ => 0
          }
        }
      })

    private var tableNextRange: TokenRange = tokenRanges.head
    @volatile
    private var tableNextToken: Long = tokenRanges.head.start
    private val tableVersionsCache = mutable.Queue[VersionRecord]()
    private var tableLastMutationsCount: Int = 1
    private var tableLastCollection: Int = 1 // always force first collection

    private[mysql] def tryCollect() {
      val currentMutations = tablesMutationsCount(table).get()
      val lastMutations = tableLastMutationsCount
      val mutationsDiff = currentMutations - lastMutations
      tableLastMutationsCount = currentMutations

      val lastCollection = tableLastCollection
      if (mutationsDiff > 0 || lastCollection > 0) {
        val toCollect = math.max(math.max(mutationsDiff, config.gcMinimumCollection), lastCollection) * config.gcCollectionFactor
        log.debug("Collecting {} from table {}", toCollect, table.name)
        val collected = collect(toCollect.toInt, config.gcVersionsBatch)
        log.debug("Collected {} from table {}", collected, table.name)
        tableLastCollection = collected
      }
    }

    /**
     * Garbage collect at least specified versions (if any can be collected) from a table
     * Not thread safe! Call from 1 thread at the time!
     *
     * @param toCollect Number of versions to collect
     * @return Collected versions
     */
    private[mysql] def collect(toCollect: Int, versionBatchSize: Int): Int = {
      debug("GCing {} iteration starting, need {} to be collected", table.uniqueName, toCollect)

      var trx: MysqlTransaction = null
      var collectedTotal = 0

      collectTimer.time({
        try {
          trx = createStorageTransaction

          val lastToken = tableNextToken
          val lastRange = tableNextRange
          val toToken = math.min(lastToken + config.gcTokenStep, lastRange.end)

          // no more versions in cache, fetch new batch
          var nextToken = lastToken
          if (tableVersionsCache.size == 0) {
            val fetched = trx.getTopMostVersions(table, lastToken, toToken, versionBatchSize)
            tableVersionsCache ++= fetched

            if (fetched.size < versionBatchSize) {
              nextToken = toToken
            }
          }

          var collectedVersions = 0
          while (tableVersionsCache.size > 0 && collectedVersions < toCollect) {
            val record = tableVersionsCache.front // peek only, record is dequeued only once completed
            val versions = record.versions
            val toDeleteVersions = versions.sortBy(_.value).slice(0, versions.size - table.maxVersions)

            if (toDeleteVersions.size > 0) {
              trx.truncateVersions(table, record.token, record.accessPath, toDeleteVersions)
              collectedVersions += toDeleteVersions.size
            }

            if (record.versionsCount <= versions.size) {
              // All extra record versions has been truncated, officialy dequeue it
              tableVersionsCache.dequeue()
            } else {
              // More versions need to be truncated, reload and update record
              trx.getTopMostVersions(table, record.token, record.token, versionBatchSize).find(
                _.accessPath == record.accessPath) match {
                case Some(reloaded) => {
                  extraVersionsLoadedMeter.mark(reloaded.versions.size)
                  record.versions = reloaded.versions
                  record.versionsCount = reloaded.versionsCount
                }
                case _ => {
                  warn("Record version not found (table={}, tk={}, path={})", table.name, record.token, record.accessPath)

                  // The access path we were trying to GC is beyond the version batch size i.e. if the batch is 100,
                  // we were trying the GC the 101st or later. Clearing the cache, will restart GC to the first range
                  // of access path for the same token.
                  tableVersionsCache.clear()
                }
              }
            }

            if (record.token > nextToken)
              nextToken = record.token
          }

          if (tableVersionsCache.size == 0 && nextToken >= lastRange.end) {
            tableNextRange = lastRange.nextRange(tokenRanges).getOrElse(tokenRanges.head)
            tableNextToken = tableNextRange.start
          } else {
            tableNextRange = lastRange
            tableNextToken = nextToken
          }
          collectedTotal += collectedVersions

          collectedVersionsMeter.mark(collectedTotal)
          trx.commit()
        } catch {
          case e: Exception => {
            try {
              if (trx != null)
                trx.rollback()
            } catch {
              case _ =>
            }

            error("Caught an exception in {} garbage collector!", table.uniqueName, e)
            throw e
          }
        }
      })

      debug("GCing iteration done on {}! Collected {} versions", table.uniqueName, collectedTotal)
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


