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
import com.wajam.nrv.utils.Closable

/**
 * MySQL backed storage
 */
class MysqlStorage(config: MysqlStorageConfiguration, garbageCollection: Boolean = true)
  extends Storage with ConsistentStorage with Logging with Value with Instrumented {
  val name = config.name
  private[mysql] var model: Model = _
  private[mysql] var transactionMetrics: MysqlTransaction.Metrics = null
  private var tablesMutationsCount = Map[Table, AtomicInteger]()
  val valueSerializer = new ProtobufTranslator
  private var currentConsistentTimestamps: (TokenRange) => Timestamp = (_) => Long.MinValue
  private var openReadIterators: List[MutationGroupIterator] = List()

  lazy private val lastTimestampTimer = metrics.timer("get-last-timestamp-time")
  lazy private val truncateTransactionTimer = metrics.timer("truncate-transaction")
  lazy private val readTransactionsInitTimer = metrics.timer("read-transactions-init")
  lazy private val readTransactionsNextTimer = metrics.timer("read-transactions-next")

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

    def getTable(coll: TableCollection, tableNames: List[String]): Table = {
      if (tableNames == Nil) {
        coll match {
          case table: Table => table
          case _ => throw new StorageException("Non existing table")
        }
      } else {
        coll.getTable(tableNames.head) match {
          case Some(table) => getTable(table, tableNames.tail)
          case None => throw new StorageException("Non existing table %s".format(tableNames.head))
        }
      }
    }

    into.value = new TableValue(this, getTable(model, extractTableNames(keys:_*)))
  }

  private def extractTableNames(params: Object*): List[String] = {
    params(0) match {
      case StringValue(tableName) => {
        List(tableName)
      }
      case ListValue(values) => {
        values.map {
          case StringValue(tableName) => tableName
          case _ => throw new InvalidParameter("Expected parameter at position 0 to be of instance ListValue[Seq[StringValue]]")
        }.toList
      }
      case _ => {
        throw new InvalidParameter("Expected parameter at position 0 to be of instance StringValue or ListValue")
      }
    }
  }

  /**
   * Returns the latest record timestamp for the specified token ranges
   */
  def getLastTimestamp(ranges: Seq[TokenRange]): Option[Timestamp] = {
    var trx: MysqlTransaction = null
    try {
      lastTimestampTimer.time {
        trx = createStorageTransaction
        model.allHierarchyTables.map(table => trx.getLastTimestamp(table, ranges)).max
      }
    } finally {
      if (trx != null) {
        trx.rollback()
      }
    }
  }

  /**
   * Setup the function which returns the most recent timestamp considered as consistent by the Consistency manager
   * for the specified token range. The consistency of the records more recent that the consistent timestamp is
   * unconfirmed and these records must be excluded from processing tasks such as GC or percolation.
   */
  def setCurrentConsistentTimestamp(getCurrentConsistentTimestamps: (TokenRange) => Timestamp) {
    currentConsistentTimestamps = getCurrentConsistentTimestamps
  }

  private[mysql] def getCurrentConsistentTimestamp(ranges: Seq[TokenRange]): Timestamp = {
    ranges.map(range => currentConsistentTimestamps(range)).min
  }

  /**
   * Truncate all records at the given timestamp for the specified token.
   */
  def truncateAt(timestamp: Timestamp, token: Long) {
    truncateTransactionTimer.time {
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
  }

  /**
   * Returns the mutation transactions from and to the given timestamps inclusively for the specified token ranges.
   */
  def readTransactions(from: Timestamp, to: Timestamp, ranges: Seq[TokenRange]) = {
    readTransactionsInitTimer.time {
      new Iterator[MutationGroup] with Closable {
        private val itr = new MutationGroupIterator(from, to, ranges)
        private var nextGroup: Option[MutationGroup] = readNext()

        // Add this iterator in GC exclusion list until closed
        MysqlStorage.this.synchronized {
          openReadIterators = itr :: openReadIterators
        }

        private def readNext(): Option[MutationGroup] = {
          readTransactionsNextTimer.time {
            if (itr.next()) {
              Some(itr.mutationGroup)
            } else {
              None
            }
          }
        }

        def hasNext = nextGroup.isDefined

        def next() = {
          val result = nextGroup
          nextGroup = readNext()
          trace("readTransactions.next {}", result)
          result.get // Must fail if next is called while hasNext is false
        }

        def close() {
          // Remove this iterator from GC exlusion list
          MysqlStorage.this.synchronized {
            openReadIterators = openReadIterators.filterNot(_ == itr)
          }
        }
      }
    }
  }

  /**
   * A group of records from different tables which been mutated at the same timestamp.
   */
  case class MutationGroup(token: Long, timestamp: Timestamp, var records: List[Record] = Nil)
    extends TransactionRecord {

    /**
     * Apply the mutation group to the specified transaction
     */
    def applyTo(transaction: Transaction) {
      import com.wajam.mry.execution.Implicits._

      // Set or delete each record
      val storage = transaction.from("mysql")
      for (record <- records.sortBy(_.table.depth)) {
        val tableNames = record.table.path.map(_.name)
        val keys = record.accessPath.keys
        if (record.value.isNull) {
          storage.from(tableNames).delete(keys)
        } else {
          storage.from(tableNames).set(keys, record.value)
        }
      }
    }

    override def toString = {
      "MutationGroup(tk=%d, ts=%s, records=%s)".format(token, timestamp, records)
    }
  }

  /**
   * Iterate group of mutation records <b>from</b> the specified timestamp up <b>to</b> specified timestamp.
   *
   * @param from start timestamp (inclusive)
   * @param to end timestamp (inclusive)
   * @param ranges token ranges
   * @param recordsCacheSize maximum number of records cached in this iterator. This is not the mutation group size but
   *                        the number of records contained in the queued mutation group. A limit of 100 records can be
   *                        reach by having 4 mutation groups of 25 records.
   * @param tableSummarySize number of transaction summary records loaded per table per call from database
   * @param tableSummaryThreshold threshold per table below which more transaction summary records are loaded
   */
  class MutationGroupIterator(val from: Timestamp, to: Timestamp, val ranges: Seq[TokenRange],
                              recordsCacheSize: Int = 100, tableSummarySize: Int = 50,
                              tableSummaryThreshold: Double = 0.70) extends Traversable[MutationGroup] {

    private case class MutationGroupSummary(token: Long, timestamp: Timestamp,
                                            var records: List[TransactionSummaryRecord] = Nil)

    private case class TableSummary(table: Table, var lastTimestamp: Timestamp,
                                    var records: List[TransactionSummaryRecord] = Nil) {
      def hasMore: Boolean = lastTimestamp < to

      def mustLoadMore: Boolean = hasMore && records.size < tableSummarySize * tableSummaryThreshold

      def takeSummaryRecordsTo(timestamp: Timestamp): List[TransactionSummaryRecord] = {
        val (taken, remaining) = records.span(_.timestamp <= timestamp)
        records = remaining
        taken
      }
    }

    // Cached mutation group ordered by timestamp.
    private var groupsQueue: List[MutationGroup] = Nil

    // Mutation group summary ordered by timestamp. Very similar to the mutation group queue but without data loaded.
    private var groupsSummaryQueue: List[MutationGroupSummary] = Nil

    // Transaction summary cached per table.
    private val tablesCache: Seq[TableSummary] = {
      val initialTimestamp = Timestamp(from.value - 1)
      model.allHierarchyTables.map(table => loadTableSummary(TableSummary(table, initialTimestamp))).toSeq
    }

    private var error: Option[Exception] = None

    def foreach[U](f: (MutationGroup) => U) {
      while (next()) {
        f(mutationGroup)
      }
    }

    /**
     * Advance to the next mutation group. Returns true if successful or false if no more group are available.
     */
    def next(): Boolean = {
      // Do not continue if we had an error before as the iterator state is likely inconsistent.
      error.foreach(e => throw e)

      // Go to next queued group and try to load more groups if queue is empty
      groupsQueue match {
        case Nil => loadCache()
        case _ :: Nil => {
          groupsQueue = Nil
          loadCache()
        }
        case _ :: tail => groupsQueue = tail
      }

      !groupsQueue.isEmpty
    }

    /**
     * Returns the current mutation group
     */
    def mutationGroup: MutationGroup = {
      groupsQueue.head
    }

    private def loadCache() {
      try {
        debug("loadCache - start %d".format(groupsQueue.size))
        val loadedCount = loadMutationGroupFromSummaryQueue(maxRecordsCount = recordsCacheSize)
        if (loadedCount < recordsCacheSize) {
          loadMoreMutationGroupSummary()
          loadMutationGroupFromSummaryQueue(maxRecordsCount = recordsCacheSize - loadedCount)
        }
        debug("loadCache - done %d".format(groupsQueue.size))
      } catch {
        case e: Exception => {
          error = Some(e)
          throw e
        }
      }
    }

    /**
     * Load mutation group from the database up up to the specified max record count and using the group
     * summary already loaded. Returns the number of records loaded during this call. The loaded record count can be
     * lower than the specified max if not enough group summary are cached.
     */
    private def loadMutationGroupFromSummaryQueue(maxRecordsCount: Int): Int = {
      if (!groupsSummaryQueue.isEmpty) {
        debug("loadMutationGroupFromSummaryQueue: max=%d".format(maxRecordsCount))

        // Compute the timestamp range and from which tables to load data while respecting the maxRecordCount as much
        // as possible. A record group is always entirely loaded and cached records can go beyond the maxRecordCount
        // (but never more than the last record group size)
        var loadCount = 0
        val loadFrom = groupsSummaryQueue.head.timestamp
        var loadTo = loadFrom
        var loadTables: mutable.Set[Table] = mutable.Set()
        while (!groupsSummaryQueue.isEmpty && loadCount < maxRecordsCount) {
          val head :: tail = groupsSummaryQueue
          loadTables ++= head.records.map(_.table)
          loadCount += head.records.size
          loadTo = head.timestamp
          groupsSummaryQueue = tail
        }

        // Finally load, merge and sort the data grouped by timestamp
        groupsQueue = (groupsQueue ++ loadTablesData(loadTables, loadFrom, loadTo)).sortBy(_.timestamp)
        debug("loadMutationGroupFromSummaryQueue done: groupsQueue=%d".format(loadCount))
        loadCount
      } else 0
    }

    /**
     * Load more mutation groups summary if needed and possible
     */
    private def loadMoreMutationGroupSummary() {
      if (!groupsSummaryQueue.isEmpty || tablesCache.exists(ti => ti.hasMore || !ti.records.isEmpty)) {
        debug("loadMoreMutationGroupSummary")

        // Ensure we have a minimum quantity of transaction summary loaded from each table
        tablesCache.withFilter(_.mustLoadMore).foreach(loadTableSummary(_))

        // Group transaction summary by timestamp
        val maxAvailable = tablesCache.minBy(_.lastTimestamp).lastTimestamp // Can groups summary up to that timestamp
        val grouped: mutable.Map[Timestamp, MutationGroupSummary] = mutable.Map()
        for (tableSummary <- tablesCache; summary <- tableSummary.takeSummaryRecordsTo(maxAvailable)) {
          val groupSummary = grouped.getOrElseUpdate(summary.timestamp, MutationGroupSummary(summary.token, summary.timestamp))
          groupSummary.records = summary :: groupSummary.records
        }
        groupsSummaryQueue = (groupsSummaryQueue ++ grouped.valuesIterator).sortBy(_.timestamp)
        debug("loadMoreMutationGroupSummary done: groupsSummaryQueue=%d".format(groupsSummaryQueue.size))
      }
    }

    private def loadTableSummary(tableSummary: TableSummary): TableSummary = {
      var trx: MysqlTransaction = null
      try {
        trace("loadTableSummary: %s %d".format(tableSummary.table, tableSummary.records.size))
        trx = createStorageTransaction
        val records = trx.getTransactionSummaryRecords(tableSummary.table, Timestamp(tableSummary.lastTimestamp.value + 1),
          tableSummarySize, ranges)
        tableSummary.records = (tableSummary.records ++ records.toList).sortBy(_.timestamp)
        tableSummary.lastTimestamp = if (records.isEmpty) {
          to
        } else {
          records.maxBy(_.timestamp).timestamp
        }
        debug("loadTableSummary: done %s %d".format(tableSummary.table, tableSummary.records.size))
        tableSummary
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
        for (table <- tables) {
          debug("loadTablesData: {}", table)
          for (record <- trx.getTransactionRecords(table, from, to, ranges)) {
            val group = grouped.getOrElseUpdate(record.timestamp, MutationGroup(record.token, record.timestamp))
            group.records = record :: group.records
          }
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
          val consistentTimestamp = getGcConsistentTimestamp(tokenRanges)

          // no more versions in cache, fetch new batch
          var nextToken = lastToken
          if (tableVersionsCache.size == 0) {
            val fetched = trx.getTopMostVersions(table, lastToken, toToken, consistentTimestamp, versionBatchSize)
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
              trx.getTopMostVersions(table, record.token, record.token, consistentTimestamp, versionBatchSize).find(
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

    /**
     * Returns the consistent timestamp that must be used by GC. The consistent timestamp exclude the transactions
     * currently read for replication.
     */
    private def getGcConsistentTimestamp(ranges: Seq[TokenRange]): Timestamp = {
      val rangesReadStartTimestamps = openReadIterators.withFilter(!_.ranges.intersect(ranges).isEmpty).map(_.from)
      (getCurrentConsistentTimestamp(ranges) :: rangesReadStartTimestamps).min
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


