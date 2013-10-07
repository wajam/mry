package com.wajam.mry.storage.mysql

import com.wajam.mry.storage.StorageTransaction
import java.sql.ResultSet
import com.wajam.mry.execution._
import com.wajam.mry.api.ProtocolTranslator
import com.wajam.tracing.Traced
import java.util.concurrent.atomic.AtomicInteger
import com.wajam.nrv.service.TokenRange
import collection.mutable.ArrayBuffer
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Mysql storage transaction
 */
class MysqlTransaction(private val storage: MysqlStorage, private val context: Option[ExecutionContext])
  extends StorageTransaction {

  private[mry] val tableMutationsCount = storage.model.allHierarchyTables.map(table => (table, new AtomicInteger(0))).toMap
  private var lazilyReadValues = List[Value]()

  val connection = storage.getConnection
  try {
    connection.setAutoCommit(false)
  } catch {
    case ex: Exception => {
      if (connection != null) {
        connection.close()
      }
      throw ex
    }
  }

  private def metrics = storage.transactionMetrics

  def rollback() {
    if (this.connection != null) {
      metrics.metricRollback.time {
        try {
          this.connection.rollback()
        } finally {
          this.connection.close()
        }
      }
    }

    storage.closeStorageTransaction(this)
  }

  private[mry] def markAsLazyRead(value: Value) {
    lazilyReadValues = value :: lazilyReadValues
  }

  private[mry] def loadLazyValues() {
    lazilyReadValues.foreach(_.serializableValue)
    lazilyReadValues = List()
  }

  def commit() {
    if (this.connection != null) {
      metrics.metricCommit.time {
        try {
          this.connection.commit()
        } finally {
          this.connection.close()
        }
      }
    }

    storage.closeStorageTransaction(this)
  }

  def set(table: Table, token: Long, timestamp: Timestamp, accessPath: AccessPath, optRecord: Option[Record]) {
    assert(accessPath.length == table.depth)

    val keysValue = accessPath.keys
    val fullTableName = table.depthName("_")

    val sqlKeys = (for (i <- 1 to keysValue.length) yield "k%d".format(i)).mkString(",")
    val sqlValues = (for (i <- 1 to table.depth) yield "?").mkString(",")


    /* Generated SQL looks like:
     *
     *   INSERT INTO `table1_table1_1_table1_1_1_data` (`tk`,k1,k2,k3,`ts`)
     *   VALUES (2517541033, 'k1', 'k1.2', 'k1.2.1', 1343138009222)
     *   ON DUPLICATE KEY UPDATE ts=VALUES(ts)', with params
     */
    val indexSql = "INSERT INTO `%s_index` (`ts`,`tk`,%s) VALUES (?,?,%s) ON DUPLICATE KEY UPDATE ts=VALUES(ts)".format(fullTableName, sqlKeys, sqlValues)

    /* Generated SQL looks like:
     *
     *   INSERT INTO `table1_table1_1_table1_1_1_data` (k1,k2,k3,`ts`,`ec`,`d`)
     *   VALUES ('k1', 'k1.2', 'k1.2.1', 1343138009222, 0, '...BLOB BYTES...')
     *   ON DUPLICATE KEY UPDATE d=VALUES(d)', with params
     */
    val dataSql = "INSERT INTO `%s_data` (`ts`,`tk`,%s,`ec`,`d`) VALUES (?,?,%s,?,?) ON DUPLICATE KEY UPDATE d=VALUES(d)".format(fullTableName, sqlKeys, sqlValues)

    optRecord match {
      case Some(record) =>
        // there is a value, we set it
        val value = record.serializeValue(storage.valueSerializer)
        metrics.tableMetricSet(table).time {
          storage.executeSqlUpdate(connection, indexSql, (Seq(timestamp.value) ++ Seq(token) ++ keysValue): _*)
          storage.executeSqlUpdate(connection, dataSql, (Seq(timestamp.value) ++ Seq(token) ++ keysValue ++ Seq(record.encoding) ++ Seq(value)): _*)
        }
        this.tableMutationsCount(table).incrementAndGet()

      case None =>
        // no value, it's a delete
        metrics.tableMetricDelete(table).time {
          storage.executeSqlUpdate(connection, indexSql, (Seq(timestamp.value) ++ Seq(token) ++ keysValue): _*)
          storage.executeSqlUpdate(connection, dataSql, (Seq(timestamp.value) ++ Seq(token) ++ keysValue ++ Seq(0) ++ Seq(null)): _*)
          this.tableMutationsCount(table).incrementAndGet()

          // delete all rows from children tables
          for (childTable <- table.allHierarchyTables) {
            val childFullTableName = childTable.depthName("_")
            val childSelectKeys = (for (i <- 1 to childTable.depth) yield "k%d".format(i)).mkString(",")
            val parentWhereKeys = (for (i <- 1 to keysValue.length) yield "k%d = ?".format(i)).mkString(" AND ")

            this.tableMutationsCount(childTable).incrementAndGet()

            /* Generated SQL looks like:
            *
            *     INSERT INTO `table1_table1_1_data`
            *       SELECT tk, k1, k2, 1343138009222 AS ts, 0 AS ec, NULL AS d
            *       FROM `table1_table1_1_index`
            *       WHERE tk = 2517541033 AND ts <= 1343138009222 AND k1 = 'k1'
            *       GROUP BY tk, k1, k2
            *     ON DUPLICATE KEY UPDATE d=VALUES(d)
            */
            val childDataSql = """
                 INSERT INTO `%1$s_data`
                  SELECT tk, %2$s, ? AS ts, 0 AS ec, NULL AS d
                  FROM `%1$s_index`
                  WHERE tk = ? AND ts <= ? AND %3$s
                  GROUP BY tk, %2$s
                 ON DUPLICATE KEY UPDATE d=VALUES(d)
                               """.format(childFullTableName, childSelectKeys, parentWhereKeys)
            storage.executeSqlUpdate(connection, childDataSql, (Seq(timestamp.value) ++ Seq(token) ++ Seq(timestamp.value) ++ keysValue): _*)


            /* Generated SQL looks like:
            *
            *     INSERT INTO `table1_table1_1_index`
            *       SELECT 1343138009222 AS ts, tk, k1, k2
            *       FROM `table1_table1_1_index`
            *       WHERE tk = 2517541033 AND ts <= 1343138009222 AND k1 = 'k1'
            *       GROUP BY tk, k1, k2
            *     ON DUPLICATE KEY UPDATE d=VALUES(d)
            */
            val childIndexSql = """
                 INSERT INTO `%1$s_index`
                  SELECT ? AS ts, tk, %2$s
                  FROM `%1$s_index`
                  WHERE tk = ? AND ts <= ? AND %3$s
                  GROUP BY tk, %2$s
                 ON DUPLICATE KEY UPDATE ts=VALUES(ts)
                                """.format(childFullTableName, childSelectKeys, parentWhereKeys)
            storage.executeSqlUpdate(connection, childIndexSql, (Seq(timestamp.value) ++ Seq(token) ++ Seq(timestamp.value) ++ keysValue): _*)
          }
        }
    }
  }

  def getMultiple(table: Table, token: Long, timestamp: Timestamp, accessPath: AccessPath,
                  includeDeleted: Boolean = false, optOffset: Option[Long] = None,
                  optCount: Option[Long] = None): RecordIterator = {
    assert(accessPath.length >= 1)

    val keysValue = accessPath.keys
    val fullTableName = table.depthName("_")
    val outerProjKeys = (for (i <- 1 to table.depth) yield "o.k%1$d".format(i)).mkString(",")
    val innerProjKeys = (for (i <- 1 to table.depth) yield "i.k%1$d".format(i)).mkString(",")
    val outerWhereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = o.k%1$d".format(i)).mkString(" AND ")
    val innerWhereKeys = (for (i <- 1 to accessPath.parts.length) yield "i.k%d = ?".format(i)).mkString(" AND ")

    val limitSql = (optOffset, optCount) match {
      case (Some(offset), Some(count)) => "LIMIT %d, %d".format(offset, count)
      case (Some(offset), None) => "LIMIT %d".format(offset)
      case (None, Some(count)) => "LIMIT 0, %d".format(count)
      case (None, None) => ""
    }

    /* Generated SQL looks like:
     *
     *   SELECT i.max_ts, i.tk, o.ec, o.d, o.k1
     *   FROM `table1_data` AS o, (
     *       SELECT i.tk, MAX(i.ts) AS max_ts, i.k1
     *       FROM `table1_index` AS i
     *       WHERE i.ts <= ? AND i.tk = ? AND i.k1 = ?
     *       GROUP BY i.tk, i.k1
     *       LIMIT 0,1000
     *   ) AS i
     *   WHERE o.tk = i.tk
     *   AND i.k1 = o.k1
     *   AND o.ts = i.max_ts
     *   AND o.d IS NOT NULL
     */
    var sql = """
        SELECT i.max_ts, i.tk, o.ec, o.d, %1$s
        FROM `%2$s_data` AS o, (
            SELECT i.tk, MAX(i.ts) AS max_ts, %3$s
            FROM `%2$s_index` AS i
            WHERE i.tk = ? AND %4$s AND i.ts <= ?
            GROUP BY i.tk, %3$s
            %5$s
        ) AS i
        WHERE o.tk = i.tk AND %6$s
        AND o.ts = i.max_ts
              """.format(outerProjKeys, fullTableName, innerProjKeys, innerWhereKeys, limitSql, outerWhereKeys)


    if (!includeDeleted)
      sql += " AND o.d IS NOT NULL "

    var results: SqlResults = null
    try {
      metrics.tableMetricGet(table).time {
        results = storage.executeSql(connection, false, sql, (Seq(token) ++ keysValue ++ Seq(timestamp.value)): _*)
      }

      new RecordIterator(storage, results, table)

    } catch {
      case e: Exception =>
        if (results != null)
          results.close()
        throw e
    }
  }

  def get(table: Table, token: Long, timestamp: Timestamp, accessPath: AccessPath, includeDeleted: Boolean = false): Option[Record] = {
    assert(accessPath.length == table.depth)

    val iter = getMultiple(table, token, timestamp, accessPath, includeDeleted)

    if (iter.next()) {
      Some(iter.record)
    } else {
      None
    }
  }

  def getTimeline(table: Table, timestamp: Timestamp, count: Int, ranges: Seq[TokenRange] = Seq(TokenRange.All),
                  selectMode: TimelineSelectMode = TimelineSelectMode.FromTimestamp): Seq[MutationRecord] = {
    val projOuterKeys = (for (i <- 1 to table.depth) yield "q1.k%1$d".format(i)).mkString(",")
    val projInnerKeys = (for (i <- 1 to table.depth) yield "ai.k%1$d".format(i)).mkString(",")
    val whereKeys1 = (for (i <- 1 to table.depth) yield "q1.k%1$d = ad.k%1$d".format(i)).mkString(" AND ")
    val whereKeys2 = (for (i <- 1 to table.depth) yield "ai.k%1$d = bi.k%1$d".format(i)).mkString(" AND ")
    val whereKeys3 = (for (i <- 1 to table.depth) yield "ci.k%1$d = ai.k%1$d".format(i)).mkString(" AND ")
    val whereKeys4 = (for (i <- 1 to table.depth) yield "q1.k%1$d = bd.k%1$d".format(i)).mkString(" AND ")
    val fullTableName = table.depthName("_")
    val lastConsistentTimestamp = storage.getCurrentConsistentTimestamp(ranges)

    val innerWhere = (Seq(
      ranges.map(r => "(ai.tk >= %1$d AND ai.tk <= %2$d)".format(r.start, r.end)).mkString("(", "OR ", ")")
    ) ++ (selectMode match {
      case TimelineSelectMode.FromTimestamp => Seq(
        """
              AND ai.ts >= %1$d AND ai.ts <= %3$d
              ORDER BY ai.ts ASC
              LIMIT 0, %2$d
        """.format(timestamp.value, count, lastConsistentTimestamp.value)
      )
      case TimelineSelectMode.AtTimestamp => Seq("AND ai.ts = %1$d".format(timestamp.value))
    })).mkString


    /* Generated SQL looks like:
     *
     *     SELECT q1.tk, ad.ts, ad.ec, ad.d, bd.ts, bd.ec, bd.d, q1.k1, q1.k2, q1.k3
     *     FROM (
     *       SELECT ai.tk, ai.ts AS new_ts, bi.ts AS old_ts, ai.k1,ai.k2,ai.k3
     *       FROM `table1_index` AS ai
     *       LEFT JOIN `table1_index` AS bi ON (ai.k1 = bi.k1 AND ai.k2 = bi.k2 AND ai.k3 = bi.k3 AND ai.tk = bi.tk AND bi.ts = (
     *         SELECT MAX(ci.ts)
     *         FROM `table1_index` AS ci
     *         WHERE ci.tk = ai.tk AND ci.k1 = ai.k1 AND ci.k2 = ai.k2 AND ci.k3 = ai.k3
     *         AND ci.ts < ai.ts
     *       ))
     *       WHERE ((ai.tk >= 3221225461 AND ai.tk <= 3310703945))
     *       AND ai.ts >= 13667145296450014 AND ai.ts <= 13667318799830001
     *       ORDER BY ai.ts ASC
     *       LIMIT 0, 100
     *     ) AS q1
     *     JOIN `table1_data` AS ad ON (q1.k1 = ad.k1 AND q1.k2 = ad.k2 AND q1.k3 = ad.k3 AND q1.tk = ad.tk AND q1.new_ts = ad.ts)
     *     LEFT JOIN `table1_data` AS bd ON (q1.k1 = bd.k1 AND q1.k2 = bd.k2 AND q1.k3 = bd.k3 AND q1.tk = bd.tk AND q1.old_ts = bd.ts)
     *     ORDER BY ad.ts ASC;
     */
    val sql = """
                SELECT q1.tk, ad.ts, ad.ec, ad.d, bd.ts, bd.ec, bd.d, %1$s
                FROM (
                    SELECT ai.tk, ai.ts AS new_ts, bi.ts AS old_ts, %8$s
                    FROM `%2$s_index` AS ai
                    LEFT JOIN `%2$s_index` AS bi ON (%4$s AND ai.tk = bi.tk AND bi.ts = (
                        SELECT MAX(ci.ts)
                        FROM `%2$s_index` AS ci
                        WHERE ci.tk = ai.tk AND %5$s
                        AND ci.ts < ai.ts
                    ))
                    WHERE %7$s
                ) AS q1
                JOIN `%2$s_data` AS ad ON (%3$s AND q1.tk = ad.tk AND q1.new_ts = ad.ts)
                LEFT JOIN `%2$s_data` AS bd ON (%6$s AND q1.tk = bd.tk AND q1.old_ts = bd.ts)
                ORDER BY ad.ts ASC;
              """.format(projOuterKeys, fullTableName, whereKeys1, whereKeys2, whereKeys3, whereKeys4, innerWhere, projInnerKeys)

    val ret = ArrayBuffer[MutationRecord]()
    var results: SqlResults = null
    try {
      metrics.tableMetricTimeline(table).time {
        results = storage.executeSql(connection, false, sql)

        while (results.resultset.next()) {
          val mut = new MutationRecord
          mut.load(storage.valueSerializer, results.resultset, table.depth)
          ret += mut
        }
      }

    } finally {
      if (results != null)
        results.close()
    }

    ret
  }

  def getTopMostVersions(table: Table, fromToken: Long, toToken: Long, lastConsistentTimestamp: Timestamp,
                         count: Int): Seq[VersionRecord] = {
    val projKeys = (for (i <- 1 to table.depth) yield "t.k%1$d".format(i)).mkString(",")
    val fullTableName = table.depthName("_")

    /*
     * Generated SQL looks like:
     *
     *    SELECT t.tk, COUNT(*) AS nb, GROUP_CONCAT(t.ts SEPARATOR ',') AS timestamps, t.k1,t.k2,t.k3
     *    FROM `table1_table1_1_table1_1_1_index` AS t
     *    WHERE t.tk >= 0 AND t.tk <= 10000
     *    AND t.ts <= 13631089220001
     *    GROUP BY t.tk,t.k1,t.k2,t.k3
     *    HAVING COUNT(*) > 3
     *    LIMIT 0, 100
     */
    val sql =
      """
         SELECT t.tk, COUNT(*) AS nb, GROUP_CONCAT(t.ts SEPARATOR ',') AS timestamps, %1$s
         FROM `%2$s_index` AS t
         WHERE t.tk >= %3$d AND t.tk <= %4$d
         AND t.ts <= %7$d
         GROUP BY t.tk, %1$s
         HAVING COUNT(*) > %5$d
         LIMIT 0, %6$d
      """.format(projKeys, fullTableName, fromToken, toToken, table.maxVersions, count, lastConsistentTimestamp.value)


    val ret = ArrayBuffer[VersionRecord]()
    var results: SqlResults = null
    try {
      metrics.tableMetricTopMostVersions(table).time {
        results = storage.executeSql(connection, false, sql)

        while (results.resultset.next()) {
          val version = new VersionRecord
          version.load(results.resultset, table.depth)
          ret += version
        }
      }

    } finally {
      if (results != null)
        results.close()
    }

    ret
  }

  def truncateVersions(table: Table, token: Long, accessPath: AccessPath, versions: Seq[Timestamp]) {
    val fullTableName = table.depthName("_")
    val whereKeys = (for (i <- 1 to table.depth) yield "k%1$d = ?".format(i)).mkString(" AND ")
    val keysValue = accessPath.keys

    /*
     * Generated SQL looks like:
     *
     *     DELETE FROM `table2_table2_1_table2_1_1_index`
     *     WHERE tk = ? AND k1 = ? AND k2 = ? AND k3 = ?
     *     AND ts IN (3242343243243, 34243423434);
     */
    val indexSql = """
                DELETE FROM `%1$s_index`
                WHERE tk = ? AND %2$s
                AND ts IN (%3$s);
                   """.format(fullTableName, whereKeys, versions.mkString(","))

    /*
     * Generated SQL looks like:
     *
     *     DELETE FROM `table2_table2_1_table2_1_1_data`
     *     WHERE tk = ? AND k1 = ? AND k2 = ? AND k3 = ?
     *     AND ts IN (3242343243243, 34243423434);
     */
    val dataSql = """
                DELETE FROM `%1$s_data`
                WHERE tk = ? AND %2$s
                AND ts IN (%3$s);
                  """.format(fullTableName, whereKeys, versions.mkString(","))

    metrics.tableMetricTruncateVersions(table).time {
      storage.executeSqlUpdate(connection, indexSql, (Seq(token) ++ keysValue): _*)
      storage.executeSqlUpdate(connection, dataSql, (Seq(token) ++ keysValue): _*)
    }
  }

  def truncateVersion(table: Table, token: Long, version: Timestamp) {
    val fullTableName = table.depthName("_")

    /*
     * Generated SQL looks like:
     *
     *     DELETE FROM `table2_table2_1_table2_1_1_index`
     *     WHERE tk = ? AND ts = ?;
     */
    val indexSql = """
                DELETE FROM `%1$s_index`
                WHERE tk = ? AND ts = ?;
                   """.format(fullTableName)

    /*
     * Generated SQL looks like:
     *
     *     DELETE FROM `table2_table2_1_table2_1_1_data`
     *     WHERE tk = ? AND ts = ?;
     */
    val dataSql = """
                DELETE FROM `%1$s_data`
                WHERE tk = ? AND ts = ?;
                  """.format(fullTableName)

    metrics.tableMetricTruncateVersion(table).time {
      storage.executeSqlUpdate(connection, indexSql, (Seq(token, version.value)): _*)
      storage.executeSqlUpdate(connection, dataSql, (Seq(token, version.value)): _*)
    }
  }

  def getSize(table: Table): Long = {
    val fullTableName = table.depthName("_")

    /*
     * Generated SQL looks like:
     *
     *     SELECT COUNT(*) AS count
     *     FROM `table2_1_1_index`
     */
    val sql = """
                SELECT COUNT(*) AS count
                FROM `%1$s_index`
              """.format(fullTableName)

    var count: Long = 0
    var results: SqlResults = null
    try {
      metrics.tableMetricSize(table).time {
        results = storage.executeSql(connection, false, sql)
      }

      if (results.resultset.next()) {
        count = results.resultset.getLong("count")
      }
    } finally {
      if (results != null)
        results.close()
    }

    count
  }

  /**
   * Generates a SQL where clause that will keep only rows after given columns and their values.
   *
   * Ex: We want only people that have name higher than "A" and then higher than 1000$ in salary
   * genWhereHigherEqualTuple(Map("name" -> "A", "salary" -> 1000))
   * returns (name > "A") OR (name == "A" AND salary >= 1000), but with placeholders (?) instead
   * of values and values in second position of the tuple
   */
  protected[mry] def genWhereHigherEqualTuple(keyVal: Map[String, Any]): (String, Seq[Any]) = {
    val keys: Seq[String] = keyVal.keys.toSeq
    val vals: Seq[Any] = keyVal.values.toSeq
    val count = keys.size

    val ors = Seq.range(0, count).map(a => {
      val eqs = Seq.range(0, a).map(b => {
        ("%s = ?".format(keys(b)), vals(b))
      })

      val op = if (a == count - 1) ">=" else ">"
      val ands = eqs.map(_._1) ++ Seq("%s %s ?".format(keys(a), op))
      (ands.mkString("(", ") AND (", ")"), eqs.map(_._2) ++ Seq(vals(a)))
    })

    (ors.map(_._1).mkString("(", ") OR (", ")"), ors.flatMap(_._2))
  }

  def getAllLatest(table: Table, count: Long, range: TokenRange = TokenRange.All,
                   optFromRecord: Option[Record] = None, includeDeleted: Boolean = false): RecordIterator = {

    val fullTableName = table.depthName("_")
    val outerProjKeys = (for (i <- 1 to table.depth) yield "o.k%1$d".format(i)).mkString(",")
    val innerProjKeys = (for (i <- 1 to table.depth) yield "i.k%1$d".format(i)).mkString(",")
    val outerWhereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = o.k%1$d".format(i)).mkString(" AND ")
    val recordPosition: (String, Seq[Any]) = optFromRecord match {
      case Some(fromRecord) =>

        try {
          val keys = Seq.range(0, fromRecord.accessPath.parts.size).map(i => "i.k%d".format(i + 1)).zip(fromRecord.accessPath.parts.map(_.key))
          this.genWhereHigherEqualTuple((Seq(("i.tk", scala.math.max(range.start, fromRecord.token))) ++ keys).toMap)
        } catch {
          case e: Exception => e.printStackTrace()
          ("", Seq())
        }

      case None => ("i.tk >= %d".format(range.start), Seq())
    }
    val lastConsistentTimestamp = storage.getCurrentConsistentTimestamp(Seq(range))
    val deletedFilter = if (includeDeleted) "" else "AND o.d IS NOT NULL"

    /* Generated SQL looks like:
     *
     *   SELECT i.max_ts, i.tk, o.ec, o.d, o.k1
     *   FROM `table1_data` AS o, (
     *       SELECT i.tk, MAX(i.ts) AS max_ts, i.k1
     *       FROM `table1_index` AS i
     *       WHERE (((i.tk > 4025886270)) OR ((i.tk = 4025886270) AND (k1 > 'key15')))
     *       AND i.tk <= 4525886270
     *       AND i.ts <= 13631089220001
     *       GROUP BY i.tk, i.k1
     *       ORDER BY i.tk, i.k1
     *       LIMIT 0, 100
     *   ) AS i
     *   WHERE o.tk = i.tk
     *   AND i.k1 = o.k1
     *   AND o.ts = i.max_ts
     *   AND o.d IS NOT NULL
     *   ORDER BY i.tk, o.k1;
     */
    val sql = """
        SELECT i.max_ts, i.tk, o.ec, o.d, %1$s
        FROM `%2$s_data` AS o, (
            SELECT i.tk, MAX(i.ts) AS max_ts, %3$s
            FROM `%2$s_index` AS i
            WHERE (%4$s)
            AND i.tk <= %7$d
            AND i.ts <= %8$d
            GROUP BY i.tk, %3$s
            ORDER BY i.tk, %3$s
            LIMIT 0, %6$d
        ) AS i
        WHERE o.tk = i.tk
        AND %5$s
        AND o.ts = i.max_ts
        %9$s
        ORDER BY i.tk, %1$s
              """.format(outerProjKeys, fullTableName, innerProjKeys, recordPosition._1, outerWhereKeys,
      count, range.end, lastConsistentTimestamp.value, deletedFilter)

    var results: SqlResults = null
    try {
      metrics.tableMetricGetAllLatest(table).time {
        results = storage.executeSql(connection, false, sql, (recordPosition._2): _*)
      }

      new RecordIterator(storage, results, table)

    } catch {
      case e: Exception =>
        if (results != null)
          results.close()
        throw e
    }
  }

  /**
   * Returns tombstone records from the specified table within the specified token range and older than the specified
   * minTombstoneAge. Does not returns more records than the specified count. Can resume from an optional record if the
   * max was reach in a previous call.
   *
   * @param minTombstoneAge Only tomsbstone older or equals than the current consistent timestamp minus minTombstoneAge
   *                        are returned by this method.
   */
  def getTombstoneRecords(table: Table, count: Long, range: TokenRange, minTombstoneAge: Long,
                          optFromRecord: Option[TombstoneRecord] = None): Seq[TombstoneRecord] = {
    val fullTableName = table.depthName("_")
    val tableDepth = table.depth
    val projKeys = (for (i <- 1 to tableDepth) yield "k%1$d".format(i)).mkString(",")
    val recordPosition: (String, Seq[Any]) = optFromRecord match {
      case Some(fromRecord) => {
        val keys = Seq.range(0, fromRecord.accessPath.parts.size).map(i => "k%d".format(i + 1)).zip(fromRecord.accessPath.parts.map(_.key))
        this.genWhereHigherEqualTuple((Seq(("tk", scala.math.max(range.start, fromRecord.token))) ++ keys).toMap)
      }
      case None => ("tk >= %d".format(range.start), Seq())
    }
    val maxTimestamp: Timestamp = storage.getCurrentConsistentTimestamp(Seq(range)).value - minTombstoneAge

    /*
     * Generated SQL looks like:
     *
     *    SELECT tk, ts, k1
     *    FROM `table1_data`
     *    WHERE (((tk > 4025886270)) OR ((tk = 4025886270) AND (k1 > 'key15')))
     *    AND tk <= 4525886270
     *    AND ts <= 13631089220001
     *    AND d IS NULL
     *    ORDER BY tk, k1
     *    LIMIT 0, 100;
     */
    val sql =
      """
         SELECT tk, ts, %1$s
         FROM `%2$s_data`
         WHERE (%3$s)
         AND tk <= %4$d
         AND ts <= %5$d
         AND d IS NULL
         ORDER BY tk, %1$s
         LIMIT 0, %6$d;
      """.format(projKeys, fullTableName, recordPosition._1, range.end, maxTimestamp.value, count)

    val ret = ArrayBuffer[TombstoneRecord]()
    metrics.tableMetricGetTombstoneRecords(table).time {
      var results: SqlResults = null
      try {
        results = storage.executeSql(connection, update = false, sql, recordPosition._2: _*)

        while (results.resultset.next()) {
          ret += TombstoneRecord(results.resultset, table, tableDepth)
        }
      } finally {
        if (results != null)
          results.close()
      }
    }
    ret
  }

  /**
   * Delete the specified tombstone record and all its preceding records i.e. older record with exact same keys.
   * Returns the number of records deleted.
   */
  def deleteTombstoneAndOlder(record: TombstoneRecord) = {
    val fullTableName = record.table.depthName("_")
    val whereKeys = (for (i <- 1 to record.accessPath.parts.length) yield "k%d = ?".format(i)).mkString(" AND ")
    val params = Seq(record.token) ++ record.accessPath.keys ++ Seq(record.timestamp.value)

    /*
     * Generated SQL looks like:
     *
     *     DELETE FROM `table_index`
     *     WHERE tk = ? AND k1 = ? AND k2 = ? AND k3 = ? AND ts <= ?;
     */
    val indexSql = """
                DELETE FROM `%1$s_index`
                WHERE tk = ? AND %2$s AND ts <= ?;
                   """.format(fullTableName, whereKeys)

    /*
     * Generated SQL looks like:
     *
     *     DELETE FROM `table_data`
     *     WHERE tk = ? AND k1 = ? AND k2 = ? AND k3 = ? AND ts <= ?;
     */
    val dataSql = """
                DELETE FROM `%1$s_data`
                WHERE tk = ? AND %2$s AND ts <= ?;
                  """.format(fullTableName, whereKeys)


    metrics.tableMetricDeleteTombstone(record.table).time {
      storage.executeSqlUpdate(connection, indexSql, params: _*)
      storage.executeSqlUpdate(connection, dataSql, params: _*)
    }
  }

  def getLastTimestamp(table: Table, ranges: Seq[TokenRange]): Option[Timestamp] = {
    getFirstTableTimestamp(table, ranges, sortAscending = false)
  }

  def getFirstTimestamp(table: Table, ranges: Seq[TokenRange]): Option[Timestamp] = {
    getFirstTableTimestamp(table, ranges, sortAscending = true)
  }

  private def getFirstTableTimestamp(table: Table, ranges: Seq[TokenRange], sortAscending: Boolean): Option[Timestamp] = {

    val whereRanges = ranges.map(r => "(i.tk >= %1$d AND i.tk <= %2$d)".format(r.start, r.end)).mkString("(", "OR ", ")")
    val fullTableName = table.depthName("_")

    /* Generated SQL looks like:
     *
     *   SELECT i.ts AS ts
     *   FROM `table1_index` AS i
     *   WHERE ((i.tk >= 0 AND i.tk <= 89478485) OR (i.tk >= 536870910 AND i.tk <= 626349395));
     *   ORDER BY i.ts DESC
     *   LIMIT 0,1;
     */
    val sql = """
        SELECT i.ts AS ts
        FROM `%1$s_index` AS i
        WHERE %2$s
        ORDER BY i.ts %3$s
        LIMIT 0,1;
              """.format(fullTableName, whereRanges, if (sortAscending) "ASC" else "DESC")

    metrics.tableMetricGetLastTimestamp(table).time {
      var results: SqlResults = null
      try {
        results = storage.executeSql(connection, update = false, sql)
        if (results.resultset.next()) {
          val value = results.resultset.getLong("ts")
          if (results.resultset.wasNull()) {
            None
          } else {
            Some(Timestamp(value))
          }
        } else {
          None
        }
      } finally {
        if (results != null)
          results.close()
      }
    }
  }

  def getTransactionSummaryRecords(table: Table, fromTimestamp: Timestamp, toTimestamp: Timestamp, count: Int,
                                   ranges: Seq[TokenRange]): Seq[TransactionSummaryRecord] = {
    val fullTableName = table.depthName("_")
    val whereRanges = ranges.map(r => "(i.tk >= %1$d AND i.tk <= %2$d)".format(r.start, r.end)).mkString("(", "OR ", ")")

    /* Generated SQL looks like:
     *
     *     SELECT i.tk , i.ts, COUNT(*) AS count
     *     FROM `table1_index` AS i
     *     WHERE ((i.tk >= 0 AND i.tk <= 89478485) OR (i.tk >= 536870910 AND i.tk <= 626349395))
     *     AND i.ts >= 0 AND i.ts <= 864000000000
     *     GROUP BY i.tk, i.ts
     *     ORDER BY i.ts ASC
     *     LIMIT 0, 100;
     */
    val sql = """
                SELECT i.tk , i.ts, COUNT(*) AS count
                FROM `%1$s_index` AS i
                WHERE %2$s
                AND i.ts >= %3$d AND i.ts <= %5$d
                GROUP BY i.ts, i.tk
                ORDER BY i.ts ASC
                LIMIT 0, %4$d;
              """.format(fullTableName, whereRanges, fromTimestamp.value, count, toTimestamp.value)

    var ret = List[TransactionSummaryRecord]()
    metrics.tableMetricGetTransactionSummaryRecords(table).time {
      var results: SqlResults = null
      try {
        results = storage.executeSql(connection, false, sql)

        while (results.resultset.next()) {
          ret = TransactionSummaryRecord(results.resultset, table) :: ret
        }
      } finally {
        if (results != null)
          results.close()
      }
    }
    ret
  }

  def getTransactionRecords(table: Table, from: Timestamp, to: Timestamp, ranges: Seq[TokenRange]): RecordIterator = {

    val fullTableName = table.depthName("_")
    val projKeys = (for (i <- 1 to table.depth) yield "i.k%1$d".format(i)).mkString(",")
    val whereRanges = ranges.map(r => "(i.tk >= %1$d AND i.tk <= %2$d)".format(r.start, r.end)).mkString("(", "OR ", ")")
    val whereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = d.k%1$d".format(i)).mkString(" AND ")

    /* Generated SQL looks like:
     *
     *   SELECT i.ts, i.tk, d.ec, d.d, i.k1
     *   FROM `table1_data` AS d, (
     *       SELECT i.ts, i.tk, i.k1
     *       FROM `table1_index` AS i
     *       WHERE ((i.tk >= 0 AND i.tk <= 89478485) OR (i.tk >= 536870910 AND i.tk <= 626349395))
     *       AND i.ts >= 13578286851700001 AND i.ts <= 13578286975770001
     *   ) AS i
     *   WHERE d.tk = i.tk
     *   AND d.k1 = i.k1
     *   AND d.ts = i.ts
     *   ORDER BY i.ts, i,k1;
     */
    var sql = """
        SELECT i.ts, i.tk, d.ec, d.d, %1$s
        FROM `%2$s_data` AS d, (
            SELECT i.ts, i.tk, %1$s
            FROM `%2$s_index` AS i
            WHERE %3$s
            AND i.ts >= %4$d AND i.ts <= %5$d
        ) AS i
        WHERE d.tk = i.tk
        AND %6$s
        AND d.ts = i.ts
        ORDER BY i.ts, %1$s;
              """.format(projKeys, fullTableName, whereRanges, from.value, to.value, whereKeys)

    metrics.tableMetricGetTransactionRecords(table).time {
      var results: SqlResults = null
      try {
        metrics.tableMetricGet(table).time {
          results = storage.executeSql(connection, false, sql)
        }

        new RecordIterator(storage, results, table)
      } catch {
        case e: Exception =>
          if (results != null)
            results.close()
          throw e
      }
    }
  }
}

object MysqlTransaction {

  class Metrics(storage: MysqlStorage) extends Traced {
    val metricCommit = tracedTimer("mysql-commit")
    val metricRollback = tracedTimer("mysql-rollback")
    val tableMetricTimeline = generateTablesTimers("mysql-timeline")
    val tableMetricGetAllLatest = generateTablesTimers("mysql-get-all-latests")
    val tableMetricTopMostVersions = generateTablesTimers("mysql-topmosversions")
    val tableMetricSet = generateTablesTimers("mysql-set")
    val tableMetricGet = generateTablesTimers("mysql-get")
    val tableMetricDelete = generateTablesTimers("mysql-delete")
    val tableMetricTruncateVersion = generateTablesTimers("mysql-truncateversion")
    val tableMetricTruncateVersions = generateTablesTimers("mysql-truncateversions")
    val tableMetricSize = generateTablesTimers("mysql-size")
    val tableMetricGetLastTimestamp = generateTablesTimers("mysql-getlastts")
    val tableMetricGetTransactionSummaryRecords = generateTablesTimers("mysql-gettxsummaryrecords")
    val tableMetricGetTransactionRecords = generateTablesTimers("mysql-gettxrecords")
    lazy val tableMetricGetTombstoneRecords = generateTablesTimers("mysql-get-tombstone-records")
    lazy val tableMetricDeleteTombstone = generateTablesTimers("mysql-delete-tombstone")

    private def generateTablesTimers(timerName: String) = storage.model.allHierarchyTables.map(table =>
      (table, tracedTimer(timerName, table.uniqueName))).toMap

    protected override def getTracedClass = classOf[MysqlTransaction]
  }

}

case class AccessKey(var key: String)

case class AccessPath(parts: Seq[AccessKey] = Seq()) {
  def last = parts.last

  def length = parts.length

  def keys = parts.map(p => p.key)

  def apply(pos: Int) = this.parts(pos)

  override def toString: String = (for (part <- parts) yield part.key).mkString("/")
}

case class Record(table: Table, var value: Value = new MapValue(Map()), var token: Long = 0,
                  var accessPath: AccessPath = new AccessPath(), var encoding: Byte = 0,
                  var timestamp: Timestamp = Timestamp(0)) {

  override def equals(that: Any) = that match {
    case that: Record => that.token.equals(this.token) && this.accessPath.keys.equals(that.accessPath.keys)
    case _ => false
  }

  def load(serializer: ProtocolTranslator, resultset: ResultSet, depth: Int) {
    this.timestamp = Timestamp(resultset.getLong(1))
    this.token = resultset.getLong(2)
    this.encoding = resultset.getByte(3)
    this.unserializeValue(serializer, resultset.getBytes(4))

    this.accessPath = new AccessPath(for (i <- 1 to depth) yield new AccessKey(resultset.getString(4 + i)))
  }

  def serializeValue(serializer: ProtocolTranslator): Array[Byte] = {
    serializer.encodeValue(value)
  }

  def unserializeValue(serializer: ProtocolTranslator, bytes: Array[Byte]) {
    if (bytes != null)
      this.value = serializer.decodeValue(bytes).asInstanceOf[MapValue]
    else
      this.value = NullValue
  }
}

object Record {
  def apply(table: Table, token: Long, timestamp: Timestamp, value: Map[String, Value], accessPath: String*): Record = {
    val record = Record(table, MapValue(value))
    record.accessPath = AccessPath(accessPath.map(AccessKey(_)))
    record.token = token
    record.timestamp = timestamp
    record
  }

  def apply(table: Table, token: Long, timestamp: Timestamp, value: Value, accessPath: String*): Record = {
    val record = Record(table, value)
    record.accessPath = AccessPath(accessPath.map(AccessKey(_)))
    record.token = token
    record.timestamp = timestamp
    record
  }
}

class MutationRecord {
  var token: Long = 0

  var accessPath = new AccessPath()
  var newTimestamp: Timestamp = Timestamp(0)
  var newEncoding: Byte = 0
  var newValue: Option[Value] = None
  var oldTimestamp: Option[Timestamp] = None
  var oldEncoding: Byte = 0
  var oldValue: Option[Value] = None

  def load(serializer: ProtocolTranslator, resultset: ResultSet, depth: Int) {
    this.token = resultset.getLong(1)
    this.newTimestamp = Timestamp(resultset.getLong(2))
    this.newEncoding = resultset.getByte(3)
    this.newValue = this.unserializeValue(serializer, resultset.getBytes(4))

    val oldTs = resultset.getObject(5)
    if (oldTs != null) {
      this.oldTimestamp = Some(Timestamp(oldTs.asInstanceOf[Long]))
      this.oldEncoding = resultset.getByte(6)
      this.oldValue = this.unserializeValue(serializer, resultset.getBytes(7))
    }

    this.accessPath = new AccessPath(for (i <- 1 to depth) yield new AccessKey(resultset.getString(7 + i)))
  }

  def unserializeValue(serializer: ProtocolTranslator, bytes: Array[Byte]): Option[Value] = {
    if (bytes != null)
      Some(serializer.decodeValue(bytes).asInstanceOf[MapValue])
    else
      None
  }
}

case class CompositeKey[T](keys: T*)(implicit ord: math.Ordering[T]) extends Comparable[CompositeKey[T]] {
  def compareTo(that: CompositeKey[T]) = math.Ordering.Iterable[T].compare(keys, that.keys)

  def head = CompositeKey(keys.head)

  def parent = CompositeKey(keys.dropRight(1): _*)

  def ancestors: List[CompositeKey[T]] = {
    if (keys.size > 1) {
      parent :: parent.ancestors
    } else {
      Nil
    }
  }
}

object CompositeKey {
  def apply(table: Table, keys: String*): CompositeKey[(String, String)] = {
    require(table.path.size == keys.size)
    new CompositeKey(table.path.map(_.name).zip(keys): _*)
  }

  def apply(record: Record): CompositeKey[(String, String)] = {
    CompositeKey(record.table, record.accessPath.keys: _*)
  }
}

class RecordIterator(storage: MysqlStorage, results: SqlResults, table: Table) extends Traversable[Record] {
  private var hasNext = false
  private var closed = false

  def foreach[U](f: (Record) => U) {
    while (this.next()) {
      f(record)
    }
    this.close()
  }

  def next(): Boolean = {
    this.hasNext = results.resultset.next()
    this.hasNext
  }

  def record: Record = {
    if (hasNext) {
      val record = new Record(table)
      record.load(storage.valueSerializer, results.resultset, table.depth)
      record
    } else {
      null
    }
  }

  def close() {
    if (!closed) {
      results.close()
      closed = true
    }
  }
}

class VersionRecord {
  var token: Long = 0
  var versionsCount: Int = 0
  var versions = Seq[Timestamp]()
  var accessPath = new AccessPath()

  def load(resultset: ResultSet, depth: Int) {
    this.token = resultset.getLong(1)
    this.versionsCount = resultset.getInt(2)
    this.versions = resultset.getString(3).split(",").map(vers => Timestamp(vers.toLong))
    this.accessPath = new AccessPath(for (i <- 1 to depth) yield new AccessKey(resultset.getString(3 + i)))
  }
}

case class TransactionSummaryRecord(table: Table, token: Long, timestamp: Timestamp, recordsCount: Int)

object TransactionSummaryRecord {
  def apply(resultset: ResultSet, table: Table): TransactionSummaryRecord = {
    val token = resultset.getLong(1)
    val timestamp = Timestamp(resultset.getLong(2))
    val recordsCount = resultset.getInt(3)
    TransactionSummaryRecord(table, token, timestamp, recordsCount)
  }
}

case class TombstoneRecord(table: Table, token: Long, accessPath: AccessPath, timestamp: Timestamp)

object TombstoneRecord {
  def apply(resultset: ResultSet, table: Table, depth: Int): TombstoneRecord = {
    val token = resultset.getLong(1)
    val timestamp = Timestamp(resultset.getLong(2))
    val accessPath = new AccessPath(for (i <- 0 until depth) yield new AccessKey(resultset.getString(3 + i)))
    TombstoneRecord(table, token, accessPath, timestamp)
  }
}

sealed trait TimelineSelectMode

object TimelineSelectMode {

  object FromTimestamp extends TimelineSelectMode

  object AtTimestamp extends TimelineSelectMode

}

