package com.wajam.mry.storage.mysql

import com.wajam.mry.storage.StorageTransaction
import java.sql.ResultSet
import collection.mutable
import com.wajam.mry.execution._
import com.wajam.mry.api.ProtocolTranslator
import com.yammer.metrics.scala.Instrumented
import com.wajam.scn.Timestamp
import com.wajam.nrv.tracing.Traced
import java.util.concurrent.atomic.AtomicInteger

/**
 * Mysql storage transaction
 */
class MysqlTransaction(private val storage: MysqlStorage, private val context: Option[ExecutionContext]) extends StorageTransaction with Instrumented with Traced {
  private val tableMetricTimeline = generateTablesTimers("mysql-timeline")
  private val tableMetricGetAllLatest = generateTablesTimers("mysql-timeline")
  private val tableMetricTopMostVersions = generateTablesTimers("mysql-topmosversions")
  private val tableMetricSet = generateTablesTimers("mysql-set")
  private val tableMetricGet = generateTablesTimers("mysql-get")
  private val tableMetricDelete = generateTablesTimers("mysql-delete")
  private val metricCommit = tracedTimer("mysql-commit")
  private val metricRollback = tracedTimer("mysql-rollback")
  private val tableMetricTruncateVersions = generateTablesTimers("mysql-truncateversions")
  private val tableMetricSize = generateTablesTimers("mysql-size")

  protected[mry] val tableMutationsCount = storage.model.allHierarchyTables.map(table => (table, new AtomicInteger(0))).toMap

  val connection = storage.getConnection
  connection.setAutoCommit(false)

  private def generateTablesTimers(timerName: String) = storage.model.allHierarchyTables.map(table =>
    (table, tracedTimer(timerName, table.uniqueName))).toMap

  def rollback() {
    if (this.connection != null) {
      this.metricRollback.time {
        try {
          this.connection.rollback()
        } finally {
          this.connection.close()
        }
      }
    }

    storage.closeStorageTransaction(this)
  }

  def commit() {
    if (this.connection != null) {
      this.metricCommit.time {
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
     *   INSERT INTO `table1_table1_1_table1_1_1` (`ts`,`tk`,`ec`,k1,k2,k3,`d`)
     *   VALUES (1343138009222, 2517541033, 0, 'k1', 'k1.2', 'k1.2.1', '...BLOB BYTES...')
     *   ON DUPLICATE KEY UPDATE d=VALUES(d)', with params
     */
    val sql = "INSERT INTO `%s` (`ts`,`tk`,`ec`,%s,`d`) VALUES (?,?,?,%s,?) ON DUPLICATE KEY UPDATE d=VALUES(d)".format(fullTableName, sqlKeys, sqlValues)

    var results: SqlResults = null
    try {
      optRecord match {
        case Some(record) =>
          // there is a value, we set it
          val value = record.serializeValue(storage.valueSerializer)
          this.tableMetricSet(table).time {
            results = storage.executeSql(connection, true, sql, (Seq(timestamp.value) ++ Seq(token) ++ Seq(record.encoding) ++ keysValue ++ Seq(value)): _*)
          }
          this.tableMutationsCount(table).incrementAndGet()

        case None =>
          // no value, it's a delete
          this.tableMetricDelete(table).time {
            results = storage.executeSql(connection, true, sql, (Seq(timestamp.value) ++ Seq(token) ++ Seq(0) ++ keysValue ++ Seq(null)): _*)
            this.tableMutationsCount(table).incrementAndGet()

            // delete all rows from children tables
            for (childTable <- table.allHierarchyTables) {
              val childFullTableName = childTable.depthName("_")
              val childSelectKeys = (for (i <- 1 to childTable.depth) yield "k%d".format(i)).mkString(",")
              val parentWhereKeys = (for (i <- 1 to keysValue.length) yield "k%d = ?".format(i)).mkString(" AND ")

              this.tableMutationsCount(childTable).incrementAndGet()

              /* Generated SQL looks like:
              *
              *     INSERT INTO `table1_table1_1`
              *       SELECT 1343138009222 AS ts, tk, ec, k1,k2, NULL AS d
              *       FROM `table1_table1_1`
              *       WHERE tk = 2517541033 AND ts <= 1343138009222 AND k1 = 'k1'
              *       GROUP BY tk, k1, k2
              *     ON DUPLICATE KEY UPDATE d=VALUES(d)
              */
              val childSql = """
                 INSERT INTO `%1$s`
                  SELECT ? AS ts, tk, ec, %2$s, NULL AS d
                  FROM `%1$s`
                  WHERE tk = ? AND ts <= ? AND %3$s
                  GROUP BY tk, %2$s
                 ON DUPLICATE KEY UPDATE d=VALUES(d)
                             """.format(childFullTableName, childSelectKeys, parentWhereKeys)

              results = storage.executeSql(connection, true, childSql, (Seq(timestamp.value) ++ Seq(token) ++ Seq(timestamp.value) ++ keysValue): _*)
            }
          }
      }


    } finally {
      if (results != null)
        results.close()
    }
  }

  def getMultiple(table: Table, token: Long, timestamp: Timestamp, accessPath: AccessPath, includeDeleted: Boolean = false): RecordIterator = {
    assert(accessPath.length >= 1)

    val keysValue = accessPath.keys
    val fullTableName = table.depthName("_")
    val outerProjKeys = (for (i <- 1 to table.depth) yield "o.k%1$d".format(i)).mkString(",")
    val innerProjKeys = (for (i <- 1 to table.depth) yield "i.k%1$d".format(i)).mkString(",")
    val outerWhereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = o.k%1$d".format(i)).mkString(" AND ")
    val innerWhereKeys = (for (i <- 1 to accessPath.parts.length) yield "i.k%d = ?".format(i)).mkString(" AND ")

    /* Generated SQL looks like:
     *
     *   SELECT o.ts, o.tk, o.ec, o.d, o.k1
     *   FROM `table1` AS o, (
     *       SELECT i.tk, MAX(i.ts) AS max_ts, i.k1
     *       FROM `table1` AS i
     *       WHERE i.ts <= ? AND i.tk = ? AND i.k1 = ?
     *       GROUP BY i.tk, i.k1
     *   ) AS i
     *   WHERE i.tk = o.tk
     *   AND i.k1 = o.k1
     *   AND o.ts = i.max_ts
     *   AND o.d IS NOT NULL
     */
    var sql = """
        SELECT o.ts, o.tk, o.ec, o.d, %1$s
        FROM `%2$s` AS o, (
            SELECT i.tk, MAX(i.ts) AS max_ts, %3$s
            FROM `%2$s` AS i
            WHERE i.tk = ? AND %4$s AND i.ts <= ?
            GROUP BY i.tk, %3$s
        ) AS i
        WHERE i.tk = o.tk
        AND %5$s
        AND o.ts = i.max_ts
              """.format(outerProjKeys, fullTableName, innerProjKeys, innerWhereKeys, outerWhereKeys)


    if (!includeDeleted)
      sql += " AND o.d IS NOT NULL "

    var results: SqlResults = null
    try {
      this.tableMetricGet(table).time {
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

  def getTimeline(table: Table, timestamp: Timestamp, count: Int,
                  selectMode: TimelineSelectMode = TimelineSelectMode.FromTimestamp): Seq[MutationRecord] = {
    val projKeys = (for (i <- 1 to table.depth) yield "c.k%1$d".format(i)).mkString(",")
    val whereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = c.k%1$d".format(i)).mkString(" AND ")
    val joinKeys = (for (i <- 1 to table.depth) yield "c.k%1$d = l.k%1$d".format(i)).mkString(" AND ")
    val fullTableName = table.depthName("_")

    /* Generated SQL looks like:
     *
     *   SELECT c.tk, c.ts, c.ec, c.d, l.ts, l.ec, l.d, c.k1, c.k2, c.k3, c.g3
     *   FROM `table1_table1_1_table1_1_1` AS c
     *   LEFT JOIN `table1_table1_1_table1_1_1` l
     *      ON (c.tk = l.tk AND c.k1 = l.k1 AND c.k2 = l.k2
     *          AND l.ts = ( SELECT MAX(i.ts)
     *                       FROM `table1_table1_1_table1_1_1` AS i USE INDEX(revkey)
     *                       WHERE i.tk = c.tk AND i.k1 = c.k1
     *                       AND i.k2 = c.k2
     *                       AND i.k3 = c.k3
     *                       AND i.ts < c.ts
     *                     )
     *   )
     *   WHERE c.ts >= 1343135748256
     *   ORDER BY c.ts ASC
     *   LIMIT 0, 100;
     */
    var sql = """
                SELECT c.tk, c.ts, c.ec, c.d, l.ts, l.ec, l.d, %1$s
                FROM `%2$s` AS c
                LEFT JOIN `%2$s` l ON (c.tk = l.tk AND %3$s AND l.ts = (
                 SELECT MAX(i.ts)
                 FROM `%2$s` AS i USE INDEX(revkey)
                 WHERE i.tk = c.tk AND %4$s AND i.ts < c.ts))
              """.format(projKeys, fullTableName, joinKeys, whereKeys)

    selectMode match {
      case TimelineSelectMode.FromTimestamp => {
        sql +=
          """
            WHERE c.ts >= %1$d
            ORDER BY c.ts ASC
            LIMIT 0, %2$d;
          """.format(timestamp.value, count)
      }
      case TimelineSelectMode.AtTimestamp => {
        sql +=
          """
            WHERE c.ts = %1$d;""".format(timestamp.value)
      }
    }

    var ret = mutable.LinkedList[MutationRecord]()
    var results: SqlResults = null
    try {
      this.tableMetricTimeline(table).time {
        results = storage.executeSql(connection, false, sql)

        while (results.resultset.next()) {
          val mut = new MutationRecord
          mut.load(storage.valueSerializer, results.resultset, table.depth)
          ret :+= mut
        }
      }

    } finally {
      if (results != null)
        results.close()
    }

    ret
  }

  def getTopMostVersions(table: Table, fromToken: Long, count: Int): Seq[VersionRecord] = {
    val projKeys = (for (i <- 1 to table.depth) yield "t.k%1$d".format(i)).mkString(",")
    val fullTableName = table.depthName("_")

    /*
     * Generated SQL looks like:
     *
     *    SELECT t.tk, t.ec, COUNT(*) AS nb, GROUP_CONCAT(t.ts SEPARATOR ',') AS timestamps, t.k1,t.k2,t.k3
     *    FROM `table1_table1_1_table1_1_1` AS t
     *    WHERE t.tk >= 0
     *    GROUP BY t.tk,t.k1,t.k2,t.k3
     *    HAVING COUNT(*) > 3
     *    LIMIT 0, 100
     */
    val sql =
      """
         SELECT t.tk, t.ec, COUNT(*) AS nb, GROUP_CONCAT(t.ts SEPARATOR ',') AS timestamps, %1$s
         FROM `%2$s` AS t
         WHERE t.tk >= %3$d
         GROUP BY t.tk, %1$s
         HAVING COUNT(*) > %4$d
         LIMIT 0, %5$d
      """.format(projKeys, fullTableName, fromToken, table.maxVersions, count)


    var ret = mutable.LinkedList[VersionRecord]()
    var results: SqlResults = null
    try {
      this.tableMetricTopMostVersions(table).time {
        results = storage.executeSql(connection, false, sql)

        while (results.resultset.next()) {
          val version = new VersionRecord
          version.load(results.resultset, table.depth)
          ret :+= version
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
     *     DELETE FROM `table2_table2_1_table2_1_1`
     *     WHERE tk = ?
     *     AND k1 = ? AND k2 = ? AND k3 = ?
     *     AND ts IN (3242343243243, 34243423434);
     */
    val sql = """
                DELETE FROM `%1$s`
                WHERE tk = ?
                AND %2$s
                AND ts IN (%3$s);
              """.format(fullTableName, whereKeys, versions.mkString(","))

    var results: SqlResults = null
    try {
      this.tableMetricTruncateVersions(table).time {
        results = storage.executeSql(connection, true, sql, (Seq(token) ++ keysValue): _*)
      }

    } finally {
      if (results != null)
        results.close()
    }
  }

  def getSize(table: Table): Long = {
    val fullTableName = table.depthName("_")

    /*
     * Generated SQL looks like:
     *
     *     SELECT COUNT(*) AS count
     *     FROM `table2_1_1`
     */
    val sql = """
                SELECT COUNT(*) AS count
                FROM `%1$s`
              """.format(fullTableName)

    var count: Long = 0
    var results: SqlResults = null
    try {
      this.tableMetricSize(table).time {
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

  def getAllLatest(table: Table, count: Long, optFromRecord: Option[Record] = None): RecordIterator = {

    val fullTableName = table.depthName("_")
    val outerProjKeys = (for (i <- 1 to table.depth) yield "o.k%1$d".format(i)).mkString(",")
    val innerProjKeys = (for (i <- 1 to table.depth) yield "i.k%1$d".format(i)).mkString(",")
    val outerWhereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = o.k%1$d".format(i)).mkString(" AND ")
    val recordPosition: (String, Seq[Any]) = optFromRecord match {
      case Some(fromRecord) =>

        try {
          val keys = Seq.range(0, fromRecord.accessPath.parts.size).map(i => "i.k%d".format(i + 1)).zip(fromRecord.accessPath.parts.map(_.key))
          this.genWhereHigherEqualTuple((Seq(("i.tk", fromRecord.token)) ++ keys).toMap)
        } catch {
          case e: Exception => e.printStackTrace()
          ("", Seq())
        }

      case None => ("i.tk >= 0", Seq())
    }


    /* Generated SQL looks like:
     *
     *   SELECT o.ts, o.tk, o.ec, o.d, o.k1
     *   FROM `table1` AS o, (
     *       SELECT i.tk, MAX(i.ts) AS max_ts, i.k1
     *       FROM `table1` AS i
     *       WHERE ((i.tk > 4025886270)) OR ((i.tk = 4025886270) AND (k1 > 'key15'))
     *       GROUP BY i.tk, i.k1
     *       ORDER BY i.tk, i.k1
     *   ) AS i
     *   WHERE i.tk = o.tk
     *   AND i.k1 = o.k1
     *   AND o.ts = i.max_ts
     *   AND o.d IS NOT NULL
     *   ORDER BY o.tk, o.k1
     *   LIMIT 0, 1000
     */
    val sql = """
        SELECT o.ts, o.tk, o.ec, o.d, %1$s
        FROM `%2$s` AS o, (
            SELECT i.tk, MAX(i.ts) AS max_ts, %3$s
            FROM `%2$s` AS i
            WHERE %4$s
            GROUP BY i.tk, %3$s
            ORDER BY i.tk, %3$s
        ) AS i
        WHERE i.tk = o.tk
        AND %5$s
        AND o.ts = i.max_ts
        AND o.d IS NOT NULL
        ORDER BY o.tk, %1$s
        LIMIT 0, ?
              """.format(outerProjKeys, fullTableName, innerProjKeys, recordPosition._1, outerWhereKeys)

    var results: SqlResults = null
    try {
      this.tableMetricGetAllLatest(table).time {
        results = storage.executeSql(connection, false, sql, (recordPosition._2 ++ Seq(count)): _*)
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

class AccessKey(var key: String)

class AccessPath(val parts: Seq[AccessKey] = Seq()) {
  def last = parts.last

  def length = parts.length

  def keys = parts.map(p => p.key)

  def apply(pos: Int) = this.parts(pos)

  override def toString: String = (for (part <- parts) yield part.key).mkString("/")
}

class Record(var value: Value = new MapValue(Map())) {
  var accessPath = new AccessPath()
  var token: Long = 0
  var encoding: Byte = 0
  var timestamp: Timestamp = Timestamp(0)

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
      this.value = NullValue()
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
      val record = new Record
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
  var encoding: Byte = 0
  var versionsCount: Int = 0
  var versions = Seq[Timestamp]()
  var accessPath = new AccessPath()

  def load(resultset: ResultSet, depth: Int) {
    this.token = resultset.getLong(1)
    this.encoding = resultset.getByte(2)
    this.versionsCount = resultset.getInt(3)
    this.versions = resultset.getString(4).split(",").map(vers => Timestamp(vers.toLong))
    this.accessPath = new AccessPath(for (i <- 1 to depth) yield new AccessKey(resultset.getString(4 + i)))
  }
}

sealed trait TimelineSelectMode

object TimelineSelectMode {

  object FromTimestamp extends TimelineSelectMode

  object AtTimestamp extends TimelineSelectMode

}

