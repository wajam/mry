package com.wajam.mry.storage.mysql

import com.wajam.mry.storage.StorageTransaction
import java.sql.ResultSet
import collection.mutable
import com.wajam.mry.execution._
import com.wajam.mry.api.ProtocolTranslator
import com.yammer.metrics.scala.Instrumented
import com.wajam.scn.Timestamp
import com.wajam.scn.storage.TimestampUtil
import com.wajam.nrv.tracing.Traced

/**
 * Mysql storage transaction
 */
class MysqlTransaction(private val storage: MysqlStorage, private val context: Option[ExecutionContext]) extends StorageTransaction with Instrumented with Traced {
  private val metricTimeline = tracedTimer("mysql-timeline")
  private val metricTopMostVersions = tracedTimer("mysql-topmostversions")
  private val metricSet = tracedTimer("mysql-set")
  private val metricGet = tracedTimer("mysql-get")
  private val metricDelete = tracedTimer("mysql-delete")
  private val metricCommit = tracedTimer("mysql-commit")
  private val metricRollback = tracedTimer("mysql-rollback")
  private val metricTruncateVersions = tracedTimer("mysql-truncateversions")
  private val metricSize = tracedTimer("mysql-count")

  var mutationsCount = 0

  val connection = storage.getConnection
  connection.setAutoCommit(false)

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
     *   INSERT INTO `table1_table1_1_table1_1_1` (`ts`,`tk`,k1,k2,k3,`d`)
     *   VALUES (1343138009222, 2517541033, 'k1', 'k1.2', 'k1.2.1', '...BLOB BYTES...')
     *   ON DUPLICATE KEY UPDATE d=VALUES(d)', with params
     */
    val sql = "INSERT INTO `%s` (`ts`,`tk`,%s,`d`) VALUES (?,?,%s,?) ON DUPLICATE KEY UPDATE d=VALUES(d)".format(fullTableName, sqlKeys, sqlValues)

    var results: SqlResults = null
    try {
      if (optRecord.isDefined) {
        // there is a value, we set it
        val value = optRecord.get.serializeValue(storage.valueSerializer)
        this.metricSet.time {
          results = storage.executeSql(connection, true, sql, (Seq(timestamp.value) ++ Seq(token) ++ keysValue ++ Seq(value)): _*)
        }

      } else {
        // no value, it's a delete
        this.metricDelete.time {
          results = storage.executeSql(connection, true, sql, (Seq(timestamp.value) ++ Seq(token) ++ keysValue ++ Seq(null)): _*)

          // delete all rows from children tables
          for (childTable <- table.allHierarchyTables) {
            val childFullTableName = childTable.depthName("_")

            val childSelectKeys = (for (i <- 1 to childTable.depth) yield "k%d".format(i)).mkString(",")
            val parentWhereKeys = (for (i <- 1 to keysValue.length) yield "k%d = ?".format(i)).mkString(" AND ")

            /* Generated SQL looks like:
            *
            *     INSERT INTO `table1_table1_1`
            *       SELECT 1343138009222 AS ts, tk, k1,k2, NULL AS d
            *       FROM `table1_table1_1`
            *       WHERE tk = 2517541033 AND ts <= 1343138009222 AND k1 = 'k1'
            *       GROUP BY tk, k1, k2
            *     ON DUPLICATE KEY UPDATE d=VALUES(d)
            */
            val childSql = """
                 INSERT INTO `%1$s`
                  SELECT ? AS ts, tk, %2$s, NULL AS d
                  FROM `%1$s`
                  WHERE tk = ? AND ts <= ? AND %3$s
                  GROUP BY tk, %2$s
                 ON DUPLICATE KEY UPDATE d=VALUES(d)
                           """.format(childFullTableName, childSelectKeys, parentWhereKeys)

            results = storage.executeSql(connection, true, childSql, (Seq(timestamp.value) ++ Seq(token) ++ Seq(timestamp.value) ++ keysValue): _*)
          }
        }
      }

      this.mutationsCount += 1

    } finally {
      if (results != null)
        results.close()
    }
  }

  def getMultiple(table: Table, token: Long, timestamp: Timestamp, accessPath: AccessPath, includeDeleted: Boolean = false): RecordIterator = {
    assert(accessPath.length >= 1)

    val keysValue = accessPath.keys
    val fullTableName = table.depthName("_")

    val projKeys = (for (i <- 1 to table.depth) yield "o.k%1$d".format(i)).mkString(",")
    val outerWhereKeys = (for (i <- 1 to accessPath.parts.length) yield "o.k%d = ?".format(i)).mkString(" AND ")
    val innerWhereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = o.k%1$d".format(i)).mkString(" AND ")

    /* Generated SQL looks like:
     *
     *   SELECT o.ts, o.tk, d, o.k1, o.k2, o.k3
     *   FROM `table1_table1_1_table1_1_1` AS o
     *   WHERE o.`tk` = 2517541033
     *   AND o.k1 = 'k1' AND o.k2 = 'k2'
     *   AND o.`ts` = (  SELECT MAX(ts)
     *                   FROM `table1_table1_1_table1_1_1` AS i
     *                   WHERE i.`ts` <= ? AND i.`tk` = 2517541033 AND i.k1 = o.k1 AND i.k2 = o.k2 AND i.k3 = o.k3)
     *   AND o.d IS NOT NULL
     */
    var sql = """
        SELECT o.ts, o.tk, d, %1$s
        FROM `%2$s` AS o
        WHERE o.`tk` = ?
        AND %3$s
        AND o.`ts` = (  SELECT MAX(ts)
                        FROM `%2$s` AS i
                        WHERE i.`ts` <= ? AND i.`tk` = ? AND %4$s)
              """.format(projKeys, fullTableName, outerWhereKeys, innerWhereKeys)

    if (!includeDeleted)
      sql += " AND o.d IS NOT NULL "

    var results: SqlResults = null
    try {
      this.metricGet.time {
        results = storage.executeSql(connection, false, sql, (Seq(token) ++ keysValue ++ Seq(timestamp.value) ++ Seq(token)): _*)
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

  def getTimeline(table: Table, fromTimestamp: Timestamp, count: Int): Seq[MutationRecord] = {
    val projKeys = (for (i <- 1 to table.depth) yield "c.k%1$d".format(i)).mkString(",")
    val whereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = c.k%1$d".format(i)).mkString(" AND ")
    val joinKeys = (for (i <- 1 to table.depth) yield "c.k%1$d = l.k%1$d".format(i)).mkString(" AND ")
    val fullTableName = table.depthName("_")

    /* Generated SQL looks like:
     *
     *   SELECT c.tk, c.ts, c.d, l.ts, l.d, c.k1, c.k2, c.k3, c.g3
     *   FROM `table1_table1_1_table1_1_1` AS c
     *   LEFT JOIN `table1_table1_1_table1_1_1` l
     *      ON (c.tk = l.tk AND c.k1 = l.k1 AND c.k2 = l.k2
     *          AND l.ts = ( SELECT MAX(i.ts)
     *                       FROM `table1_table1_1_table1_1_1` AS i
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
    val sql = """
                SELECT c.tk, c.ts, c.d, l.ts, l.d, %1$s
                FROM `%2$s` AS c
                LEFT JOIN `%2$s` l ON (c.tk = l.tk AND %3$s AND l.ts = (SELECT MAX(i.ts) FROM `%2$s` AS i WHERE i.tk = c.tk AND %4$s AND i.ts < c.ts))
                WHERE c.ts >= %5$d
                ORDER BY c.ts ASC
                LIMIT 0, %6$d;
              """.format(projKeys, fullTableName, joinKeys, whereKeys, fromTimestamp.value, count)

    var ret = mutable.LinkedList[MutationRecord]()
    var results: SqlResults = null
    try {
      this.metricTimeline.time {
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
     *    SELECT t.tk, COUNT(*) AS nb, t.k1,t.k2,t.k3
     *    FROM `table1_table1_1_table1_1_1` AS t
     *    WHERE t.tk >= 0
     *    GROUP BY t.k1,t.k2,t.k3
     *    HAVING COUNT(*) > 3
     *    LIMIT 0, 100
     */
    val sql =
      """
         SELECT t.tk, COUNT(*) AS nb, %1$s
         FROM `%2$s` AS t
         WHERE t.tk >= %3$d
         GROUP BY %1$s
         HAVING COUNT(*) > %4$d
         LIMIT 0, %5$d
      """.format(projKeys, fullTableName, fromToken, table.maxVersions, count)


    var ret = mutable.LinkedList[VersionRecord]()
    var results: SqlResults = null
    try {
      this.metricTopMostVersions.time {
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

  def truncateVersions(table: Table, token: Long, accessPath: AccessPath, count: Int, maxTimestamp: Timestamp = TimestampUtil.MAX) {
    val fullTableName = table.depthName("_")
    val whereKeys = (for (i <- 1 to table.depth) yield "k%1$d = ?".format(i)).mkString(" AND ")
    val keysValue = accessPath.keys

    /*
     * Generated SQL looks like:
     *
     *     DELETE FROM `table2_table2_1_table2_1_1`
     *     WHERE tk = ?
     *     AND k1 = ? AND k2 = ? AND k3 = ?
     *     AND ts < 9223372036854775807
     *     ORDER BY ts ASC
     *     LIMIT 2;
     */
    val sql = """
                DELETE FROM `%1$s`
                WHERE tk = ?
                AND %2$s
                AND ts < %3$d
                ORDER BY ts ASC
                LIMIT %4$d;
              """.format(fullTableName, whereKeys, maxTimestamp.value, count)

    var results: SqlResults = null
    try {
      this.metricTruncateVersions.time {
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
      this.metricSize.time {
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

  def getAllLatest(table: Table, fromTimestamp: Timestamp, currentTimestamp: Timestamp, count: Long): RecordIterator = {

    val fullTableName = table.depthName("_")

    val projKeys = (for (i <- 1 to table.depth) yield "o.k%1$d".format(i)).mkString(",")
    val innerWhereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = o.k%1$d".format(i)).mkString(" AND ")

    /* Generated SQL looks like:
     *
     *   SELECT o.ts, o.tk, d, o.k1, o.k2, o.k3
     *   FROM `table1_table1_1_table1_1_1` AS o
     *   WHERE o.`ts` = (  SELECT MAX(ts)
     *                     FROM `table1_table1_1_table1_1_1` AS i
     *                     WHERE i.`ts` <= ? AND i.`ts` > 0 AND i.`tk` = o.`tk`
     *                     AND i.k1 = o.k1 AND i.k2 = o.k2 AND i.k3 = o.k3)
     *   AND o.d IS NOT NULL
     *   ORDER BY o.`ts` ASC
     *   LIMIT 0, 100;
     */
    val sql = """
        SELECT o.ts, o.tk, d, %1$s
        FROM `%2$s` AS o
        WHERE o.`ts` = (  SELECT MAX(ts)
                          FROM `%2$s` AS i
                          WHERE i.`ts` <= ? AND i.`ts` > ? AND i.`tk` = o.`tk`
                          AND %3$s)
        AND o.d IS NOT NULL
        ORDER BY o.`ts` ASC
        LIMIT 0, ?
              """.format(projKeys, fullTableName, innerWhereKeys)

    var results: SqlResults = null
    try {
      this.metricGet.time {
        results = storage.executeSql(connection, false, sql, currentTimestamp.value, fromTimestamp.value, count)
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

class AccessPath(var parts: Seq[AccessKey] = Seq()) {
  def last = parts.last

  def length = parts.length

  def keys = parts.map(p => p.key)

  def apply(pos: Int) = this.parts(pos)

  override def toString: String = (for (part <- parts) yield part.key).mkString("/")
}

class Record(var value: Value = new MapValue(Map())) {
  var accessPath = new AccessPath()
  var token: Long = 0
  var timestamp: Timestamp = Timestamp(0)

  def load(serializer: ProtocolTranslator, resultset: ResultSet, depth: Int) {
    this.timestamp = Timestamp(resultset.getLong(1))
    this.token = resultset.getLong(2)
    this.unserializeValue(serializer, resultset.getBytes(3))

    this.accessPath = new AccessPath(for (i <- 1 to depth) yield new AccessKey(resultset.getString(3 + i)))
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
  var newValue: Option[Value] = None
  var oldTimestamp: Option[Timestamp] = None
  var oldValue: Option[Value] = None

  def load(serializer: ProtocolTranslator, resultset: ResultSet, depth: Int) {
    this.token = resultset.getLong(1)
    this.newTimestamp = Timestamp(resultset.getLong(2))
    this.newValue = this.unserializeValue(serializer, resultset.getBytes(3))

    val oldTs = resultset.getObject(4)
    if (oldTs != null) {
      this.oldTimestamp = Some(Timestamp(oldTs.asInstanceOf[Long]))
      this.oldValue = this.unserializeValue(serializer, resultset.getBytes(5))
    }

    this.accessPath = new AccessPath(for (i <- 1 to depth) yield new AccessKey(resultset.getString(5 + i)))
  }

  def unserializeValue(serializer: ProtocolTranslator, bytes: Array[Byte]): Option[Value] = {
    if (bytes != null)
      Some(serializer.decodeValue(bytes).asInstanceOf[MapValue])
    else
      None
  }
}

class RecordIterator(storage: MysqlStorage, results: SqlResults, table: Table) {
  private var hasNext = false

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

  def close() = results.close()
}

class VersionRecord {
  var token: Long = 0
  var versionsCount: Int = 0
  var accessPath = new AccessPath()

  def load(resultset: ResultSet, depth: Int) {
    this.token = resultset.getLong(1)
    this.versionsCount = resultset.getInt(2)
    this.accessPath = new AccessPath(for (i <- 1 to depth) yield new AccessKey(resultset.getString(2 + i)))
  }
}

