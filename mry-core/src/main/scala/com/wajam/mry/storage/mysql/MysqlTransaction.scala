package com.wajam.mry.storage.mysql

import com.wajam.mry.storage.StorageTransaction
import java.sql.ResultSet
import collection.mutable
import com.wajam.mry.execution._
import com.wajam.mry.api.ProtocolTranslator
import com.yammer.metrics.scala.Instrumented

/**
 * Mysql storage transaction
 */
class MysqlTransaction(storage: MysqlStorage) extends StorageTransaction with Instrumented {
  private val metricTimeline = metrics.timer("mysql-timeline")
  private val metricTopMostVersions = metrics.timer("mysql-topmostversions")
  private val metricSet = metrics.timer("mysql-set")
  private val metricGet = metrics.timer("mysql-get")
  private val metricDelete = metrics.timer("mysql-delete")
  private val metricCommit = metrics.timer("mysql-commit")
  private val metricRollback = metrics.timer("mysql-rollback")
  private val metricTruncateVersions = metrics.timer("mysql-truncateversions")
  private val metricSize = metrics.timer("mysql-count")

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
    val gensValue = accessPath.generations.map(p => p.get)
    val fullTableName = table.depthName("_")

    val sqlKeys = (for (i <- 1 to keysValue.length) yield "k%d".format(i)).mkString(",")
    val sqlGens = (for (i <- 1 to gensValue.length) yield "g%d".format(i)).mkString(",")
    val sqlGensUpdate = (for (i <- 1 to gensValue.length) yield "g%1$d = VALUES(g%1$d)".format(i)).mkString(",")
    val sqlValues = (for (i <- 1 to table.depth * 2) yield "?").mkString(",")
    val sql = "INSERT INTO `%s` (`ts`,`tk`,%s,%s,`d`) VALUES (?,?,%s,?) ON DUPLICATE KEY UPDATE d=VALUES(d), %s".format(fullTableName, sqlKeys, sqlGens, sqlValues, sqlGensUpdate)

    var results: SqlResults = null
    try {
      if (optRecord.isDefined) {
        // there is a value, we set it
        val value = optRecord.get.serializeValue(storage.valueSerializer)
        this.metricSet.time {
          results = storage.executeSql(connection, true, sql, (Seq(timestamp.value) ++ Seq(token) ++ keysValue ++ gensValue ++ Seq(value)): _*)
        }

      } else {
        // no value, it's a delete
        this.metricDelete.time {
          results = storage.executeSql(connection, true, sql, (Seq(timestamp.value) ++ Seq(token) ++ keysValue ++ gensValue ++ Seq(null)): _*)
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
    val optGensValue = accessPath.generations
    val gensValue = for (optGen <- optGensValue if optGen.isDefined) yield optGen.get
    val fullTableName = table.depthName("_")

    val projKeys = (for (i <- 1 to table.depth) yield "o.k%1$d, o.g%1$d".format(i)).mkString(",")
    val outerWhereKeys = (for (i <- 1 to accessPath.parts.length) yield "o.k%d = ?".format(i)).mkString(" AND ")
    val innerWhereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = o.k%1$d".format(i)).mkString(" AND ")
    val innerWhereGens =
      if (gensValue.length > 1)
        " AND " + (for (i <- 1 to optGensValue.length - 1 if optGensValue(i - 1).isDefined) yield "o.g%d = ?".format(i)).mkString(" AND ")
      else
        ""

    val innerGensValue =
      if (gensValue.length > 1)
        gensValue
      else
        Seq()

    val outerWhereGens =
      if (gensValue.length > 0)
        " AND " + (for (i <- 1 to optGensValue.length if optGensValue(i - 1).isDefined) yield "o.g%d = ?".format(i)).mkString(" AND ")
      else
        ""


    var sql = """
        SELECT o.ts, o.tk, d, %1$s
        FROM `%2$s` AS o
        WHERE o.`tk` = ?
        AND %3$s
        %4$s
        AND o.`ts` = (  SELECT MAX(ts)
                        FROM `%2$s` AS i
                        WHERE i.`ts` <= ? AND i.`tk` = ? AND %5$s %6$s)
              """.format(projKeys, fullTableName, outerWhereKeys, outerWhereGens, innerWhereKeys, innerWhereGens)

    if (!includeDeleted)
      sql += " AND o.d IS NOT NULL "

    var results: SqlResults = null
    try {
      this.metricGet.time {
        results = storage.executeSql(connection, false, sql, (Seq(token) ++ keysValue ++ gensValue ++ Seq(timestamp.value) ++ Seq(token) ++ innerGensValue): _*)
      }

      new RecordIterator(storage, results, table)

    } catch {
      case e: Exception =>
        if (results != null)
          results.close()
        null
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
    val projKeys = (for (i <- 1 to table.depth) yield "c.k%1$d, c.g%1$d".format(i)).mkString(",")
    val whereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = c.k%1$d AND i.g%1$d = c.g%1$d".format(i)).mkString(" AND ")
    val fullTableName = table.depthName("_")

    val sql = """
                SELECT c.tk, c.ts, c.d, l.ts, l.d, %1$s
                FROM `%2$s` AS c
                LEFT JOIN `%2$s` l ON (c.tk = l.tk AND c.k1 = l.k1 AND l.ts = (SELECT MAX(i.ts) FROM `%2$s` AS i WHERE i.tk = c.tk AND %3$s AND i.ts < c.ts))
                WHERE c.ts >= %4$d
                ORDER BY c.ts ASC
                LIMIT 0, %5$d;
              """.format(projKeys, fullTableName, whereKeys, fromTimestamp.value, count)

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
          val gen = new VersionRecord
          gen.load(results.resultset, table.depth)
          ret :+= gen
        }
      }

    } finally {
      if (results != null)
        results.close()
    }

    ret
  }

  def truncateVersions(table: Table, token: Long, accessPath: AccessPath, count: Int, maxTimestamp: Timestamp = Timestamp.MAX) {
    val fullTableName = table.depthName("_")
    val whereKeys = (for (i <- 1 to table.depth) yield "k%1$d = ?".format(i)).mkString(" AND ")
    val keysValue = accessPath.keys

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
}

class AccessKey(var key: String, var generation: Option[Int] = None)

class AccessPath(var parts: Seq[AccessKey] = Seq()) {
  def last = parts.last

  def length = parts.length

  def keys = parts.map(p => p.key)

  def generations = parts.map(p => p.generation)

  def apply(pos: Int) = this.parts(pos)

  override def toString: String = (for (part <- parts) yield "%s(gen=%s)".format(part.key, part.generation)).mkString("/")
}

class Record(var value: Value = new MapValue(Map())) {
  var accessPath = new AccessPath()
  var token: Long = 0
  var timestamp: Timestamp = Timestamp(0)

  def load(serializer: ProtocolTranslator, resultset: ResultSet, depth: Int) {
    this.timestamp = Timestamp(resultset.getLong(1))
    this.token = resultset.getLong(2)
    this.unserializeValue(serializer, resultset.getBytes(3))

    this.accessPath = new AccessPath(for (i <- 1 to depth) yield new AccessKey(resultset.getString(3 + 2 * i - 1), Some(resultset.getInt(3 + 2 * i))))
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

    this.accessPath = new AccessPath(for (i <- 1 to depth) yield new AccessKey(resultset.getString(5 + 2 * i - 1), Some(resultset.getInt(5 + 2 * i))))
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
  var generations: Int = 0
  var accessPath = new AccessPath()

  def load(resultset: ResultSet, depth: Int) {
    this.token = resultset.getLong(1)
    this.generations = resultset.getInt(2)
    this.accessPath = new AccessPath(for (i <- 1 to depth) yield new AccessKey(resultset.getString(2 + i)))
  }
}

