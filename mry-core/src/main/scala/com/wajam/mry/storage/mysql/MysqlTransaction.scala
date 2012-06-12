package com.wajam.mry.storage.mysql

import com.wajam.mry.storage.StorageTransaction
import java.sql.ResultSet
import scala.collection.mutable
import com.wajam.mry.execution._
import com.wajam.mry.api.ProtocolTranslator
import com.yammer.metrics.scala.Instrumented

/**
 * Mysql storage transaction
 */
class MysqlTransaction(storage: MysqlStorage) extends StorageTransaction with Instrumented {
  private val metricTimeline = metrics.timer("mysql-timeline")
  private val metricSet = metrics.timer("mysql-set")
  private val metricGet = metrics.timer("mysql-get")
  private val metricDelete = metrics.timer("mysql-delete")
  private val metricCommit = metrics.timer("mysql-commit")
  private val metricRollback = metrics.timer("mysql-rollback")

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
  }

  def set(table: Table, token: Long, timestamp: Timestamp, keys: Seq[String], optRecord: Option[Record]) {
    assert(keys.length == table.depth)

    val fullTableName = table.depthName("_")
    val sqlKeys = (for (i <- 1 to table.depth) yield "k%d".format(i)).mkString(",")
    val sqlValues = (for (i <- 1 to table.depth) yield "?").mkString(",")

    val sql = "INSERT INTO `%s` (`ts`,`tk`,%s,`d`) VALUES (?,?,%s,?) ON DUPLICATE KEY UPDATE d=VALUES(d)".format(fullTableName, sqlKeys, sqlValues)

    var results: storage.SqlResults = null
    try {
      if (optRecord.isDefined) {
        // there is a value, we set it
        val value = optRecord.get.serializeValue(storage.valueSerializer)
        this.metricSet.time {
          results = storage.executeSql(connection, true, sql, (Seq(timestamp.value) ++ Seq(token) ++ keys ++ Seq(value)): _*)
        }

      } else {
        // no value, it's a delete
        this.metricDelete.time {
          results = storage.executeSql(connection, true, sql, (Seq(timestamp.value) ++ Seq(token) ++ keys ++ Seq(null)): _*)
        }
      }

    } finally {
      if (results != null)
        results.close()
    }
  }

  def get(table: Table, token: Long, timestamp: Timestamp, keys: Seq[String]): Option[Record] = {
    assert(keys.length == table.depth)

    val fullTableName = table.depthName("_")
    val projKeys = (for (i <- 1 to table.depth) yield "o.k%d".format(i)).mkString(",")
    val outerWhereKeys = (for (i <- 1 to table.depth) yield "o.k%d = ?".format(i)).mkString(" AND ")
    val innerWhereKeys = (for (i <- 1 to table.depth) yield "i.k%d = ?".format(i)).mkString(" AND ")

    val sql = """
        SELECT o.ts, o.tk, %1$s, d
        FROM `%2$s` AS o
        WHERE o.`tk` = ?
        AND %3$s
        AND o.`ts` = (  SELECT MAX(ts)
                        FROM `%2$s` AS i
                        WHERE i.`ts` <= ? AND i.`tk` = ? AND %4$s)
        AND o.d IS NOT NULL;
              """.format(projKeys, fullTableName, outerWhereKeys, innerWhereKeys)


    var results: storage.SqlResults = null
    try {
      this.metricGet.time {
        results = storage.executeSql(connection, false, sql, (Seq(token) ++ keys ++ Seq(timestamp.value) ++ Seq(token) ++ keys): _*)
      }

      if (results.resultset.next()) {
        val record = new Record
        record.load(storage.valueSerializer, results.resultset, table.depth)
        return Some(record)
      }

    } finally {
      if (results != null)
        results.close()
    }

    None
  }

  def getTimeline(table: Table, fromTimestamp: Timestamp, count: Int): Seq[MutationRecord] = {
    val projKeys = (for (i <- 1 to table.depth) yield "c.k%d".format(i)).mkString(",")
    val whereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = c.k%1$d".format(i)).mkString(" AND ")
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
    var results: storage.SqlResults = null
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
}

class Record(var value: MapValue = new MapValue(Map())) {
  var key: String = ""
  var token: Long = 0
  var timestamp: Timestamp = Timestamp(0)

  def load(serializer: ProtocolTranslator, resultset: ResultSet, depth: Int) {
    this.timestamp = Timestamp(resultset.getLong(1))
    this.token = resultset.getLong(2)

    // TODO: set key
    for (i <- 0 to depth) {
    }

    this.unserializeValue(serializer, resultset.getBytes(depth + 3))
  }

  def serializeValue(serializer: ProtocolTranslator): Array[Byte] = {
    serializer.encodeValue(value)
  }

  def unserializeValue(serializer: ProtocolTranslator, bytes: Array[Byte]) {
    this.value = serializer.decodeValue(bytes).asInstanceOf[MapValue]
  }
}

class MutationRecord {
  var token: Long = 0
  var keys: Seq[String] = Seq()
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

    this.keys = for (i <- 1 to depth) yield resultset.getString(5 + i)
  }

  def unserializeValue(serializer: ProtocolTranslator, bytes: Array[Byte]): Option[Value] = {
    if (bytes != null)
      Some(serializer.decodeValue(bytes).asInstanceOf[MapValue])
    else
      None
  }
}


