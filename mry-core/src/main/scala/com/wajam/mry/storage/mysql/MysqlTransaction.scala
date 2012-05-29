package com.wajam.mry.storage.mysql

import com.wajam.mry.storage.StorageTransaction
import java.sql.ResultSet
import scala.collection.mutable
import com.wajam.mry.execution._

/**
 * Mysql storage transaction
 */
class MysqlTransaction(storage: MysqlStorage, context: ExecutionContext) extends StorageTransaction {
  val connection = storage.getConnection
  connection.setAutoCommit(false)

  def rollback() {
    if (this.connection != null) {
      try {
        this.connection.rollback()
      } finally {
        this.connection.close()
      }
    }
  }

  def commit() {
    if (this.connection != null) {
      try {
        this.connection.commit()
      } finally {
        this.connection.close()
      }
    }
  }

  def readRecord(resultset: ResultSet, depth: Int): Record = {
    val record = new Record
    record.timestamp = Timestamp(resultset.getLong(1))
    record.token = resultset.getLong(2)

    // TODO: set key
    for (i <- 0 to depth) {
    }

    record.unserializeValue(resultset.getBytes(depth + 3))

    record
  }

  class Record(var value: MapValue = new MapValue(Map()), var key: String = "", var token: Long = 0, var timestamp: Timestamp = Timestamp(0)) {
    def serializeValue: Array[Byte] = {
      storage.valueSerializer.encodeValue(value)
    }

    def unserializeValue(bytes: Array[Byte]) {
      this.value = storage.valueSerializer.decodeValue(bytes).asInstanceOf[MapValue]
    }
  }

  def set(table: Table, token: Long, keys: Seq[String], optRecord: Option[Record]) {
    assert(keys.length == table.depth)

    val fullTableName = table.depthName("_")
    val sqlKeys = (for (i <- 1 to table.depth) yield "k%d".format(i)).mkString(",")
    val sqlValues = (for (i <- 1 to table.depth) yield "?").mkString(",")

    val sql = "INSERT INTO `%s` (`ts`,`tk`,%s,`d`) VALUES (?,?,%s,?) ON DUPLICATE KEY UPDATE d=VALUES(d)".format(fullTableName, sqlKeys, sqlValues)

    var results: storage.SqlResults = null
    try {
      // if none, it's a delete
      val value = optRecord match {
        case Some(r) => r.serializeValue
        case None => null
      }

      results = storage.executeSql(connection, true, sql, (Seq(this.context.timestamp.value) ++ Seq(token) ++ keys ++ Seq(value)): _*)
    } finally {
      if (results != null)
        results.close()
    }
  }

  def get(table: Table, token: Long, keys: Seq[String]): Option[Record] = {
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
      results = storage.executeSql(connection, false, sql, (Seq(token) ++ keys ++ Seq(this.context.timestamp.value) ++ Seq(token) ++ keys): _*)
      if (results.resultset.next()) {
        return Some(readRecord(results.resultset, table.depth))
      }

    } finally {
      if (results != null)
        results.close()
    }

    None
  }

  class MutationRecord {
    var token: Long = 0
    var keys: Seq[String] = Seq()
    var newTimestamp: Timestamp = Timestamp(0)
    var newValue: Value = new MapValue(Map())
    var oldTimestamp: Option[Timestamp] = None
    var oldValue: Option[Value] = None

    def load(resultset: ResultSet, depth: Int) {
      this.token = resultset.getLong(1)
      this.newTimestamp = Timestamp(resultset.getLong(2))
      this.newValue = this.unserializeValue(resultset.getBytes(3))

      val oldTs = resultset.getObject(4)
      if (oldTs != null) {
        this.oldTimestamp = Some(Timestamp(oldTs.asInstanceOf[Long]))
        this.oldValue = Some(this.unserializeValue(resultset.getBytes(5)))
      }

      this.keys = for (i <- 1 to depth) yield resultset.getString(5 + i)
    }

    def unserializeValue(bytes: Array[Byte]): Value = {
      if (bytes != null)
        storage.valueSerializer.decodeValue(bytes).asInstanceOf[MapValue]
      else
        NullValue()
    }
  }

  def getTimeline(table: Table, fromTimestamp: Timestamp, count: Int): Seq[MutationRecord] = {
    val projKeys = (for (i <- 1 to table.depth) yield "c.k%d".format(i)).mkString(",")
    val whereKeys = (for (i <- 1 to table.depth) yield "i.k%1$d = c.k%1$d".format(i)).mkString(" AND ")
    val fullTableName = table.depthName("_")

    val sql = """
        SELECT c.tk, c.ts, c.d, l.ts, l.d, %1$s
        FROM `%2$s` AS c LEFT JOIN `%2$s` l ON (c.tk = l.tk AND c.k1 = l.k1 AND l.ts < c.ts)
        WHERE (l.ts IS NULL OR l.ts = (SELECT MAX(i.ts) FROM `%2$s` AS i WHERE i.tk = c.tk AND %3$s AND i.ts < c.ts))
        AND c.ts >= %4$d
        ORDER BY c.ts ASC
        LIMIT 0,%5$d
              """.format(projKeys, fullTableName, whereKeys, fromTimestamp.value, count)


    var ret = mutable.LinkedList[MutationRecord]()
    var results: storage.SqlResults = null
    try {
      results = storage.executeSql(connection, false, sql)

      while (results.resultset.next()) {
        val mut = new MutationRecord()
        mut.load(results.resultset, table.depth)
        ret :+= mut
      }

    } finally {
      if (results != null)
        results.close()
    }

    ret
  }

}

