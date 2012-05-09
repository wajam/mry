package com.wajam.mry.storage

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.wajam.nrv.Logging
import java.sql.{PreparedStatement, ResultSet, SQLException, Connection}
import com.wajam.mry.execution._
import com.wajam.mry.api.protobuf.ProtobufTranslator

/**
 * MySQL backed storage
 */
class MysqlStorage(name: String, host: String, database: String, username: String, password: String) extends Storage(name) with Logging {
  val datasource = new ComboPooledDataSource()
  var model = new MysqlModel

  val valueSerializer = new ProtobufTranslator

  datasource.setDriverClass("com.mysql.jdbc.Driver")
  datasource.setJdbcUrl(String.format("jdbc:mysql://%s/%s?zeroDateTimeBehavior=convertToNull", host, database))
  datasource.setUser(username)
  datasource.setPassword(password)

  def getStorageTransaction(context: ExecutionContext) = new MysqlTransaction(context)

  def getStorageValue(context: ExecutionContext): Value = new StorageValue

  def syncModel(model: MysqlModel) {
    this.model = model

    val mysqlTables = this.getTables
    var modelTables = Map[String, Boolean]()

    def sync(table: MysqlTable) {
      val fullName = table.depthName("_")
      modelTables += (fullName -> true)

      if (!mysqlTables.contains(fullName)) {
        createTable(table, fullName)
      }

      for ((name, table) <- table.tables) {
        sync(table)
      }
    }

    for ((name, table) <- model.tables) {
      sync(table)
    }

    for (table <- mysqlTables) {
      if (!modelTables.contains(table))
        this.dropTable(table)
    }
  }

  def nuke() {
    val tables = this.getTables

    for (table <- tables) {
      this.dropTable(table)
    }
  }

  def close() {
    this.datasource.close()
  }

  private def getConnection = datasource.getConnection

  private class SqlResults {
    var statement: PreparedStatement = null
    var resultset: ResultSet = null

    def close() {
      if (statement != null)
        statement.close()

      if (resultset != null)
        resultset.close()
    }
  }

  @throws(classOf[SQLException])
  private def executeSql(connection: Connection, update: Boolean, sql: String, params: Any*): SqlResults = {
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

  private def createTable(table: MysqlTable, fullTableName: String) {
    var sql = "CREATE TABLE `%s` ( ".format(fullTableName) +
      "`ts` bigint(20) NOT NULL AUTO_INCREMENT, " +
      "`tk` bigint(20) NOT NULL, "

    var keyList = ""
    for (i <- 1 to table.depth) {
      sql += " k%d varchar(128) NOT NULL, ".format(i)

      if (keyList != "")
        keyList += ","

      keyList += "k%d".format(i)
    }
    sql += " `d` blob NOT NULL, " +
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

  class MysqlTransaction(context: ExecutionContext) extends StorageTransaction {
    val connection = getConnection
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

    def set(table: MysqlTable, token: Long, keys: Seq[String], record: Record) {
      assert(keys.length == table.depth)

      val fullTableName = table.depthName("_")
      val sqlKeys = (for (i <- 1 to table.depth) yield "k%d".format(i)).mkString(",")
      val sqlValues = (for (i <- 1 to table.depth) yield "?").mkString(",")

      val sql = "INSERT INTO `%s` (`ts`,`tk`,%s,`d`) VALUES (?,?,%s,?) ON DUPLICATE KEY UPDATE d=VALUES(d)".format(fullTableName, sqlKeys, sqlValues)

      var results: SqlResults = null
      try {
        results = executeSql(connection, true, sql, (Seq(this.context.timestamp.value) ++ Seq(token) ++ keys ++ Seq(record.serializeValue)): _*)
      } finally {
        if (results != null)
          results.close()
      }
    }

    def get(table: MysqlTable, token: Long, keys: Seq[String]): Option[Record] = {
      assert(keys.length == table.depth)

      val fullTableName = table.depthName("_")
      val projKeys = (for (i <- 1 to table.depth) yield "k%d".format(i)).mkString(",")
      val whereKeys = (for (i <- 1 to table.depth) yield "k%d = ?".format(i)).mkString(" AND ")

      val sql = "SELECT ts, tk, %s, d FROM `%s` WHERE `ts` <= ? AND `tk` = ? AND %s ORDER BY `ts` DESC LIMIT 0,1".format(projKeys, fullTableName, whereKeys)

      var results: SqlResults = null
      try {
        results = executeSql(connection, false, sql, (Seq(this.context.timestamp.value) ++ Seq(token) ++ keys): _*)
        if (results.resultset.next()) {
          return Some(readRecord(results.resultset, table.depth))
        }

      } finally {
        if (results != null)
          results.close()
      }

      None
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
        valueSerializer.encodeValue(value)
      }

      def unserializeValue(bytes: Array[Byte]) {
        this.value = valueSerializer.decodeValue(bytes).asInstanceOf[MapValue]
      }
    }

  }

  class StorageValue extends Value {
    override def execFrom(context: ExecutionContext, into: Variable, keys: Object*) {
      var tableName = param[StringValue](keys, 0).strValue
      var optTable = model.getTable(tableName)

      optTable match {
        case Some(t) =>
          into.value = new TableValue(t)

        case None =>
          throw new StorageException("Non existing table %s".format(tableName))

      }
    }
  }

  class TableValue(table: MysqlTable, prefixKeys: Seq[String] = Seq()) extends Value {
    override def execGet(context: ExecutionContext, into: Variable, keys: Object*) {
      // TODO: get with no keys = multiple row

      val key = param[StringValue](keys, 0).strValue
      val keysSeq = prefixKeys ++ Seq(key)

      val token = context.getToken(keysSeq(0))
      context.useToken(token)

      if (!context.dryMode) {
        into.value = new RecordValue(context, table, token, keysSeq)
      }
    }

    override def execSet(context: ExecutionContext, into: Variable, value: Object, keys: Object*) {
      val key = param[StringValue](keys, 0).strValue
      val mapVal = param[MapValue](value)
      val keysSeq = prefixKeys ++ Seq(key)

      val token = context.getToken(keysSeq(0))
      context.useToken(token)
      context.isMutation = true

      if (!context.dryMode) {
        val transaction = context.getStorageTransaction(MysqlStorage.this).asInstanceOf[MysqlTransaction]
        transaction.set(table, token, keysSeq, new transaction.Record(mapVal))
      }
    }

    class RecordValue(context: ExecutionContext, table: MysqlTable, token: Long, prefixKeys: Seq[String]) extends Value {
      val transaction = context.getStorageTransaction(MysqlStorage.this).asInstanceOf[MysqlTransaction]

      override def serializableValue = this.innerValue

      override def proxiedSource: Option[OperationSource] = Some(this.innerValue)

      lazy val record = transaction.get(table, token, prefixKeys)

      lazy val innerValue = {
        this.record match {
          case Some(r) =>
            r.value
          case None =>
            new NullValue
        }
      }

      override def execFrom(context: ExecutionContext, into: Variable, keys: Object*) {
        var tableName = param[StringValue](keys, 0).strValue
        var optTable = table.getTable(tableName)

        optTable match {
          case Some(t) =>
            into.value = new TableValue(t, prefixKeys)

          case None =>
            throw new StorageException("Non existing table %s".format(tableName))

        }
      }
    }

  }

}
