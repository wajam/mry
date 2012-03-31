package com.wajam.mry.storage.mysql

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.wajam.nrv.Logging
import java.sql.{PreparedStatement, ResultSet, SQLException, Connection}
import com.wajam.mry.execution._
import com.wajam.mry.storage.{StorageException, StorageTransaction, Storage}

/**
 * MySQL backed storage
 */
class MysqlStorage(name: String, host: String, database: String, username: String, password: String) extends Storage(name) with Logging {
  val datasource = new ComboPooledDataSource()
  var model = new Model

  datasource.setDriverClass("com.mysql.jdbc.Driver")
  datasource.setJdbcUrl(String.format("jdbc:mysql://%s/%s?zeroDateTimeBehavior=convertToNull", host, database))
  datasource.setUser(username)
  datasource.setPassword(password)

  def getStorageTransaction(time: Timestamp) = new MysqlTransaction(time)

  def getStorageValue(transaction: StorageTransaction, context: ExecutionContext): Value = transaction.asInstanceOf[MysqlTransaction]

  def syncModel(model: Model) {
    this.model = model

    val mysqlTables = this.getTables
    var modelTables = Map[String, Boolean]()

    def sync(table: Table) {
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

  class SqlResults {
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

  private def createTable(table: Table, fullTableName: String) {
    var sql = "CREATE TABLE `%s` ( ".format(fullTableName) +
      "`t` bigint(20) NOT NULL AUTO_INCREMENT, "

    var keyList = ""
    for (i <- 1 to table.depth) {
      sql += " k%d varchar(128) NOT NULL, ".format(i)

      if (keyList != "")
        keyList += ","

      keyList += "k%d".format(i)
    }
    sql += " `d` blob NOT NULL, " +
      " PRIMARY KEY (`t`," + keyList + "), " +
      "	UNIQUE KEY `revkey` (" + keyList + ",`t`) " +
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

  class MysqlTransaction(time: Timestamp) extends StorageTransaction with Value {
    val connection = getConnection
    connection.setAutoCommit(false)

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

    class TableValue(table: Table, prefixKeys: Seq[String] = Seq()) extends Value {
      override def execGet(context: ExecutionContext, into: Variable, keys: Object*) {
        val key = param[StringValue](keys, 0).strValue
        val keysSeq = prefixKeys ++ Seq(key)
        val record = MysqlTransaction.this.get(table, keysSeq)

        if (record != None) {
          into.value = new RecordValue(table, record.get, keysSeq)
        }
      }

      override def execSet(context: ExecutionContext, into: Variable, value: Object, keys: Object*) {
        val key = param[StringValue](keys, 0).strValue
        val strVal = param[StringValue](value).strValue

        MysqlTransaction.this.set(table, prefixKeys ++ Seq(key), strVal)
      }
    }

    class RecordValue(table: Table, record: Record, prefixKeys: Seq[String]) extends StringValue(record.stringValue) {
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


    private def set(table: Table, keys: Seq[String], record: Record) {
      assert(keys.length == table.depth)

      val fullName = table.depthName("_")
      var sqlKeys = ""
      var sqlUpdateKeys = ""
      var sqlValues = ""

      for (i <- 1 to table.depth) {
        if (i > 1) {
          sqlKeys += ","
          sqlValues += ","
        }

        val k = "k%d".format(i)
        sqlKeys += k
        sqlValues += "?"
        sqlUpdateKeys += "k%d = ?".format(i)
      }

      val sql = "INSERT INTO `%s` (`t`,%s,`d`) VALUES (?,%s,?) ON DUPLICATE KEY UPDATE d=VALUES(d)".format(fullName, sqlKeys, sqlValues)


      var results: SqlResults = null
      try {
        results = executeSql(connection, true, sql, (Seq(time.value) ++ keys ++ Seq(record.value)): _*)
      } finally {
        if (results != null)
          results.close()
      }
    }

    private def get(table: Table, keys: Seq[String]): Option[Record] = {
      assert(keys.length == table.depth)

      val fullName = table.depthName("_")
      var projKeys = ""
      var whereKeys = ""

      for (i <- 1 to table.depth) {
        projKeys += "k%d,".format(i)
        whereKeys += " AND k%d = ?".format(i)
      }

      val sql = "SELECT t, %s d FROM `%s` WHERE `t` <= ? %s ORDER BY `t` DESC LIMIT 0,1".format(projKeys, fullName, whereKeys)

      var results: SqlResults = null
      try {
        results = executeSql(connection, false, sql, (Seq(time.value) ++ keys): _*)
        if (results.resultset.next()) {
          return Some(readRecord(results.resultset, table.depth))
        }

      } finally {
        if (results != null)
          results.close()
      }

      None
    }

    private def readRecord(resultset: ResultSet, depth: Int): Record = {
      val record = new Record
      record.timestamp = Timestamp(resultset.getLong(1))

      // TODO: set key
      for (i <- 0 to depth) {
      }

      record.value = resultset.getBytes(depth + 2)

      record
    }

  }

}
