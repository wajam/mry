package com.wajam.mry.storage.mysql

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.wajam.nrv.Logging
import java.sql.{PreparedStatement, ResultSet, SQLException, Connection}
import com.wajam.mry.execution._
import com.wajam.mry.api.protobuf.ProtobufTranslator
import com.wajam.mry.storage._
import com.yammer.metrics.scala.Instrumented

/**
 * MySQL backed storage
 */
class MysqlStorage(name: String, host: String, database: String, username: String, password: String) extends Storage(name) with Logging with Value with Instrumented {
  val datasource = new ComboPooledDataSource()
  var model = new Model

  val valueSerializer = new ProtobufTranslator

  datasource.setDriverClass("com.mysql.jdbc.Driver")
  datasource.setJdbcUrl(String.format("jdbc:mysql://%s/%s?zeroDateTimeBehavior=convertToNull", host, database))
  datasource.setUser(username)
  datasource.setPassword(password)

  def getStorageTransaction(context: ExecutionContext) = this.getStorageTransaction

  def getStorageTransaction = new MysqlTransaction(this)

  def getStorageValue(context: ExecutionContext): Value = this

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

  def getConnection = datasource.getConnection

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
  def executeSql(connection: Connection, update: Boolean, sql: String, params: Any*): SqlResults = {
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
      "`ts` bigint(20) NOT NULL AUTO_INCREMENT, " +
      "`tk` bigint(20) NOT NULL, "

    var keyList = ""
    for (i <- 1 to table.depth) {
      sql += " k%d varchar(128) NOT NULL, ".format(i)

      if (keyList != "")
        keyList += ","

      keyList += "k%d".format(i)
    }
    sql += " `d` blob NULL, " +
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

}
