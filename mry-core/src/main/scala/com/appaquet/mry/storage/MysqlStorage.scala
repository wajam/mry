package com.appaquet.mry.storage

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.appaquet.mry.model.{Table, Model}
import com.appaquet.mry.execution.Timestamp
import java.sql._
import com.appaquet.nrv.Logging

/**
 * MySQL backed storage
 */
class MysqlStorage(host: String, database: String, username: String, password: String) extends Storage with Logging {
  val datasource = new ComboPooledDataSource()

  datasource.setDriverClass("com.mysql.jdbc.Driver")
  datasource.setJdbcUrl(String.format("jdbc:mysql://%s/%s?zeroDateTimeBehavior=convertToNull", host, database))
  datasource.setUser(username)
  datasource.setPassword(password)

  private def getConnection = datasource.getConnection

  class SqlResults {
    var connection: Connection = null
    var statement: PreparedStatement = null
    var resultset: ResultSet = null

    def close() {
      if (connection != null)
        connection.close()

      if (statement != null)
        statement.close()

      if (resultset != null)
        resultset.close()
    }
  }


  @throws(classOf[SQLException])
  private def executeSql(sql: String, params: Object*): SqlResults = {
    val results = new SqlResults

    try {
      results.connection = getConnection
      results.statement = results.connection.prepareStatement(sql)

      var p = 0
      for (param <- params) {
        p += 1
        results.statement.setObject(p, param)
      }

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

    try {
      results = executeSql("SHOW TABLES")
      val rs = results.resultset

      var tables = List[String]()
      while (rs.next()) {
        tables ::= rs.getString(1)
      }

      tables
    } catch {
      case e:Exception => {
        error("Couldn't get tables from database", e)
        throw e
      }
    } finally {
      if (results != null)
        results.close()
    }
  }

  private def createTable(table: Table, fullTableName:String) {
    var sql = "CREATE TABLE ? ( " +
      "`t` bigint(20) NOT NULL AUTO_INCREMENT, "

    var keyList = ""
    for (i <- 1 to table.depth + 1) {
      sql += " k%d varchar(128) NOT NULL, ".format(i)

      if (keyList != "")
        keyList += ","

      keyList += "k%d".format(i)
    }
    sql += "	`d` blob NOT NULL, " +
      " PRIMARY KEY (`t`," + keyList + "), "
      "	UNIQUE KEY `revkey` (" + keyList + ",`t`) "
      ") ENGINE=InnoDB  DEFAULT CHARSET=utf8;"


    var results:SqlResults = null
    try {
      results = this.executeSql(sql, fullTableName)
    } finally {
      if (results != null)
        results.close()
    }
  }

  def getTransaction = new Transaction

  def syncModel(model: Model) {
    val currentTables = this.getTables

    def sync(table: Table) {
      val fullName = table.depthName("_")
      println(fullName)
      if (!currentTables.contains(fullName)) {
        createTable(table, fullName)
      }

      for ((name, table) <- table.tables) {
        sync(table)
      }
    }

    for ((name, table) <- model.tables) {
      sync(table)
    }
  }

  def nuke() = null


  class Transaction extends StorageTransaction {
    val connection = getConnection

    def set(table: Table, keys: Seq[String], record: Record) {
      table.depth
    }

    def get(table: Table, keys: Seq[String]): Option[Record] = {
      null
    }

    def query(query: Query): Result[Record] = {
      null
    }

    def timeline(table: Table, from: Timestamp): Result[RecordMutation] = {
      null
    }

    def rollback() {}

    def commit() {}
  }

}
