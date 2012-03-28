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

    val connection = this.getConnection
    var results: SqlResults = null
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

  def getTransaction(time: Timestamp) = new Transaction(time)

  def syncModel(model: Model) {
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


  class Transaction(time: Timestamp) extends StorageTransaction {
    val connection = getConnection
    connection.setAutoCommit(false)

    def set(table: Table, keys: Seq[String], record: Record) {
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

    private def readRecord(resultset:ResultSet, depth:Int):Record = {
      val record = new Record
      record.timestamp = Timestamp(resultset.getLong(1))

      // TODO: set key
      for (i <- 0 to depth) {
      }
      
      record.value = resultset.getBytes(depth+2)

      record
    }

    def get(table: Table, keys: Seq[String]): Option[Record] = {
      assert(keys.length == table.depth)

      /*
      sqlKeys := ""
      projKeys := ""
      for i := 1; i <= len(keys); i++ {
        if sqlKeys != "" {
          sqlKeys += " AND "
        }
        projKeys += fmt.Sprintf("k%d,", i)
        sqlKeys += fmt.Sprintf("k%d = ?", i)
      }

      stmt, err := t.client.Prepare("SELECT t," + projKeys + " d FROM `" + t.client.Escape(t.storage.toTableString(table)) + "` WHERE " + sqlKeys + " AND `t` <= ? ORDER BY `t` DESC LIMIT 0,1")
      */

      val fullName = table.depthName("_")
      var projKeys = ""
      var whereKeys = ""

      for (i <- 1 to table.depth) {
        projKeys += "k%d,".format(i)
        whereKeys += " AND k%d = ?".format(i)
      }

      val sql = "SELECT t, %s d FROM `%s` WHERE `t` <= ? %s ORDER BY `t` DESC LIMIT 0,1".format(projKeys, fullName, whereKeys)
      
      var results:SqlResults = null
      try {
        results = executeSql(connection, false, sql, (Seq(time.value) ++ keys):_*)
        if (results.resultset.next()) {
          return Some(readRecord(results.resultset, table.depth))
        }
        
      } finally {
        if (results != null)
          results.close()
      }

      None
    }

    def query(query: Query): Result[Record] = {
      null
    }

    def timeline(table: Table, from: Timestamp): Result[RecordMutation] = {
      null
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
  }

}
