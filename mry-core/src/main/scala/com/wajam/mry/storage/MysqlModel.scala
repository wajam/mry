package com.wajam.mry.storage

class MysqlModel extends MysqlTableCollection {
}

trait MysqlTableCollection {
  var tables = Map[String, MysqlTable]()

  def currentTable: Option[MysqlTable] = None

  var parentTable: Option[MysqlTable] = None

  def addTable(table: MysqlTable): MysqlTable = {
    table.parentTable = this.currentTable
    this.tables += (table.name -> table)
    table
  }

  def depth: Int = {
    this.parentTable match {
      case None => 1
      case Some(p) => p.depth + 1
    }
  }

  def getTable(name: String): Option[MysqlTable] = this.tables.get(name)
}

class MysqlTable(var name: String, var parent: Option[MysqlTable] = None) extends MysqlTableCollection {
  override def currentTable = Some(this)

  def depthName(glue: String): String = {
    this.parentTable match {
      case None => this.name
      case Some(t) => t.depthName(glue) + glue + this.name
    }
  }
}
