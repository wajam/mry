package com.wajam.mry.model

/**
 * Collection of tables
 */
trait TableCollection {
  var tables = Map[String, Table]()

  def currentTable: Option[Table] = None

  var parentTable: Option[Table] = None

  def addTable(table: Table): Table = {
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

  def getTable(name: String): Option[Table] = this.tables.get(name)
}
