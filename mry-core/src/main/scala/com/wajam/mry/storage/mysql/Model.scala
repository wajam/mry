package com.wajam.mry.storage.mysql

class Model extends TableCollection {
}

trait TableCollection {
  var tables = Map[String, Table]()

  def allHierarchyTables:Seq[Table] = {
    var allTables = Seq[Table]()
    for ((name, table) <- this.tables) {
      allTables ++= Seq(table) ++ table.allHierarchyTables
    }
    allTables
  }

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

class Table(var name: String, var parent: Option[Table] = None, var maxVersions:Int = 3) extends TableCollection {
  override def currentTable = Some(this)

  def depthName(glue: String): String = {
    this.parentTable match {
      case None => this.name
      case Some(t) => t.depthName(glue) + glue + this.name
    }
  }
}



