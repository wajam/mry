package com.wajam.mry.storage.mysql

class Model extends TableCollection {
}

trait TableCollection {
  var tables = Map[String, Table]()

  def allHierarchyTables: Iterable[Table] = {
    this.tables.values.flatMap(table => table.allHierarchyTables ++ Seq(table))
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

class Table(var name: String, var parent: Option[Table] = None, var maxVersions: Int = 3) extends TableCollection {
  override def currentTable = Some(this)

  def depthName(glue: String): String = {
    this.parentTable match {
      case None => this.name
      case Some(t) => t.depthName(glue) + glue + this.name
    }
  }

  lazy val uniqueName = depthName("_")

  override def hashCode(): Int = uniqueName.hashCode

  override def equals(that: Any) = that match {
    case other: Table => this.uniqueName.equalsIgnoreCase(other.uniqueName)
    case _ => false
  }

  override def toString: String = uniqueName
}



