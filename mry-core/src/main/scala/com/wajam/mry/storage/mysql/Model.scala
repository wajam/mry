package com.wajam.mry.storage.mysql

import scala.annotation.tailrec

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

class Table(val name: String, parent: Option[Table] = None, val maxVersions: Int = 3) extends TableCollection {
  override def currentTable = Some(this)

  def depthName(glue: String): String = {
    this.parentTable match {
      case None => this.name
      case Some(t) => t.depthName(glue) + glue + this.name
    }
  }

  lazy val uniqueName = depthName("_")

  def path: List[Table] = {
    this.parentTable match {
      case None => List(this)
      case Some(t) => t.path :+  this
    }
  }

  @tailrec
  final def getTopLevelTable: Table = {
    this.parentTable match {
      case Some(tableParent) => tableParent.getTopLevelTable
      case None => this
    }
  }

  override def hashCode(): Int = uniqueName.hashCode

  override def equals(that: Any) = that match {
    case other: Table => this.uniqueName.equalsIgnoreCase(other.uniqueName)
    case _ => false
  }

  override def toString: String = uniqueName
}



