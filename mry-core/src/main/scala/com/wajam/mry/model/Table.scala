package com.wajam.mry.model

/**
 * Table of a storage
 */
class Table(var name: String, var parent: Option[Table] = None) extends TableCollection {
  var indexes = List[Index]()

  override def currentTable = Some(this)

  def depthName(glue: String): String = {
    this.parentTable match {
      case None => this.name
      case Some(t) => t.depthName(glue) + glue + this.name
    }
  }
}
