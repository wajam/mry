package com.appaquet.mry.model

/**
 * Table of a storage
 */
class Table(var name:String, var parent:Option[Table] = None) extends TableCollection {
  var indexes = List[Index]()
  override def currentTable = Some(this)
}
