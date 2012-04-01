package com.wajam.mry.execution


/**
 * Value (string, int, record) used in transaction operations
 */
trait Value extends Object with OperationSource {
  def value = this
  def serializableValue:Value = this

  def equalsValue(that: Value): Boolean = this == that
}

class NullValue() extends Value with Serializable {
  override def equalsValue(that: Value): Boolean = this.isInstanceOf[NullValue]
}

class MapValue(var mapValue: Map[String, Value]) extends Value with Serializable {

  def apply(key:String) = {
    mapValue.getOrElse(key, new NullValue)
  }
}

class StringValue(var strValue: String) extends Value with Serializable {
  override def toString = strValue

  override def equalsValue(that: Value): Boolean = {
    that match {
      case s:StringValue => s.strValue == strValue
      case _ => false
    }
  }
}



