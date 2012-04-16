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

class ListValue(var listValue: List[Value]) extends Value with Serializable {

  def apply(index:Int) = {
    listValue(index)
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

class IntValue(var intValue: Long) extends Value with Serializable {
  override def toString = String.valueOf(intValue)

  override def equalsValue(that: Value): Boolean = {
    that match {
      case i:IntValue => i.intValue == i.intValue
      case _ => false
    }
  }
}


