package com.wajam.mry.execution


/**
 * Value (string, int, record) used in transaction operations
 */
trait Value extends Object with OperationSource {
  def value = this
  def serializableValue:Value = this

  def equalsValue(that: Value): Boolean = this == that

  def isNull = this.equalsValue(NullValue.NULL_VALUE)
}

class NullValue() extends Value with Serializable {
  override def equalsValue(that: Value): Boolean = this.isInstanceOf[NullValue]
}

object NullValue {
  val NULL_VALUE = new NullValue()
  def apply() = NULL_VALUE
}

class MapValue(var mapValue: Map[String, Value]) extends Value with Serializable {

  def apply(key:String) = {
    mapValue.getOrElse(key, new NullValue)
  }
}

class ListValue(var listValue: Seq[Value]) extends Value with Serializable {

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


