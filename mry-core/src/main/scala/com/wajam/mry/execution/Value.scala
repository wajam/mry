package com.wajam.mry.execution


/**
 * Value (string, int, record) used in transaction operations
 */
trait Value extends Object with OperationSource {
  def value = this

  def serializableValue: Value = this

  def equalsValue(that: Value): Boolean = this == that

  def isNull = false
}

class NullValue extends Value with Serializable {
  override def isNull = true
}

object NullValue extends NullValue

@SerialVersionUID(3469418720141064443L)
case class MapValue(mapValue: Map[String, Value]) extends Value {

  def apply(key: String) = {
    mapValue.getOrElse(key, NullValue)
  }

  override def serializableValue: Value = {
    val newMap = for ((k, v) <- mapValue) yield (k -> v.serializableValue)
    new MapValue(newMap)
  }
}

@SerialVersionUID(3729883700870722479L)
case class ListValue(listValue: Seq[Value]) extends Value {

  def apply(index: Int) = {
    listValue(index)
  }

  override def serializableValue: Value = {
    val newList = for (oldValue <- listValue) yield oldValue.serializableValue
    new ListValue(newList)
  }
}

@SerialVersionUID(-3026000576636973393L)
case class StringValue(strValue: String) extends Value {
  override def toString = strValue
}

@SerialVersionUID(6885681030783170441L)
case class IntValue(intValue: Long) extends Value {
  override def toString = String.valueOf(intValue)
}

@SerialVersionUID(3071120965134758093L)
case class BoolValue(boolValue: Boolean) extends Value {
  override def toString = String.valueOf(boolValue)
}

@SerialVersionUID(-4318700849067154549L)
case class DoubleValue(doubleValue: Double) extends Value {
  override def toString = String.valueOf(doubleValue)
}

