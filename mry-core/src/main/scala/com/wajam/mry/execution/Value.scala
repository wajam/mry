package com.wajam.mry.execution


/**
 * Value (string, int, record) used in transaction operations
 */
trait Value extends Object with OperationSource {
  def value = this

  def serializableValue: Value = this

  def equalsValue(that: Value): Boolean = this == that

  def isNull = this.equalsValue(NullValue)
}

class NullValue extends Value with Serializable {
  override def equalsValue(that: Value): Boolean = this.isInstanceOf[NullValue]
}

object NullValue extends NullValue

class MapValue(var mapValue: Map[String, Value]) extends Value with Serializable {

  def apply(key: String) = {
    mapValue.getOrElse(key, new NullValue)
  }

  override def serializableValue: Value = {
    val newMap = for ((k, v) <- mapValue) yield (k -> v.serializableValue)
    new MapValue(newMap)
  }
}

class ListValue(var listValue: Seq[Value]) extends Value with Serializable {

  def apply(index: Int) = {
    listValue(index)
  }

  override def serializableValue: Value = {
    val newList = for (oldValue <- listValue) yield oldValue.serializableValue
    new ListValue(newList)
  }
}

class StringValue(var strValue: String) extends Value with Serializable {
  override def toString = strValue

  override def equalsValue(that: Value): Boolean = {
    that match {
      case s: StringValue => s.strValue == strValue
      case _ => false
    }
  }
}

class IntValue(var intValue: Long) extends Value with Serializable {
  override def toString = String.valueOf(intValue)

  override def equalsValue(that: Value): Boolean = {
    that match {
      case i: IntValue => i.intValue == this.intValue
      case _ => false
    }
  }
}

class BoolValue(var boolValue: Boolean) extends Value with Serializable {
  override def toString = String.valueOf(boolValue)

  override def equalsValue(that: Value): Boolean = {
    that match {
      case b: BoolValue => b.boolValue == this.boolValue
      case _ => false
    }
  }
}

class DoubleValue(var doubleValue: Double) extends Value with Serializable {
  override def toString = String.valueOf(doubleValue)

  override def equalsValue(that: Value): Boolean = {
    that match {
      case d: DoubleValue => d.doubleValue == this.doubleValue
      case _ => false
    }
  }
}

