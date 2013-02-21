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

@SerialVersionUID(3469418720141064443L)
class MapValue(val mapValue: Map[String, Value]) extends Value with Serializable {

  def apply(key: String) = {
    mapValue.getOrElse(key, new NullValue)
  }

  override def serializableValue: Value = {
    val newMap = for ((k, v) <- mapValue) yield (k -> v.serializableValue)
    new MapValue(newMap)
  }
}

object MapValue {
  def unapply(mv: MapValue) = Some(mv.mapValue)
}

@SerialVersionUID(3729883700870722479L)
class ListValue(val listValue: Seq[Value]) extends Value with Serializable {

  def apply(index: Int) = {
    listValue(index)
  }

  override def serializableValue: Value = {
    val newList = for (oldValue <- listValue) yield oldValue.serializableValue
    new ListValue(newList)
  }
}

object ListValue {
  def unapply(lv: ListValue) = Some(lv.listValue)
}

@SerialVersionUID(-3026000576636973393L)
class StringValue(val strValue: String) extends Value with Serializable {

  override def toString = strValue

  override def equalsValue(that: Value): Boolean = {
    that match {
      case s: StringValue => s.strValue == strValue
      case _ => false
    }
  }
}

object StringValue {
  def unapply(s: StringValue) = Some(s.strValue)
}

@SerialVersionUID(6885681030783170441L)
class IntValue(val intValue: Long) extends Value with Serializable {
  override def toString = String.valueOf(intValue)

  override def equalsValue(that: Value): Boolean = {
    that match {
      case i: IntValue => i.intValue == this.intValue
      case _ => false
    }
  }
}

object IntValue {
  def unapply(i: IntValue) = Some(i.intValue)
}

@SerialVersionUID(3071120965134758093L)
class BoolValue(val boolValue: Boolean) extends Value with Serializable {
  override def toString = String.valueOf(boolValue)

  override def equalsValue(that: Value): Boolean = {
    that match {
      case b: BoolValue => b.boolValue == this.boolValue
      case _ => false
    }
  }
}

object BoolValue {
  def unapply(b: BoolValue) = Some(b.boolValue)
}

@SerialVersionUID(-4318700849067154549L)
class DoubleValue(val doubleValue: Double) extends Value with Serializable {
  override def toString = String.valueOf(doubleValue)

  override def equalsValue(that: Value): Boolean = {
    that match {
      case d: DoubleValue => d.doubleValue == this.doubleValue
      case _ => false
    }
  }
}

object DoubleValue {
  def unapply(d: DoubleValue) = Some(d.doubleValue)
}
