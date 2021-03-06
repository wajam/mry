package com.wajam.mry.execution


/**
 * Value (string, int, record) used in transaction operations
 */
trait Value extends Object with OperationSource {
  def value = this

  def serializableValue: Value = this

  def equalsValue(that: Value): Boolean = this == that

  override def equalsContent(obj: Any): Boolean = {
    this.equalsValue(obj.asInstanceOf[Value])
  }

  def isNull = false
}

/**
 * Default (Not supported) predicate
 *
 * It allow Mry proxying (see OperationSource (getProxiedSource.execPredicate)) to work.
 * Without it concrete class would throw a runtime exception.
 *
 * It avoid duplication and it allow smooth transition from no support to full predicate support, in the concrete class.
 *
 * TODO: Implement predicate on all values.
 */
trait DefaultPredicate extends Value with OperationSource {

  override def execPredicate(context: ExecutionContext, into: Variable, key: Object, filter: MryFilters.MryFilter, value: Object) = {

    into.value = BoolValue(false)
  }
}

@SerialVersionUID(-8696609946517999638L)
class NullValue extends Value with DefaultPredicate with Serializable {
  override def equalsValue(that: Value): Boolean = this.isInstanceOf[NullValue]
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

  override def execFiltering(context: ExecutionContext, into: Variable, key: Object, filter: MryFilters.MryFilter, value: Object): Unit = {

    // Foward filtering to children
    into.value = MapValue(mapValue.map { case (k, v) =>
          v.execFiltering(context, into, key, filter, value)
          (k -> into.value)
      })
  }

  override def execPredicate(context: ExecutionContext, into: Variable, key: Object, filter: MryFilters.MryFilter, value: Object): Unit = {
    val result = mapValue.get(key.toString) match {
      case Some(v) =>
        MryFilters(filter).execute(v, value)
      case None => true
    }

    into.value = BoolValue(result)
  }

  override def execProjection(context: ExecutionContext, into: Variable, keys: Object*): Unit = {
    into.value = if (!keys.isEmpty) {
        MapValue(mapValue.filter(e => keys.contains(StringValue(e._1))))
      } else {
        MapValue(mapValue)
      }
  }

  // partial update support
  override def execSet(context: ExecutionContext, into: Variable, data: Object*): Unit = {
    val fieldName = param[StringValue](data, 0).strValue
    val fieldValue = param[Value](data, 1)
    into.value = MapValue(mapValue + (fieldName -> fieldValue))
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

  override def execProjection(context: ExecutionContext, into: Variable, keys: Object*) {
    into.value =
      if (!keys.isEmpty) {
        ListValue(listValue.map(v => {
          v.execProjection(context, into, keys: _*)
          into.value
        }))
      } else {
        ListValue(listValue)
      }
  }

  override def execFiltering(context: ExecutionContext, into: Variable, key: Object, filter: MryFilters.MryFilter, value: Object) {

    val temp = listValue

      // Filter the child mapValue that doesn't match the filter
      .filter { (v) =>
        v.execPredicate(context, into, key, filter, value)
        val BoolValue(filtered) = into.value
        filtered
      }

    into.value = ListValue(temp)
  }
}

@SerialVersionUID(-3026000576636973393L)
case class StringValue(strValue: String) extends Value with DefaultPredicate  {
  override def toString = strValue

  override def equalsValue(that: Value): Boolean = {
    that match {
      case StringValue(s) => s == strValue
      case _ => false
    }
  }
}

@SerialVersionUID(6885681030783170441L)
case class IntValue(intValue: Long) extends Value with DefaultPredicate {
  override def toString = String.valueOf(intValue)

  override def equalsValue(that: Value): Boolean = {
    that match {
      case IntValue(i) => i == this.intValue
      case _ => false
    }
  }
}

@SerialVersionUID(3071120965134758093L)
case class BoolValue(boolValue: Boolean) extends Value with DefaultPredicate  {
  override def toString = String.valueOf(boolValue)

  override def equalsValue(that: Value): Boolean = {
    that match {
      case BoolValue(b) => b == this.boolValue
      case _ => false
    }
  }
}

@SerialVersionUID(-4318700849067154549L)
case class DoubleValue(doubleValue: Double) extends Value with DefaultPredicate  {
  override def toString = String.valueOf(doubleValue)

  override def equalsValue(that: Value): Boolean = {
    that match {
      case DoubleValue(d) => d == this.doubleValue
      case _ => false
    }
  }
}

