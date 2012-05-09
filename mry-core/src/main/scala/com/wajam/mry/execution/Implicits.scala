package com.wajam.mry.execution

/**
 * Implicits that can be used to help transfering values from scala types to
 * mry types
 */
object Implicits {
  implicit def toVal(value: Value) = value

  implicit def value2object(value: Value): Any = {
    value match {
      case rVal:StringValue =>
        rVal.strValue

      case rVal:IntValue =>
        rVal.intValue

      case rVal:MapValue =>
        for ((k, v) <- rVal.mapValue) yield (k, value2object(v))

      case rVal:ListValue =>
        for (v <- rVal.listValue) yield value2object(v)

      case _ =>
        null
    }
  }

  implicit def string2value(value: String) = new StringValue(value)

  implicit def value2string(value: StringValue) = value.strValue

  implicit def int2value(value: Int) = new IntValue(value)

  implicit def value2int(value: IntValue) = value.intValue

  implicit def map2value(map: Map[String, Value]) = new MapValue(map)

  implicit def map2value(elems: (String, Value)*) = new MapValue(Map[String, Value](elems: _*))

  implicit def value2map(value: MapValue) = value.mapValue

  implicit def list2value(list: List[Value]) = new ListValue(list)

  implicit def list2value(list: Seq[Value]) = new ListValue(list)

  implicit def value2list(value: ListValue) = value.listValue
}
