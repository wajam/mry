package com.wajam.mry.execution

import scala.language.implicitConversions

/**
 * Implicits that can be used to help converting values from scala types to
 * mry types
 */
object Implicits {
  implicit def toVal(value: Value) = value

  implicit def value2object(value: Value): Any = {
    value match {
      case rVal: StringValue =>
        rVal.strValue

      case rVal: IntValue =>
        rVal.intValue

      case rVal: BoolValue =>
        rVal.boolValue

      case rVal: DoubleValue =>
        rVal.doubleValue

      case rVal: MapValue =>
        for ((k, v) <- rVal.mapValue) yield (k, value2object(v))

      case rVal: ListValue =>
        for (v <- rVal.listValue) yield value2object(v)

      case _ =>
        null
    }
  }

  implicit def string2value(value: String) = new StringValue(value)

  implicit def value2string(value: StringValue) = value.strValue

  implicit def int2value(value: Int) = new IntValue(value)

  implicit def value2int(value: IntValue) = value.intValue

  implicit def long2value(value: Long) = new IntValue(value)

  implicit def value2long(value: IntValue) = value.intValue

  implicit def bool2value(value: Boolean) = new BoolValue(value)

  implicit def value2bool(value: BoolValue) = value.boolValue

  implicit def double2value(value: Double) = new DoubleValue(value)

  implicit def value2double(value: DoubleValue) = value.doubleValue

  implicit def map2value(map: Map[String, Any]) = new MapValue(map.map {
    case (k, v) => (k -> object2value(v))
  })

  implicit def value2map(value: MapValue) = value.mapValue

  implicit def list2value(list: Seq[Any]) = new ListValue(for (e <- list) yield object2value(e))

  implicit def value2list(value: ListValue) = value.listValue

  def object2value(obj: Any): Value = {
    obj match {
      case v: String => v
      case v: Int => v
      case v: Long => v
      case v: Boolean => v
      case v: Double => v
      case v: Map[_, _] => v.asInstanceOf[Map[String, Any]]
      case v: Seq[_] => v
      case v: Value => v
    }
  }

}
