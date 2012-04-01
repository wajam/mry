package com.wajam.mry.execution

/**
 * Implicits that can be used to help transfering values from scala types to
 * mry types
 */
object Implicits {
  implicit def toVal(value: Value) = value

  implicit def string2value(value: String) = new StringValue(value)

  implicit def value2string(value: StringValue) = value.value

  implicit def map2value(map: Map[String, Value]) = new MapValue(map)

  implicit def map2value(elems: (String, Value)*) = new MapValue(Map[String, Value](elems: _*))

  implicit def value2map(value: MapValue) = value.mapValue
}
