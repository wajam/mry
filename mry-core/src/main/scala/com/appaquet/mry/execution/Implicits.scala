package com.appaquet.mry.execution

/**
 * Implicits that can be used to help transfering values from scala types to
 * mry types
 */
object Implicits {
  implicit def string2value(value:String) = new ValueString(value)
}
