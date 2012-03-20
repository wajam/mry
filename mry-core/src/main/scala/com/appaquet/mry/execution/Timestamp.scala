package com.appaquet.mry.execution

/**
 * Description
 */

class Timestamp(var value:Long) {
}

object Timestamp {
  def apply(value:Long) = new Timestamp(value)
}
