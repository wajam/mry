package com.appaquet.mry.execution

/**
 * Timestamp used to represent mutation time on storage
 */
class Timestamp(var value:Long) {
}

object Timestamp {
  def apply(value:Long) = new Timestamp(value)
  
  def now = new Timestamp(System.currentTimeMillis())
  
  implicit def long2timestamp(value:Long) = Timestamp(value)
  implicit def timestamp2long(ts:Timestamp) = ts.value
}
