package com.wajam.mry.execution

/**
 * Timestamp used to represent mutation time on storage
 */
class Timestamp(var value: Long) extends Serializable {
  override def toString: String = value.toString

  override def equals(obj: Any): Boolean = obj match {
    case t:Timestamp => t.value == value
    case _ => false
  }
}

object Timestamp {
  def apply(value: Long) = new Timestamp(value)

  def now = new Timestamp(System.currentTimeMillis())

  val MAX = new Timestamp(Long.MaxValue)

  val MIN = new Timestamp(0)

  implicit def long2timestamp(value: Long) = Timestamp(value)

  implicit def timestamp2long(ts: Timestamp) = ts.value
}
