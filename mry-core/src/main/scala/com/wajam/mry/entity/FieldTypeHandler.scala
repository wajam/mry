package com.wajam.mry.entity

import com.wajam.mry.execution._

trait FieldTypeHandler[T] {
  def init(v: Field[T]) {
  }

  def toMry(v: T): Value

  def fromMry(value: Value): T
}
object FieldTypeHandler {

  implicit object StringMryConverter extends FieldTypeHandler[String] {
    def toMry(v: String): Value = StringValue(v)

    def fromMry(value: Value): String = value match {
      case StringValue(v) => v
      case _ => throw new IllegalArgumentException
    }
  }

  implicit object OptionalStringMryConverter extends OptionalFieldTypeHandler[String]


  implicit object IntMryConverter extends FieldTypeHandler[Int] {
    def toMry(v: Int): Value = IntValue(v)

    def fromMry(value: Value): Int = value match {
      case IntValue(v) => v.toInt
      case _ => throw new IllegalArgumentException
    }
  }

  implicit object OptionalIntMryConverter extends OptionalFieldTypeHandler[Int]


  implicit object LongMryConverter extends FieldTypeHandler[Long] {
    def toMry(v: Long): Value = IntValue(v)

    def fromMry(value: Value): Long = value match {
      case IntValue(v) => v
      case _ => throw new IllegalArgumentException
    }
  }

  implicit object OptionalLongMryConverter extends OptionalFieldTypeHandler[Long]

  implicit object BooleanMryConverter extends FieldTypeHandler[Boolean] {
    def toMry(v: Boolean): Value = BoolValue(v)

    def fromMry(value: Value): Boolean = value match {
      case BoolValue(v) => v
      case _ => throw new IllegalArgumentException
    }
  }

  implicit object OptionalBooleanMryConverter extends OptionalFieldTypeHandler[Boolean]

  implicit object DoubleMryConverter extends FieldTypeHandler[Double] {
    def toMry(v: Double): Value = DoubleValue(v)

    def fromMry(value: Value): Double = value match {
      case DoubleValue(v) => v
      case _ => throw new IllegalArgumentException
    }
  }

  implicit object OptionalDoubleMryConverter extends OptionalFieldTypeHandler[Double]

}



class OptionalFieldTypeHandler[T](implicit h: FieldTypeHandler[T]) extends FieldTypeHandler[Option[T]] {
  override def init(v: Field[Option[T]]) {
    v.withDefault(None)
  }

  def toMry(v: Option[T]): Value = v match {
    case Some(s) => h.toMry(s)
    case None => NullValue
  }

  def fromMry(value: Value): Option[T] = value match {
    case NullValue => None
    case _ => Some(h.fromMry(value))
  }
}

