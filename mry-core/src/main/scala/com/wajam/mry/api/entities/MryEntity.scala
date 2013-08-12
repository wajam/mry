package com.wajam.mry.api.entities

import scala.collection.mutable
import com.wajam.mry.execution._
import com.wajam.mry.execution.Implicits._
import com.wajam.mry.execution.MapValue
import org.joda.time.DateTime

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

  implicit object DateTimeMryConverter extends FieldTypeHandler[DateTime] {
    def toMry(v: DateTime): Value = IntValue(v.getMillis)

    def fromMry(value: Value): DateTime = value match {
      case IntValue(v) => new DateTime(v)
      case _ => throw new IllegalArgumentException
    }
  }

  implicit object OptionalDateTimeMryConverter extends OptionalFieldTypeHandler[DateTime]

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

trait FieldTypeHandler[T] {
  def init(v: Field[T]) {
  }

  def toMry(v: T): Value

  def fromMry(value: Value): T
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


trait MryConvertible {
  def toMry: Value

  def fromMry(value: Value): this.type
}

trait FieldLike extends MryConvertible {
  def init() {
  }

  val name: String
}


object Field {
  def apply[T](collection: FieldCollection, name: String)(implicit m: FieldTypeHandler[T]) = new Field(collection, name)(m)

  def unapply[T](s: Field[T]) = Some(s.name, s.get)
}


class Field[T](collection: FieldCollection, val name: String)(implicit m: FieldTypeHandler[T]) extends FieldLike {
  protected var value: Option[T] = None

  protected var default: Option[T] = None

  collection.registerField(this)
  m.init(this)

  def toMry: Value = {
    m.toMry(get)
  }

  def fromMry(value: Value): this.type = {
    this.set(m.fromMry(value))
    this
  }


  def get = value match {
    case Some(v) => v
    case None => default match {
      case Some(d) => d
      case None => throw new RuntimeException("Field %s is not initialized and doesn't have a default value".format(name))
    }
  }

  def set(v: T) {
    value = Some(v)
  }

  def withDefault(default: T) = {
    this.default = Some(default)
    this
  }

  def isInitialized = value.isDefined

  override def init() {
    (value, default) match {
      case (Some(_), _) => // everything is good
      case (None, Some(_)) => // everything is good
      case (None, None) => throw new RuntimeException("Field %s is not initialized".format(name))
    }
  }

  override def toString = name + "=" + get
}


object ListField {

  def apply[T](collection: FieldCollection, name: String)(implicit m: FieldTypeHandler[T]) =
    new ListField[T](collection, name)(m)
}


class ListField[T](collection: FieldCollection, val name: String)(implicit m: FieldTypeHandler[T])
  extends FieldLike {

  collection.registerField(this)

  protected var _list = Seq[T]()

  def list = _list

  def set(iter: Iterable[T]) {
    _list = iter.toSeq
  }

  def clear() {
    _list = Seq()
  }

  def apply(pos: Int) = _list(pos)

  def fromMry(value: Value): this.type = {
    value match {
      case s: ListValue => this.set(s.listValue.map(m.fromMry))
      case _ => throw new ClassCastException("Cannot load field %s with value %s".format(name, value))
    }
    this
  }

  def toMry = list.map(m.toMry)

  override def toString() = list.mkString(name + "[", ",", "]")
}


trait FieldCollection
  extends MryConvertible {

  protected val fields = mutable.HashMap[String, FieldLike]()
  protected var initialized = false

  def registerField(field: FieldLike) {
    this.fields += field.name -> field
  }

  override def toString = fields.values.mkString("{", ",", "}")

  def init() {
    if (!initialized) {
      this.fields.values.foreach(_.init())
      initialized = true
    }
  }

  def toMry: Value = {
    this.init()
    fields.flatMap {
      case (name, field) =>
        field.toMry match {
          case n: NullValue => None
          case o: Value => Some(name -> o)
        }
    }.toMap
  }

  def fromMry(value: Value): this.type = {
    value match {
      case mv: MapValue =>
        mv.mapValue.foreach {
          case (fieldName, fieldValue) =>
            fields.get(fieldName) match {
              case Some(f) => f.fromMry(fieldValue)
              case None => println("Field %s doesn't exist!".format(fieldName))
            }
        }

      case _ => throw new scala.IllegalArgumentException("Expected a MapValue, got %s".format(value))
    }

    this.init()
    this
  }
}


class OptionalFieldsGroup(collection: FieldCollection, val name: String)
  extends FieldLike
  with FieldCollection {

  collection.registerField(this)

  var isDefined: Boolean = false

  def define(f: => Unit) {
    f
    isDefined = true
  }

  def reset() {
    isDefined = false
  }

  def map[A](f: this.type => A): Option[A] = {
    if (isDefined)
      Some(f(this))
    else
      None
  }

  def flatMap[A](f: this.type => Option[A]): Option[A] = {
    if (isDefined)
      f(this)
    else
      None
  }

  override def fromMry(value: Value) = {
    value match {
      case mv: MapValue =>
        super.fromMry(value)
        this.isDefined = true
      case _ => // it's not loaded
    }
    this
  }

  override def toMry: Value = {
    if (isDefined)
      super.toMry
    else
      NullValue
  }

  override def init() {
    if (isDefined)
      super.init()
  }

  override def toString: String = name + "=" + (if (isDefined) super.toString else "undefined")
}


abstract class MryEntity extends FieldCollection {
  val modelName: String

  def key: String

  override def toString: String = modelName + super.toString

  override def hashCode(): Int = key.hashCode()

  override def equals(that: Any) = that match {
    case other: MryEntity => this.key == other.key
    case _ => false
  }
}


