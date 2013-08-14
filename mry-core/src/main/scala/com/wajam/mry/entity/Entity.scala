package com.wajam.mry.entity

import scala.collection.mutable.{HashMap => MutableHashMap}
import com.wajam.mry.execution._
import com.wajam.mry.execution.Implicits._

trait MryConvertible {

  def toMry: Value

  def fromMry(value: Value): this.type
}

trait FieldLike extends MryConvertible {

  def validate(): Option[String] = {
    None
  }

  def name: String
}

case class Field[T](private val collection: FieldCollection, name: String)(implicit m: FieldTypeHandler[T]) extends FieldLike {

  private var value: Option[T] = None
  private val default: Option[T] = m.getDefault(this)

  collection.registerField(this)

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
      case None => throw new RuntimeException(s"Field $name is not initialized and doesn't have a default value")
    }
  }

  def set(v: T) {
    value = Some(v)
  }

  def isInitialized = value.isDefined

  override def validate() = {
    (value, default) match {
      case (Some(_), _) => None // everything is good
      case (None, Some(_)) => None // everything is good
      case (None, None) => Some(s"Field $name is not initialized")
    }
  }

  override def toString = s"$name=$get"
}

case class ListField[T](private val collection: FieldCollection, name: String)(implicit m: FieldTypeHandler[T])
  extends FieldLike {

  collection.registerField(this)

  private var _list = Seq[T]()

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

  override def toString() = list.mkString(s"$name[", ",", "]")
}


trait FieldCollection
  extends MryConvertible {

  private val fields = MutableHashMap[String, FieldLike]()

  def registerField(field: FieldLike) {
    fields += field.name -> field
  }

  override def toString = fields.values.mkString("{", ",", "}")

  def validate(): Option[String] = {
    this.fields.values.map(_.validate()).collect({
      case Some(error) => error
    }).headOption
  }

  def toMry: Value = {

    this.validate() match {
      case Some(error) => throw new RuntimeException(error)
      case None => fields.flatMap {
        case (name, field) =>
          field.toMry match {
            case _: NullValue => None
            case o: Value => Some(name -> o)
          }
      }.toMap
    }
  }

  def fromMry(value: Value): this.type = {
    value match {
      case mv: MapValue =>
        mv.mapValue.foreach {
          case (fieldName, fieldValue) =>
            fields.get(fieldName) match {
              case Some(f: FieldLike) => f.fromMry(fieldValue)
              case _ => //println("Field %s doesn't exist!".format(fieldName)) // Side-effecting LIKE A BOSS
            }
        }

      case _ => throw new scala.IllegalArgumentException("Expected a MapValue, got %s".format(value))
    }
    this.validate()
    this
  }
}

case class OptionalFieldsGroup(collection: FieldCollection, name: String)
  extends FieldLike
  with FieldCollection {

  collection.registerField(this)

  def isDefined = defined
  private var defined = false

  def define() {
    if (defined) throw new RuntimeException("Already defined")
    else defined = true
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
      case MapValue(_) =>
        super.fromMry(value)
        this.define()
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

  override def validate() = {
    if (isDefined) super.validate()
    else None
  }

  override def toString: String = s"$name=${if (isDefined) super.toString else "undefined"}"
}


abstract class Entity extends FieldCollection {
  def modelName: String

  def key: String

  override def toString: String = modelName + super.toString

  override def hashCode(): Int = key.hashCode()

  override def equals(that: Any) = that match {
    case other: Entity => this.key == other.key
    case _ => false
  }
}

