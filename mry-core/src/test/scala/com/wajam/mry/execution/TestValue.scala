package com.wajam.mry.execution

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class TestValue extends FunSuite {

  test("null value should be always match null, whatever instances it is") {
    assert((new NullValue).isNull)
    assert(NullValue().isNull)
    assert(NullValue.NULL_VALUE.isNull)
  }

  test("list value should return a list of serializable values when serializing it") {
    class NonSerializable(var intVal: Long) extends IntValue(intVal) {
      override def serializableValue: Value = new IntValue(intVal)
    }

    val list = new ListValue(Seq(new NonSerializable(1), new NonSerializable(2)))

    assert(list.listValue(0).isInstanceOf[NonSerializable])
    assert(!list.serializableValue.asInstanceOf[ListValue].listValue(0).isInstanceOf[NonSerializable])
  }
}
