package com.wajam.mry.execution

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class TestValue extends FunSuite {

  test("null value should be always match null, whatever instances it is") {
    assert((NullValue).isNull)
    assert(NullValue.isNull)
  }

  test("list value should return a list of serializable values when serializing it") {
    val list = ListValue(Seq(new NonSerializableInt(1), new NonSerializableInt(2)))
    assert(list.listValue(0).isInstanceOf[NonSerializableInt])
    assert(!list.serializableValue.asInstanceOf[ListValue].listValue(0).isInstanceOf[NonSerializableInt])
  }

  test("map value should return a map of serializable values when serializing it") {
    val map = MapValue(Map("test" -> new NonSerializableInt(1)))
    assert(map.mapValue("test").isInstanceOf[NonSerializableInt])
    assert(!map.serializableValue.asInstanceOf[MapValue].mapValue("test").isInstanceOf[NonSerializableInt])
  }


  class NonSerializableInt(var intVal: Long) extends IntValue(intVal) {
    override def serializableValue: Value = IntValue(intVal)
  }

}
