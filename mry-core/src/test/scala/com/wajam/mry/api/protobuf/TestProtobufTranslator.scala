package com.wajam.mry.api.protobuf

import org.scalatest.FunSuite
import com.wajam.mry.execution.Implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.mry.execution.{ListValue, Value, MapValue, NullValue}

@RunWith(classOf[JUnitRunner])
class TestProtobufTranslator extends FunSuite {
  val translator = new ProtobufTranslator

  test("value encode/decode") {
    var bytes = translator.encodeValue("testvalue")
    var value = translator.decodeValue(bytes)
    assert(value.equalsValue("testvalue"))

    bytes = translator.encodeValue(NullValue)
    value = translator.decodeValue(bytes)
    assert(value.equalsValue(NullValue))

    bytes = translator.encodeValue(3423)
    value = translator.decodeValue(bytes)
    assert(value.equalsValue(3423))

    bytes = translator.encodeValue(Map[String, Value]("key1" -> "value1", "key2" -> "value2"))
    value = translator.decodeValue(bytes)
    value match {
      case mv: MapValue =>
        assert(mv.size == 2)
        assert(mv.get("key1").get.equalsValue("value1"))
        assert(mv.get("key2").get.equalsValue("value2"))
      case _ =>
        fail("Didn't unserialize to map")
    }

    bytes = translator.encodeValue(Map[String, Value]())
    value = translator.decodeValue(bytes)
    value match {
      case mv: MapValue =>
        assert(mv.size == 0)
      case _ =>
        fail("Didn't unserialize to map")
    }

    bytes = translator.encodeValue(Seq[Value]("value1", "value2", "value3"))
    value = translator.decodeValue(bytes)
    value match {
      case lv: ListValue =>
        assert(lv.size == 3)
        assert(lv(0).equalsValue("value1"))
        assert(lv(1).equalsValue("value2"))
        assert(lv(2).equalsValue("value3"))
      case _ =>
        fail("Didn't unserialize to list")
    }

    bytes = translator.encodeValue(List[Value]())
    value = translator.decodeValue(bytes)
    value match {
      case lv: ListValue =>
        assert(lv.size == 0)
      case _ =>
        fail("Didn't unserialize to list")
    }
  }


  ignore("transaction encode/decode") {

  }
}
