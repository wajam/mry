package com.wajam.mry.api.protobuf

import org.scalatest.FunSuite
import com.wajam.mry.execution.Implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.mry.execution._
import com.wajam.mry.execution.MapValue
import com.wajam.mry.execution.ListValue
import org.scalatest.matchers.ShouldMatchers
import com.wajam.mry.api.Transport

@RunWith(classOf[JUnitRunner])
class TestProtobufTranslator extends FunSuite with ShouldMatchers {
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

  private def buildTransaction(): Transaction = {
    val t = new Transaction((b) => b.returns(b.from("B").get(1000).set("C").limit(100).projection("D").delete("E")))
    t
  }

  test("stuff") {

    val op: Operation = new Operation.From(null, new Variable(null, 0), Seq())

    assert(op.isInstanceOf[Operation.WithIntoAndKeys])

    val blob = op.asInstanceOf[Operation.WithIntoAndKeys]

    val i = blob.into
    val j = blob.keys

    assert(i != null)
    assert(j != null)
    assert(true)

  }

  test("transaction equals content is working")
  {
    val t = buildTransaction
    val t2 = buildTransaction

    assert(t.equalsContent(t))
    assert(t.equalsContent(t2))

    t.variables(0).value = new StringValue("Difference")

    assert(!t.equalsContent(t2))
  }

  test("transaction encode") {

    val t = buildTransaction

    // Check transaction equals is working first!
    t.equalsContent(t)

    val bytes = translator.encodeTransaction(t)

    bytes should not be(new Array[Byte](0))
  }

  test("transaction more complex stuff") {

    val composite = (b: Block with OperationApi) => {
      val context = b.from("context")
      b.returns(b.from("memory").get("key"), context.get("tokens"), context.get("local_node"))
    }

    val t = new Transaction(composite)

    val bytes = translator.encodeTransaction(t)
    val t2 = translator.decodeTransaction(bytes)

    t equalsContent t2 should be(true)
  }

  test("transaction encode/decode") {

    val t = buildTransaction

    // Validate the validate function
    validateLink(t)

    val bytes = translator.encodeTransaction(t)
    val t2 = translator.decodeTransaction(bytes)

    t equalsContent t2 should be(true)

    // Validate the decoded transaction
    validateLink(t2)
  }

  test("transport encode/decode: transaction") {

    val t = buildTransaction

    val transport = new Transport(Some(t), Seq())

    val bytes = translator.encodeAll(transport)
    val transport2 = translator.decodeAll(bytes)

    transport2.request.isDefined should be(true)
    transport2.response should be(Seq())

    transport.request.get equalsContent transport2.request.get should be(true)

    // Validate the decoded transaction
    validateLink(transport.request.get)
    validateLink(transport2.request.get)
  }

  test("transport encode/decode: results") {

    val results = Seq(
      new NullValue(),
      new MapValue(Map("A"-> "1")),
      new ListValue(Seq("2", "3")),
      new StringValue("4"),
      new IntValue(5),
      new BoolValue(true),
      new BoolValue(false),
      new DoubleValue(7.35))

    val transport = new Transport(None, results)

    val bytes = translator.encodeAll(transport)
    val transport2 = translator.decodeAll(bytes)

    val results2 = transport2.response

    transport2.request.isDefined should be(false)

    val merged = results.zip(results2)

    merged.map { (zip) =>
      zip._1 equalsValue zip._2 should be(true)
    }

  }

  private def validateLink(t: Transaction) {
    assert(t.operations(0).source === t)
    assert(t.operations(1).source === t.variables(0))
    assert(t.operations(2).source === t.variables(1))
    assert(t.operations(3).source === t.variables(2))
    assert(t.operations(4).source === t.variables(3))
    assert(t.operations(5).source === t.variables(4))
    assert(t.operations(6).source === t)
  }
}
