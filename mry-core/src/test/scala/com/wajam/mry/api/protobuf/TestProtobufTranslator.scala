package com.wajam.mry.api.protobuf

import org.scalatest.FunSuite
import com.wajam.mry.execution.Implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.mry.execution._
import org.scalatest.{Matchers => ShouldMatchers}
import com.wajam.mry.api.Transport
import com.wajam.mry.execution.Operation._
import com.wajam.mry.SlowTest

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
    new Transaction((b) => b.returns(b.from("B").get(1000).set("C").filter("A", MryFilters.Equals, "1").limit(100).projection("D").delete("E")))
  }

  test("test validation function") {
    val t = buildTransaction

    // Validate the validate function
    validateOperationsSources(t, t)
    validateOperationVariablesAreBoundsToBlock(t)
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

  test("transaction encode/decode embedded transaction") {

    val composite = (b: Block with OperationApi) => {
      val context = b.from("context")
      b.returns(b.from("memory").get("key"), context.get("tokens"), context.get("local_node"))
    }

    val t = new Transaction(composite)

    val bytes = translator.encodeTransaction(t)
    val t2 = translator.decodeTransaction(bytes)

    t equalsContent t2 should be(true)
    validateOperationsSources(t, t2)
  }

  test("transaction encode/decode") {

    val t = buildTransaction

    // Validate the validate function
    validateOperationsSources(t, t)

    val bytes = translator.encodeTransaction(t)
    val t2 = translator.decodeTransaction(bytes)

    t equalsContent t2 should be(true)

    // Validate the decoded transaction
    validateOperationsSources(t, t2)
    validateOperationVariablesAreBoundsToBlock(t2)
  }

  test("transport encode/decode: transaction") {

    val t = buildTransaction

    val transport = new Transport(Some(t), None)

    val bytes = translator.encodeAll(transport)
    val transport2 = translator.decodeAll(bytes)

    transport2.transaction.isDefined should be(true)
    transport2.values should be(None)

    transport.transaction.get equalsContent transport2.transaction.get should be(true)

    // Validate the decoded transaction
    validateOperationsSources(transport.transaction.get, transport2.transaction.get)
    validateOperationVariablesAreBoundsToBlock(transport2.transaction.get)
  }

  test("transport encode/decode: results") {

    val results = Seq(
      NullValue,
      new MapValue(Map("A"-> "1")),
      new ListValue(Seq("2", "3")),
      new StringValue("4"),
      new IntValue(5),
      new BoolValue(true),
      new BoolValue(false),
      new DoubleValue(7.35))

    val transport = new Transport(None, Some(results))

    val bytes = translator.encodeAll(transport)
    val transport2 = translator.decodeAll(bytes)

    val results2 = transport2.values

    transport2.transaction.isDefined should be(false)

    val merged = results.zip(results2)

    merged.map { case (z1, z2) =>
      z1 equalsValue z2 should equal (true)
    }
  }

  test("transport encode/decode: empty result") {

    val results: Seq[Value] = Seq()

    val transport = new Transport(None, Some(results))

    val bytes = translator.encodeAll(transport)
    val transport2 = translator.decodeAll(bytes)

    val results2 = transport2.values.get

    transport2.transaction.isDefined should be(false)

    assert(results == results2)
  }

  test("transport encode/decode: with (n > 100K), should terminate and yield the proper size", SlowTest) {

    def randomTransaction(opCount: Int) = {
      val t = new Transaction
      val v = t.from("followees").get(Long.MaxValue)

      for (i <- 1 to opCount)
        v.from("followeers").delete(Long.MaxValue)

      t
    }

    def timed(method: () => Unit): Long = {

      val startTime = System.currentTimeMillis()

      method()

      val endTime = System.currentTimeMillis()

      endTime - startTime
    }

    val n = 100 * 1000

    val t = new ProtobufTranslator()

    val trx: Transaction = randomTransaction(n)

    var bytes: Array[Byte] = null

    val encodeMs = timed {
      () =>  { bytes = t.encodeTransaction(trx) }
    }

    encodeMs should be <= 10000L // Should actually be around 1500ms
    bytes.length should be === 11954748

    val decodeMs = timed {
      () =>  { bytes = t.encodeTransaction(trx) }
    }

    decodeMs should be <= 10000L // Should actually be around 2000ms
  }

  /**
   * Check that all operation.source from t1
   * match the equivalent source from t2
   */
  def validateOperationsSources(t1: Transaction, t2: Transaction) {

    val operations = t1.operations.zip(t2.operations)
    val variables = t1.variables.zip(t2.variables)

    for ((op1, op2) <- operations) {
      if (op1.source eq t1)
      {
        assert(op2.source eq t2)
      }
      else
      {
        val (_, vs) = variables.find((v) => op1.source eq v._1).get
        assert(op2.source eq vs)
      }
    }
  }

  /**
   * Check that all operation variables are bounds to the a variable
   * from block and not a copy
   */
  def validateOperationVariablesAreBoundsToBlock(t: Transaction) {

    for (o <- t.operations) {

      val validateWithFrom = (from: Seq[Variable]) => {
        assert(from.forall((v) => t.variables.exists(v eq _)))
      }

      val validateWithIntoAndSeqObject = (into: Variable, objects: Seq[Object]) => {

        assert(t.variables.exists(_ eq into))
        assert(objects.filter(_.isInstanceOf[Variable]).forall((v) => t.variables.exists(v eq _)))
      }

      val validateFilter = (op: Filter) => {
        assert(t.variables.exists(_ eq op.into))
      }

      o match {
        case op: Return => validateWithFrom(op.from)
        case op: From => validateWithIntoAndSeqObject(op.into, op.keys)
        case op: Get => validateWithIntoAndSeqObject(op.into, op.keys)
        case op: Set => validateWithIntoAndSeqObject(op.into, op.data)
        case op: Delete => validateWithIntoAndSeqObject(op.into, op.data)
        case op: Limit => validateWithIntoAndSeqObject(op.into, op.keys)
        case op: Projection => validateWithIntoAndSeqObject(op.into, op.keys)
        case op: Filter => validateFilter(op)
      }
    }
  }
}