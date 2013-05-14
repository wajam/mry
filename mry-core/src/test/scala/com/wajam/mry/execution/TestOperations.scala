package com.wajam.mry.execution

import org.scalatest.FunSuite
import com.wajam.mry.execution.Implicits._
import org.scalatest.matchers.ShouldMatchers

/**
 * Test non-database dependent Mry Operations
 */
class TestOperations extends FunSuite with ShouldMatchers {

  // This mock class is used to simulate the proxy concept used in Record and MultipleRecordValue
  case class MockProxy(innerValue: Value) extends Value {

    // Serialized version of this record is the inner map or null
    override def serializableValue = innerValue

    // Operations are executed on record data (map or null)
    override def proxiedSource: Option[OperationSource] = Some(innerValue)
  }

  test("should support projection on single record, at map level") {
    val mapValue: MapValue = Map("key1" -> toVal("value1"), "key2" -> toVal("value2"))
    val operationSource: OperationSource = mapValue

    val v = new Variable(null, 0)

    operationSource.execProjection(null, v, StringValue("key2"))
    v.value should be(MapValue(Map("key2" -> toVal("value2"))))
  }

  test("should single support projection on single record with many keys, at list level") {
    val listValue: ListValue = ListValue(Seq(Map("key1" -> toVal("value1"), "key2" -> toVal("value2"))))
    val operationSource: OperationSource = listValue

    val v = new Variable(null, 0)

    operationSource.execProjection(null, v, StringValue("key2"))
    v.value should be(ListValue(Seq(Map("key2" -> toVal("value2")))))
  }

  test("should support equals filtering at list level") {

    val map1Value: MapValue = Map("A" -> toVal("1"))
    val map2Value: MapValue = Map("A" -> toVal("2"))
    val map3Value: MapValue = Map("A" -> toVal("2"))

    val list: ListValue = Seq(map1Value, map2Value, map3Value)

    val operationSource: OperationSource = list

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    val ListValue(finalList) = v.value

    finalList.listValue.size should equal(2)

    finalList(0) should be(map2Value)
    finalList(1) should be(map3Value)
  }

  test("should support equals filtering at list level with heterogeneous list") {

    val map1Value: MapValue = Map("A" -> toVal("1"))
    val map2Value: MapValue = Map("A" -> toVal("2"))
    val map3Value: MapValue = Map("A" -> toVal("2"))
    val map4Value: DoubleValue = 4.0

    val list: ListValue = Seq(map1Value, map2Value, map3Value, map4Value)

    val operationSource: OperationSource = list

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    val ListValue(finalList) = v.value

    finalList.listValue.size should equal(2)

    finalList(0) should be(map2Value)
    finalList(1) should be(map3Value)
  }

  test("should support equals filtering at list level with proxies") {

    val map1Value = MockProxy(Map("A" -> toVal("1")))
    val map2Value = MockProxy(Map("A" -> toVal("2")))
    val map3Value = MockProxy(Map("A" -> toVal("2")))

    val list = MockProxy(Seq(map1Value, map2Value, map3Value))

    val operationSource: OperationSource = list

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    val ListValue(finalList) = v.value

    finalList.listValue.size should equal(2)

    finalList(0) should be(map2Value)
    finalList(1) should be(map3Value)
  }

  test("lte filtering") {

    val map1Value: MapValue = Map("A" -> toVal(1))
    val map2Value: MapValue = Map("A" -> toVal(2))
    val map3Value: MapValue = Map("A" -> toVal(2))
    val map4Value: MapValue = Map("A" -> toVal(3))

    val list: ListValue = Seq(map1Value, map2Value, map3Value, map4Value)

    val operationSource: OperationSource = list

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.LesserThanOrEqual, IntValue(2))

    val ListValue(finalList) = v.value
    finalList.size should equal(3)

    finalList(0) should be(map1Value)
    finalList(1) should be(map2Value)
    finalList(2) should be(map3Value)
  }

  test("lt filtering") {

    val map1Value: MapValue = Map("A" -> toVal(1))
    val map2Value: MapValue = Map("A" -> toVal(2))
    val map3Value: MapValue = Map("A" -> toVal(2))
    val map4Value: MapValue = Map("A" -> toVal(3))

    val list: ListValue = Seq(map1Value, map2Value, map3Value, map4Value)

    val operationSource: OperationSource = list

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.LesserThan, IntValue(3))

    val ListValue(finalList) = v.value
    finalList.size should equal(3)

    finalList(0) should be(map1Value)
    finalList(1) should be(map2Value)
    finalList(2) should be(map2Value)
  }

  test("gt filtering") {

    val map1Value: MapValue = Map("A" -> toVal(1))
    val map2Value: MapValue = Map("A" -> toVal(2))
    val map3Value: MapValue = Map("A" -> toVal(2))
    val map4Value: MapValue = Map("A" -> toVal(3))

    val list: ListValue = Seq(map1Value, map2Value, map3Value, map4Value)

    val operationSource: OperationSource = list

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.GreaterThan, IntValue(2))

    val ListValue(finalList) = v.value
    finalList.size should equal(1)

    finalList(0) should be(map4Value)

  }

  test("gte filtering") {

    val map1Value: MapValue = Map("A" -> toVal(1))
    val map2Value: MapValue = Map("A" -> toVal(2))
    val map3Value: MapValue = Map("A" -> toVal(2))
    val map4Value: MapValue = Map("A" -> toVal(3))

    val list: ListValue = Seq(map1Value, map2Value, map3Value, map4Value)

    val operationSource: OperationSource = list

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.GreaterThanOrEqual, IntValue(2))

    val ListValue(finalList) = v.value
    finalList.size should equal(3)

    finalList(0) should be(map2Value)
    finalList(1) should be(map3Value)
    finalList(2) should be(map4Value)
  }
}
