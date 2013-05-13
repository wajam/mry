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

  def sampleFilterList(): ListValue = {
    val expectedMap1Value: MapValue = Map("A" -> toVal("1"))
    val expectedMap2Value: MapValue = Map("A" -> toVal("2"))
    val expectedMap3Value: MapValue = Map("A" -> toVal("2"))

    val expectedList: ListValue = Seq(expectedMap1Value, expectedMap2Value, expectedMap3Value)

    expectedList
  }

  def sampleProxyFilterList() = {
    val expectedMap1Value = MockProxy(Map("A" -> toVal("1")))
    val expectedMap2Value = MockProxy(Map("A" -> toVal("2")))
    val expectedMap3Value = MockProxy(Map("A" -> toVal("2")))

    val expectedList = MockProxy(Seq(expectedMap1Value, expectedMap2Value, expectedMap3Value))

    expectedList
  }

  def sampleFilterList2(): ListValue = {
    val expectedMap1Value: MapValue = Map("A" -> toVal(1))
    val expectedMap2Value: MapValue = Map("A" -> toVal(2))
    val expectedMap3Value: MapValue = Map("A" -> toVal(2))
    val expectedMap4Value: MapValue = Map("A" -> toVal(3))

    val expectedList: ListValue = Seq(expectedMap1Value, expectedMap2Value, expectedMap3Value, expectedMap4Value)

    expectedList
  }

  def assertFilterResult(list: ListValue, count: Int) {

    list.listValue.size should equal(count)

    val target = MapValue(Map("A" -> toVal("2")))

    list(0) should be(target)
    list(1) should be(target)
  }

  test("should support equals filtering at list level") {

    val expectedList = sampleFilterList()

    val operationSource: OperationSource = expectedList

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    val finalList = v.value.asInstanceOf[ListValue]
    assertFilterResult(finalList, 2)
  }

  test("should support equals filtering at list level with heterogeneous list") {

    val expectedMap4Value: DoubleValue = 4.0

    val expectedList = sampleFilterList()

    val lv = expectedList.listValue
    val expectedList2 = ListValue(lv :+ expectedMap4Value)

    val operationSource: OperationSource = expectedList2

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    val finalList = v.value.asInstanceOf[ListValue]

    // Should drop all non maps, and should works
    assertFilterResult(finalList, 2)
  }

  test("should support equals filtering at list level with proxies") {

    val expectedList = sampleProxyFilterList()

    val operationSource: OperationSource = expectedList

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    // Materialize the list
    val finalList = v.value.asInstanceOf[ListValue].map { case(v) => v.serializableValue}

    assertFilterResult(finalList, 2)
  }

  test("lte filtering") {

    val expectedList = sampleFilterList2()

    val operationSource: OperationSource = expectedList

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.LesserThanOrEqual, IntValue(2))

    val finalList = v.value.asInstanceOf[ListValue]
    finalList.size should equal(3)

    finalList(0) should be(MapValue(Map("A" -> toVal(1))))
    finalList(1) should be(MapValue(Map("A" -> toVal(2))))
    finalList(2) should be(MapValue(Map("A" -> toVal(2))))
  }

  test("lt filtering") {

    val expectedList = sampleFilterList2()

    val operationSource: OperationSource = expectedList

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.LesserThan, IntValue(3))

    val finalList = v.value.asInstanceOf[ListValue]
    finalList.size should equal(3)

    finalList(0) should be(MapValue(Map("A" -> toVal(1))))
    finalList(1) should be(MapValue(Map("A" -> toVal(2))))
    finalList(2) should be(MapValue(Map("A" -> toVal(2))))
  }

  test("gt filtering") {

    val expectedList = sampleFilterList2()

    val operationSource: OperationSource = expectedList

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.GreaterThan, IntValue(2))

    val finalList = v.value.asInstanceOf[ListValue]
    finalList.size should equal(1)

    finalList(0) should be(MapValue(Map("A" -> toVal(3))))

  }

  test("gte filtering") {

    val expectedList = sampleFilterList2()

    val operationSource: OperationSource = expectedList

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.GreaterThanOrEqual, IntValue(2))

    val finalList = v.value.asInstanceOf[ListValue]
    finalList.size should equal(3)

    finalList(0) should be(MapValue(Map("A" -> toVal(2))))
    finalList(1) should be(MapValue(Map("A" -> toVal(2))))
    finalList(2) should be(MapValue(Map("A" -> toVal(3))))
  }
}
