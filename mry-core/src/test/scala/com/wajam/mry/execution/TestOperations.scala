package com.wajam.mry.execution

import org.scalatest.FunSuite
import com.wajam.mry.execution.Implicits._
import org.scalatest.matchers.ShouldMatchers

/**
 * Test non-database dependent Mry Operations
 */
class TestOperations extends FunSuite with ShouldMatchers {

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

    // Add non-map before and after
    val lv = expectedList.listValue
    val expectedList2 = ListValue(lv :+ expectedMap4Value)

    val operationSource: OperationSource = expectedList2

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    val finalList = v.value.asInstanceOf[ListValue]
    assertFilterResult(finalList, 3)
  }

  test("should support equals filtering at map level with heterogeneous map") {

    val expectedList = sampleFilterList()
    val extraMapValue: IntValue = 1

    val root: MapValue = Map("list" -> expectedList, "extra" -> extraMapValue)

    val operationSource: OperationSource = root

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    val map = v.value.asInstanceOf[MapValue].mapValue

    val finalList = map("list").asInstanceOf[ListValue]

    assertFilterResult(finalList, 2)

    map("extra") should equal(IntValue(1))
  }

  test("should support equals filtering with map-list-map hierarchy") {

    val expectedList = sampleFilterList()

    val root: MapValue = Map("list" -> expectedList)

    val operationSource: OperationSource = root

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    val finalList = v.value.asInstanceOf[MapValue].mapValue("list").asInstanceOf[ListValue]

    assertFilterResult(finalList, 2)
  }

  test("equals filtering recurse list-map (map-list-map-list-map to infinity)") {

    val expectedList2 = sampleFilterList()

    val intermediateMap: MapValue = Map("list2" -> expectedList2)

    val expectedList1: ListValue = Seq(intermediateMap)

    val root: MapValue = Map("list1" -> expectedList1)

    val operationSource: OperationSource = root

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    val finalList = v.value.asInstanceOf[MapValue].mapValue("list1").asInstanceOf[ListValue].listValue(0).asInstanceOf[MapValue].mapValue("list2").asInstanceOf[ListValue]

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
