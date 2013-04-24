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

  test("should support equals filtering at list level") {
    val expectedMap1Value: MapValue = Map("A" -> toVal("1"))
    val expectedMap2Value: MapValue = Map("A" -> toVal("2"))
    val expectedMap3Value: MapValue = Map("A" -> toVal("2"))

    val expectedList: ListValue = Seq(expectedMap1Value, expectedMap2Value, expectedMap3Value)

    val operationSource: OperationSource = expectedList

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    v.value.asInstanceOf[ListValue].listValue.size should equal(2)
  }


  test("should support equals filtering at list level with heterogeneous list") {

    val expectedMap0Value: IntValue = 1
    val expectedMap1Value: MapValue = Map("A" -> toVal("1"))
    val expectedMap2Value: MapValue = Map("A" -> toVal("2"))
    val expectedMap3Value: MapValue = Map("A" -> toVal("2"))
    val expectedMap4Value: DoubleValue = 4.0

    val expectedList: ListValue = Seq(expectedMap0Value, expectedMap1Value, expectedMap2Value, expectedMap3Value, expectedMap4Value)

    val operationSource: OperationSource = expectedList

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    v.value.asInstanceOf[ListValue].listValue.size should equal(4)
  }

  test("should support equals filtering at map level with heterogeneous map") {

    val expectedMap1Value: MapValue = Map("A" -> toVal("1"))
    val expectedMap2Value: MapValue = Map("A" -> toVal("2"))
    val expectedMap3Value: MapValue = Map("A" -> toVal("2"))

    val expectedList: ListValue = Seq(expectedMap1Value, expectedMap2Value, expectedMap3Value)

    val extraMapValue: IntValue = 1

    val root: MapValue = Map("list" -> expectedList, "extra" -> extraMapValue)

    val operationSource: OperationSource = root

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    val map = v.value.asInstanceOf[MapValue].mapValue

    map("list").asInstanceOf[ListValue].listValue.size should equal(2)
    map("extra") should equal(IntValue(1))
  }

  test("should support equals filtering with map-list-map hierarchy") {
    val expectedMap1Value: MapValue = Map("A" -> toVal("1"))
    val expectedMap2Value: MapValue = Map("A" -> toVal("2"))
    val expectedMap3Value: MapValue = Map("A" -> toVal("2"))

    val expectedList: ListValue = Seq(expectedMap1Value, expectedMap2Value, expectedMap3Value)

    val root: MapValue = Map("list" -> expectedList)

    val operationSource: OperationSource = root

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    v.value.asInstanceOf[MapValue].mapValue("list").asInstanceOf[ListValue].listValue.size should equal(2)
  }

  test("equals filtering recurse list-map (map-list-map-list-map to infinity)") {
    val expectedMap1Value: MapValue = Map("A" -> toVal("1"))
    val expectedMap2Value: MapValue = Map("A" -> toVal("2"))
    val expectedMap3Value: MapValue = Map("A" -> toVal("2"))

    val expectedList2: ListValue = Seq(expectedMap1Value, expectedMap2Value, expectedMap3Value)

    val intermediateMap: MapValue = Map("list2" -> expectedList2)

    val expectedList1: ListValue = Seq(intermediateMap)

    val root: MapValue = Map("list1" -> expectedList1)

    val operationSource: OperationSource = root

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    v.value.asInstanceOf[MapValue].mapValue("list1").asInstanceOf[ListValue].listValue(0).asInstanceOf[MapValue].mapValue("list2").asInstanceOf[ListValue].listValue.size should equal(2)
  }
}
