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


  test("should support equals following map-list-map hierarchy") {
    val expectedMap1Value: MapValue = Map("A" -> toVal("1"))
    val expectedMap2Value: MapValue = Map("A" -> toVal("2"))
    val expectedMap3Value: MapValue = Map("A" -> toVal("2"))

    val expectedList: ListValue = Seq(expectedMap1Value, expectedMap2Value, expectedMap3Value)

    val root: MapValue = Map("list" -> expectedList)

    val operationSource: OperationSource = root

    val v = new Variable(null, 0)

    operationSource.execFiltering(null, v, StringValue("A"), MryFilters.Equals, StringValue("2"))

    v.value.asInstanceOf[MapValue].mapValue.size should equal(2)
  }
}
