package com.wajam.mry.storage.mysql

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.mry.execution.Implicits._
import com.wajam.mry.execution._
import com.wajam.mry.storage.StorageException
import collection.mutable
import util.Random
import com.wajam.mry.storage.mysql.TimelineSelectMode.AtTimestamp
import com.wajam.nrv.service.TokenRange
import org.mockito.Mockito._
import org.scalatest.{Matchers => ShouldMatchers, FunSuite}
import com.wajam.nrv.utils.TimestampIdGenerator
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Test MySQL storage
 */
@RunWith(classOf[JUnitRunner])
class TestMysqlStorage extends FunSuite with MysqlStorageFixture with ShouldMatchers {

  def createNowTimestamp() = Timestamp(System.currentTimeMillis(), 0)

  testsFor(apiOperations("MysqlStorage")(createMysqlStorage _))
  testsFor(apiOperations("CachedMysqlStorage")(createCachedMysqlStorage _))

  def apiOperations(textPrefix: String)(implicit createStorage: () => MysqlStorage) {
    test(s"$textPrefix - should get committed record") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", Map("mapk" -> toVal("value1")))
        }, commit = true)

        val Seq(v) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          t.ret(table.get("key1"))
        }, commit = false)

        v.value.serializableValue match {
          case m: MapValue =>
            assert(m("mapk").equalsValue("value1"))
          case _ =>
            fail("Didn't receive a map")
        }
      }
    }

    test(s"$textPrefix - should support filtering") {
      withFixture { f =>
        val mapValue1: MapValue = Map("A" -> toVal("1"))
        val mapValue2: MapValue = Map("A" -> toVal("2"))
        val mapValue3: MapValue = Map("A" -> toVal("2"))

        val expectedList: ListValue = Seq(mapValue1, mapValue2, mapValue3)

        val metaMapValue: MapValue = Map("list" -> expectedList)

        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", metaMapValue)
        }, commit = true)

        val Seq(v) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          t.ret(table.get("key1").filter("A", MryFilters.Equals, "2"))
        }, commit = false)

        val MapValue(mapValue) = v.value
        val ListValue(finalList) = mapValue("list")

        finalList.size should equal(2)

        finalList(0) should equal(mapValue2)
        finalList(1) should equal(mapValue3)
      }
    }

    test(s"$textPrefix - should support filtering, lte") {
      withFixture { f =>
        val mapValue1: MapValue = Map("A" -> toVal(1))
        val mapValue2: MapValue = Map("A" -> toVal(2))
        val mapValue3: MapValue = Map("A" -> toVal(4))

        val expectedList: ListValue = Seq(mapValue1, mapValue2, mapValue3)

        val metaMapValue: MapValue = Map("list" -> expectedList)

        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", metaMapValue)
        }, commit = true)

        val Seq(v) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          t.ret(table.get("key1").filter("A", MryFilters.LesserThanOrEqual, 3))
        }, commit = false)

        val MapValue(mapValue) = v.value
        val ListValue(finalList) = mapValue("list")

        finalList.size should equal(2)

        finalList(0) should equal(mapValue1)
        finalList(1) should equal(mapValue2)
      }
    }

    test(s"$textPrefix - should support projection on single record") {
      withFixture { f =>
        val mapValue: MapValue = Map("mapk" -> toVal("value1"))
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", mapValue)
        }, commit = true)

        val Seq(v) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          t.ret(table.get("key1").projection("mapk"))
        }, commit = false)

        assert(v.equalsValue(mapValue))
      }
    }

    test(s"$textPrefix - empty projection should return all fields") {
      withFixture { f =>
        val mapValue: MapValue = Map("mapk" -> toVal("value1"))
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", mapValue)
        }, commit = true)

        val Seq(v) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          t.ret(table.get("key1").projection())
        }, commit = false)

        assert(v.equalsValue(mapValue))
      }
    }


    test(s"$textPrefix - should support multiple projection on single record") {
      withFixture { f =>
        val mapValue: MapValue = Map("key1" -> toVal("value1"), "key2" -> toVal("value2"))
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", mapValue)
        }, commit = true)

        val Seq(v) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          t.ret(table.get("key1").projection("key1", "key2", "other"))
        }, commit = false)

        assert(v.equalsValue(mapValue))
      }
    }

    test(s"$textPrefix - should support projection that results in empty record") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", Map("mapk" -> toVal("value1")))
        }, commit = true)

        val Seq(v) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          t.ret(table.get("key1").projection("noproj"))
        }, commit = false)

        assert(v.equalsValue(MapValue(Map[String, Value]())))
      }
    }

    test(s"$textPrefix - should support projection on record list") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", Map("mapk" -> toVal("value1")))
          val table1_1 = table.get("key1").from("table1_1")
          table1_1.set("key1", Map("mapk" -> toVal("value1")))
          table1_1.set("key2", Map("mapk" -> toVal("value2"), "noproj" -> toVal("x")))
          table1_1.set("key3", Map("noproj" -> toVal("x")))
        }, commit = true)

        val Seq(ListValue(Seq(MapValue(m1), MapValue(m2), MapValue(m3)))) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          t.ret(table.get("key1").from("table1_1").get().projection("mapk"))
        }, commit = false)

        assert(m1 === Map("mapk" -> toVal("value1")))
        assert(m2 === Map("mapk" -> toVal("value2")))
        assert(m3.isEmpty)
      }
    }

    test(s"$textPrefix - empty projection should not filter fields on record list") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", Map("mapk" -> toVal("value1")))
          val table1_1 = table.get("key1").from("table1_1")
          table1_1.set("key1", Map("mapk" -> toVal("value1")))
          table1_1.set("key2", Map("mapk" -> toVal("value2"), "noproj" -> toVal("x")))
          table1_1.set("key3", Map("noproj" -> toVal("x")))
        }, commit = true)

        val Seq(ListValue(Seq(MapValue(m1), MapValue(m2), MapValue(m3)))) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          t.ret(table.get("key1").from("table1_1").get().projection())
        }, commit = false)

        assert(m1 === Map("mapk" -> toVal("value1")))
        assert(m2 === Map("mapk" -> toVal("value2"), "noproj" -> toVal("x")))
        assert(m3 === Map("noproj" -> toVal("x")))
      }
    }

    test(s"$textPrefix - should support projection on empty record list") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", Map("mapk" -> toVal("value1")))
        }, commit = true)

        val Seq(ListValue(l)) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          t.ret(table.get("key1").from("table1_1").get().projection("mapk"))
        }, commit = false)

        assert(l.isEmpty)
      }
    }

    test(s"$textPrefix - a get before a set should return initial value, not new value") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", Map("mapk" -> toVal("value1")))
          table.set("key2", Map("mapk" -> toVal("value2")))
        }, commit = true)

        val Seq(rec1, rec2, rec3, rec4) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          val a = table.get("key1")
          val b = table.get("key2")
          table.set("key1", Map("mapk" -> toVal("value1.2")))
          table.delete("key2")
          val c = table.get("key1")
          val d = table.get("key2")
          t.returns(a, b, c, d)
        }, commit = true)


        val val1 = rec1.asInstanceOf[MapValue].mapValue("mapk")
        val val2 = rec2.asInstanceOf[MapValue].mapValue("mapk")
        val val3 = rec3.asInstanceOf[MapValue].mapValue("mapk")
        assert(rec4.equalsValue(NullValue))

        assert(val1.equalsValue("value1"), val1)
        assert(val2.equalsValue("value2"), val2)
        assert(val3.equalsValue("value1.2"), val3)
      }
    }

    test(s"$textPrefix - a get before a delete should return initial value, not null") {
      withFixture { f =>
        f.exec(t => {
          t.from("mysql").from("table1").set("key1", Map("mapk" -> toVal("value1")))
        }, commit = true)

        val Seq(rec1, rec2) = f.exec(t => {
          val table = t.from("mysql").from("table1")
          val a = table.get("key1")
          table.delete("key1")
          val b = table.get("key1")
          t.returns(a, b)
        }, commit = true)

        val val1 = rec1.asInstanceOf[MapValue].mapValue("mapk")
        assert(rec2.equalsValue(NullValue))
        assert(val1.equalsValue("value1"), val1)
      }
    }

    test(s"$textPrefix - a get before a set on multi level hierarchy should return initial values, not new values") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", Map("mapk" -> toVal("value1")))
          val rec1 = table.get("key1")
          rec1.from("table1_1").set("key1_1", Map("mapk" -> toVal("value1_1a")))
          rec1.from("table1_1").set("key1_2", Map("mapk" -> toVal("value1_2a")))
          rec1.from("table1_1").set("key1_3", Map("mapk" -> toVal("value1_3a")))

        }, commit = true)

        val Seq(rec1, rec2) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1").get("key1").from("table1_1")
          val a = table.get()
          table.delete("key1_1")
          table.set("key1_2", Map("mapk" -> toVal("value1_2b")))
          val b = table.get()
          t.returns(a, b)
        }, commit = true)

        val val1 = rec1.asInstanceOf[ListValue]
        val val2 = rec2.asInstanceOf[ListValue]
        val1.listValue.size should be(3)
        val2.listValue.size should be(2)
        val2.listValue(1).asInstanceOf[MapValue].mapValue("mapk").equalsValue("value1_2b")
      }
    }


    test(s"$textPrefix - should not get uncommited record") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key2", Map("mapk" -> toVal("value2")))
        }, commit = false)

        val Seq(v) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          t.ret(table.get("key2"))
        }, commit = false)
        assert(v.value.serializableValue.equalsValue(NullValue))
      }
    }

    test(s"$textPrefix - should support multi hierarchy table") {
      withFixture { f =>
        val Seq(record1, record2, record3) = f.exec(t => {
          val storage = t.from("mysql")
          val table1 = storage.from("table1")
          table1.set("k1", Map("k" -> "v1"))

          val record1 = table1.get("k1")
          val table1_1 = record1.from("table1_1")
          table1_1.set("k1.2", Map("k" -> "v1.2"))
          val record2 = table1_1.get("k1.2")

          table1_1.get("k1.2").from("table1_1_1").set("k1.2.1", Map("k" -> "v1.2.1"))
          val record3 = table1_1.get("k1.2").from("table1_1_1").get("k1.2.1")

          t.ret(record1, record2, record3)
        }, commit = true)

        assert(record1.asInstanceOf[MapValue].mapValue("k").equalsValue("v1"))
        assert(record2.asInstanceOf[MapValue].mapValue("k").equalsValue("v1.2"))
        assert(record3.asInstanceOf[MapValue].mapValue("k").equalsValue("v1.2.1"))
      }
    }

    test(s"$textPrefix - should not be able to get multiple rows on first depth table") {
      withFixture { f =>
        intercept[InvalidParameter] {
          val Seq(records) = f.exec(t => {
            t.ret(t.from("mysql").from("table1").get())
          })
        }
      }
    }

    test(s"$textPrefix - should be able to get multiple rows on second and third depth table") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", Map("k" -> "value1"))
          table.set("key2", Map("k" -> "value2"))
          table.set("key3", Map("k" -> "value3"))
          table.delete("key1")
          table.get("key3").from("table1_1").set("key3.1", Map("k" -> "value3.1"))
          table.get("key3").from("table1_1").set("key3.2", Map("k" -> "value3.2"))
          table.get("key3").from("table1_1").get("key3.1").from("table1_1_1").set("key3.1.1", Map("k" -> "value3.1.1"))
          table.get("key3").from("table1_1").get("key3.1").from("table1_1_1").set("key3.1.2", Map("k" -> "value3.1.2"))
          table.get("key3").from("table1_1").get("key3.2").from("table1_1_1").set("key3.3.1", Map("k" -> "value3.3.1"))
        }, commit = true)

        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.get("key2").from("table1_1").set("key2.1", Map("k" -> "value2.1"))
          table.get("key3").from("table1_1").set("key3.3", Map("k" -> "value3.3"))
        }, commit = true)


        // shouldn't be able to get a record on second hiearchy if first hierarchy record doesn't exist
        intercept[StorageException] {
          f.exec(t => {
            val rec1 = t.from("mysql").from("table1").get("key1").from("table1_1").get()
            t.ret(rec1)
          })
        }

        // shouldn't be able to get a record on third hiearchy if second hierarchy record doesn't exist
        intercept[StorageException] {
          f.exec(t => {
            val rec1 = t.from("mysql").from("table1").get("key3").from("table1_1").get("key3.4").from("table1_1_1").get()
            t.ret(rec1)
          })
        }

        val Seq(records1, records2) = f.exec(t => {
          val rec1 = t.from("mysql").from("table1").get("key2").from("table1_1").get()
          val rec2 = t.from("mysql").from("table1").get("key3").from("table1_1").get()
          t.ret(rec1, rec2)
        })

        assert(records1.asInstanceOf[ListValue].listValue.size == 1)
        assert(records2.asInstanceOf[ListValue].listValue.size == 3)

        val Seq(records3) = f.exec(t => {
          val rec1 = t.from("mysql").from("table1").get("key3").from("table1_1").get("key3.1").from("table1_1_1").get()
          t.ret(rec1)
        })

        assert(records3.asInstanceOf[ListValue].listValue.size == 2)
      }
    }

    test(s"$textPrefix - should support delete") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", Map("k" -> "value1"))
          table.set("key2", Map("k" -> "value2"))
          table.set("key3", Map("k" -> "value3"))
          table.delete("key1")
          table.get("key3").from("table1_1").set("key3.1", Map("k" -> "value3.1"))
          table.get("key3").from("table1_1").set("key3.2", Map("k" -> "value3.2"))
          table.get("key3").from("table1_1").delete("key3.1")
        }, commit = true)


        val Seq(v1, v2, v3, v4, v5) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.delete("key2")
          val v1 = table.get("key1")
          val v2 = table.get("key2")
          val v3 = table.get("key3")
          val v4 = table.get("key3").from("table1_1").get("key3.1")
          val v5 = table.get("key3").from("table1_1").get("key3.2")
          t.ret(v1, v2, v3, v4, v5)
        }, commit = true)

        assert(v1.equalsValue(NullValue))
        assert(v2.equalsValue(NullValue))
        assert(!v3.equalsValue(NullValue))
        assert(v4.equalsValue(NullValue))
        assert(!v5.equalsValue(NullValue))
      }
    }

    test(s"$textPrefix - deletion should be a tombstone record") {
      withFixture { f =>
        val transac = new MysqlTransaction(f.mysqlStorage, None)
        val initRec = new Record(table1, Map("test" -> 1234))
        val path = new AccessPath(Seq(new AccessKey("test1")))
        transac.set(table1, 1, createNowTimestamp(), path, Some(initRec))

        val rec1 = transac.get(table1, 1, createNowTimestamp(), path)
        assert(rec1.get.value.asInstanceOf[MapValue]("test").equalsValue(1234))

        transac.set(table1, 1, createNowTimestamp(), path, None)

        val rec2 = transac.get(table1, 1, createNowTimestamp(), path)
        assert(rec2.isEmpty)

        val rec3 = transac.get(table1, 1, createNowTimestamp(), path, includeDeleted = true)
        assert(rec3.isDefined)
        assert(rec3.get.value.isNull)

        transac.commit()
      }
    }

    test(s"$textPrefix - deleting parent should delete children") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("k1", Map("k" -> "value1@1"))
          table.get("k1").from("table1_1").set("k1.1", Map("k" -> "value1.1@1"))
          table.get("k1").from("table1_1").get("k1.1").from("table1_1_1").set("k1.1.1", Map("k" -> "value1.1.1@1"))
          table.set("k2", Map("k" -> "value1@1"))
          table.get("k2").from("table1_1").set("k2.1", Map("k" -> "value2.1@1"))
          table.delete("k2")
        })

        val Seq(rec1, rec2, rec3) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.delete("k1")

          table.set("k1", Map("k" -> "value1@2"))
          table.set("k2", Map("k" -> "value2@2"))

          val rec1 = table.get("k1").from("table1_1").get("k1.1")
          val rec2 = table.get("k2").from("table1_1").get("k2.1")

          table.get("k1").from("table1_1").set("k1.1", Map("k" -> "value1.1@2"))
          val rec3 = table.get("k1").from("table1_1").get("k1.1").from("table1_1_1").get("k1.1.1")

          t.ret(rec1, rec2, rec3)
        })

        assert(rec1.isNull)
        assert(rec2.isNull)
        assert(rec3.isNull)

        val Seq(rec4, rec5) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          val rec4 = table.get("k1").from("table1_1").get("k1.1")
          val rec5 = table.get("k2").from("table1_1").get("k2.1")

          t.ret(rec4, rec5)
        })
        assert(rec4.asInstanceOf[MapValue]("k").equalsValue("value1.1@2"))
        assert(rec5.isNull)
      }
    }

    test(s"$textPrefix - operations should be kept in a history") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", Map("k" -> "value1"))
          table.set("key2", Map("k" -> "value2.0"))
          table.set("key3", Map("k" -> "value3"))
          table.delete("key1")
          table.get("key3").from("table1_1").set("key3.1", Map("k" -> "value3.1"))
          table.get("key3").from("table1_1").set("key3.2", Map("k" -> "value3.2"))
          table.get("key3").from("table1_1").delete("key3.1")
          table.get("key3").from("table1_1").get("key3.2").from("table1_1_1").set("key3.2.1", Map("k" -> "value3.2.1"))
        }, commit = true, onTimestamp = Timestamp(10))

        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key4", Map("k" -> "value4"))
          table.get("key2").from("table1_1").set("key2.1", Map("k" -> "value2.1"))
          table.get("key2").from("table1_1").get("key2.1").from("table1_1_1").set("key2.1.1", Map("k" -> "value2.1.1"))
        }, commit = false, onTimestamp = Timestamp(100))

        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key5", Map("k" -> "value5"))
          table.set("key2", Map("k" -> "value2.1"))
          table.get("key2").from("table1_1").set("key2.2", Map("k" -> "value2.1"))
          table.get("key2").from("table1_1").get("key2.2").from("table1_1_1").set("key2.2.1", Map("k" -> "value2.2.1"))
          table.delete("key3")
        }, commit = true, onTimestamp = Timestamp(200))

        val context = new ExecutionContext(f.storages)
        val table1Timeline = f.mysqlStorage.createStorageTransaction(context).getTimeline(table1, Timestamp(0), 100)

        assert(table1Timeline.size === 6)
        assert(table1Timeline(0).accessPath.keys(0) == "key1", table1Timeline(0).accessPath.keys(0))
        assert(table1Timeline(0).newValue.isEmpty)
        assert(table1Timeline(1).accessPath.keys(0) == "key2", table1Timeline(0).accessPath.keys(0))
        table1Timeline(1).newValue match {
          case Some(m: MapValue) => assert("value2.0".equalsValue(m.mapValue("k")))
          case _ => fail()
        }
        assert(table1Timeline(2).accessPath.keys(0) == "key3", table1Timeline(0).accessPath.keys(0))
        table1Timeline(2).newValue match {
          case Some(m: MapValue) => assert("value3".equalsValue(m.mapValue("k")))
          case _ => fail()
        }
        assert(table1Timeline(3).accessPath.keys(0) == "key5", table1Timeline(0).accessPath.keys(0))
        table1Timeline(3).newValue match {
          case Some(m: MapValue) => assert("value5".equalsValue(m.mapValue("k")))
          case _ => fail()
        }
        assert(table1Timeline(4).accessPath.keys(0) == "key2", table1Timeline(0).accessPath.keys(0))
        table1Timeline(4).newValue match {
          case Some(m: MapValue) => assert("value2.1".equalsValue(m.mapValue("k")))
          case _ => fail()
        }
        table1Timeline(4).oldValue match {
          case Some(m: MapValue) => assert("value2.0".equalsValue(m.mapValue("k")))
          case _ => fail()
        }
        assert(table1Timeline(5).accessPath.keys(0) == "key3", table1Timeline(0).accessPath.keys(0))
        assert(table1Timeline(5).newValue.isEmpty)
        table1Timeline(5).oldValue match {
          case Some(m: MapValue) => assert("value3".equalsValue(m.mapValue("k")))
          case _ => fail()
        }


        val table1_1Timeline = f.mysqlStorage.createStorageTransaction(context).getTimeline(table1_1, Timestamp(0), 100)
        assert(table1_1Timeline.size == 5, table1_1Timeline.size)

        val table1_1_1Timeline = f.mysqlStorage.createStorageTransaction(context).getTimeline(table1_1_1, Timestamp(0), 100)
        assert(table1_1_1Timeline.size == 3, table1_1_1Timeline.size)

        val limitedTable1Timeline = f.mysqlStorage.createStorageTransaction(context).getTimeline(table1, Timestamp(0), 2)
        limitedTable1Timeline.size should be(2)

        val table1TimelineAt10 = f.mysqlStorage.createStorageTransaction(context).getTimeline(table1, Timestamp(10),
          0, selectMode = AtTimestamp)
        table1TimelineAt10.size should be(3)

        val table1TimelineAt100 = f.mysqlStorage.createStorageTransaction(context).getTimeline(table1, Timestamp(100),
          0, selectMode = AtTimestamp)
        table1TimelineAt100.size should be(0)

        val table1TimelineAt200 = f.mysqlStorage.createStorageTransaction(context).getTimeline(table1, Timestamp(200),
          0, selectMode = AtTimestamp)
        table1TimelineAt200.size should be(3)

        val token1 = context.getToken("key1")
        val token2 = context.getToken("key2")
        val ranges = List(TokenRange(token1, token1), TokenRange(token2, token2))
        val table1RangeTimeline = f.mysqlStorage.createStorageTransaction(context).getTimeline(table1, Timestamp(0), 100, ranges)
        val allKeys = table1Timeline.map(_.accessPath.toString)
        val expectedRangeKeys = table1Timeline.filter(r => token1 == r.token || token2 == r.token).map(_.accessPath.toString)
        val actualRangeKeys = table1RangeTimeline.map(_.accessPath.toString)
        allKeys.size should be > (expectedRangeKeys.size)
        expectedRangeKeys.size should be >= 2
        actualRangeKeys should be(expectedRangeKeys)
      }
    }

    test(s"$textPrefix - deleted parents should generate deletion history for children") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", Map("k" -> "value1"))
          table.set("key2", Map("k" -> "value2"))
          table.get("key1").from("table1_1").set("key1.1", Map("k" -> "value1.1"))
          table.get("key2").from("table1_1").set("key2.1", Map("k" -> "value2.1"))
          table.delete("key2")
        }, commit = true, onTimestamp = Timestamp(10))

        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.delete("key1")
        }, commit = true, onTimestamp = Timestamp(100))

        val context = new ExecutionContext(f.storages)
        val table1_1Timeline = f.mysqlStorage.createStorageTransaction(context).getTimeline(table1_1, Timestamp(0), 100)
        assert(table1_1Timeline.size === 3, table1_1Timeline.size)
      }
    }

    test(s"$textPrefix - should be able to set the same key twice and keep the last version") {
      withFixture { f =>
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          table.set("key1", Map("k" -> toVal("value1")))
          table.set("key1", Map("k" -> toVal("value2")))
        }, commit = true)

        val Seq(a, b) = f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from("table1")
          val a = table.get("key1")
          table.set("key1", Map("k" -> toVal("value3")))
          val b = table.get("key1")

          t.ret(a, b)
        }, commit = false)

        a.value.serializableValue match {
          case m: MapValue => assert(m("k").equalsValue("value2"))
          case _ => fail()
        }

        b.value.serializableValue match {
          case m: MapValue => assert(m("k").equalsValue("value3"))
          case _ => fail()
        }
      }
    }

    test(s"$textPrefix - calling limit on a multi-record value should offset and limit the number of records returned") {
      withFixture { f =>
        val keys = List.range(0, 40).map(i => {
          "key%02d".format(i)
        })

        val Seq(a, b, c, d) = f.exec(t => {
          val t1 = t.from("mysql").from("table1")
          t1.set("key1", Map("k" -> "v"))
          val t2 = t1.get("key1").from("table1_1")
          keys foreach (key => t2.set(key, Map(key -> key)))

          val a = t2.get()
          val b = t2.get().limit(10)
          val c = t2.get().limit(0, 10)
          val d = t2.get().limit(10, 10)

          t.returns(a, b, c, d)
        }, commit = true, onTimestamp = Timestamp(0))

        val la = a.asInstanceOf[ListValue].flatMap(_.asInstanceOf[MapValue].mapValue.values).map(_.toString)
        val lb = b.asInstanceOf[ListValue].flatMap(_.asInstanceOf[MapValue].mapValue.values).map(_.toString)
        val lc = c.asInstanceOf[ListValue].flatMap(_.asInstanceOf[MapValue].mapValue.values).map(_.toString)
        val ld = d.asInstanceOf[ListValue].flatMap(_.asInstanceOf[MapValue].mapValue.values).map(_.toString)

        la should be(keys)
        lb should be(keys.slice(0, 10))
        lc should be(keys.slice(0, 10))
        ld should be(keys.slice(10, 20))
      }
    }


    test(s"$textPrefix - test limit when first element deleted") {
      withFixture { f =>
        f.exec(t => {
          t.from("mysql").from("table1").set("elem1", Map("id" -> "elem1"))
        }, commit = true)

        f.exec(t => {
          val table11 = t.from("mysql").from("table1").get("elem1").from("table1_1")
          (1 to 10) foreach (id => table11.set(id.toString, Map("id" -> id)))
        }, commit = true)

        f.exec(t => {
          t.from("mysql").from("table1").get("elem1").from("table1_1").delete("1")
        }, commit = true)

        val Seq(ListValue(limited)) = f.exec(t => {
          t.ret(t.from("mysql").from("table1").get("elem1").from("table1_1").get().limit(1))
        })

        limited should not be ('empty)
        limited should have size (1)
      }
    }
  }

  test("forced garbage collections should truncate versions and keep enough versions") {
    withFixture { f =>
      val values = mutable.Map[String, Int]()
      val rand = new Random(3234234)
      var totalCollected = 0

      for (i <- 0 to 1000) {
        val k = rand.nextInt(100).toString
        val v = values.getOrElse(k, rand.nextInt(1000))

        f.exec(t => {
          val storage = t.from("mysql")
          storage.from("table2").set(k, Map("k" -> v))
          storage.from("table2").get(k).from("table2_1").set(k, Map("k" -> v))
          storage.from("table2").get(k).from("table2_1").get(k).from("table2_1_1").set(k, Map("k" -> v))
        }, commit = true)

        if (rand.nextInt(10) == 5) {
          var trx = f.mysqlStorage.createStorageTransaction
          val beforeSizeTable2 = trx.getSize(table2)
          val beforeSizeTable2_1 = trx.getSize(table2_1)
          val beforeSizeTable2_1_1 = trx.getSize(table2_1_1)
          val totalBeforeSize = beforeSizeTable2 + beforeSizeTable2_1 + beforeSizeTable2_1_1
          trx.rollback()

          val collected = f.mysqlStorage.GarbageCollector.collectAll(rand.nextInt(10))
          totalCollected += collected

          trx = f.mysqlStorage.createStorageTransaction
          val afterSizeTable2 = trx.getSize(table2)
          val afterSizeTable2_1 = trx.getSize(table2_1)
          val afterSizeTable2_1_1 = trx.getSize(table2_1_1)
          val totalAfterSize = afterSizeTable2 + afterSizeTable2_1 + afterSizeTable2_1_1
          trx.rollback()

          assert((totalBeforeSize - collected) == totalAfterSize, "after %d > before %d, deleted %d".format(afterSizeTable2, beforeSizeTable2, collected))
        }

        values += (k -> v)
      }

      totalCollected should be > (0)

      for ((k, v) <- values) {
        val Seq(rec1, rec2, rec3) = f.exec(t => {
          val storage = t.from("mysql")
          val rec1 = storage.from("table2").get(k)
          val rec2 = storage.from("table2").get(k).from("table2_1").get(k)
          val rec3 = storage.from("table2").get(k).from("table2_1").get(k).from("table2_1_1").get(k)
          t.ret(rec1, rec2, rec3)
        }, commit = true)

        val val1 = rec1.asInstanceOf[MapValue].mapValue("k")
        val val2 = rec2.asInstanceOf[MapValue].mapValue("k")
        val val3 = rec2.asInstanceOf[MapValue].mapValue("k")

        assert(val1.equalsValue(v), "%s!=%s".format(rec1, v))
        assert(val2.equalsValue(v), "%s!=%s".format(rec2, v))
        assert(val3.equalsValue(v), "%s!=%s".format(rec3, v))
      }
    }
  }

  test("force garbage collections should delete all extra versions of a token before going to the next token") {
    withFixture { f =>
      val context = new ExecutionContext(f.storages)
      val keys = 0.to(5).map(i => {
        val key = "key%d".format(i)
        (context.getToken(key), key)
      }).sorted

      def createRecords(key: String, ver: Int) {
        println("createRecords " + key)
        for (i <- 0.until(ver)) {
          f.exec(t => {
            val t1 = t.from("mysql").from("table1")
            t1.set(key, Map(key -> i.toString))
          }, commit = true, onTimestamp = Timestamp(i))
        }
      }

      createRecords(key = keys(0)._2, ver = 5)
      createRecords(key = keys(1)._2, ver = 10)
      createRecords(key = keys(2)._2, ver = 100)
      createRecords(key = keys(3)._2, ver = 500)
      createRecords(key = keys(4)._2, ver = 5)

      // Verify versions before GC
      getTokenVersion(token = keys(0)._1).get.versionsCount should be(5)
      getTokenVersion(token = keys(1)._1).get.versionsCount should be(10)
      getTokenVersion(token = keys(2)._1).get.versionsCount should be(100)
      getTokenVersion(token = keys(3)._1) match {
        // Validate that we don't get all versions in one call
        case Some(v) => {
          v.versionsCount should be(500)
          v.versions.size should be < v.versionsCount
        }
        case _ => fail("All versions loaded in one call. Need to be in multiple calls for this test to be useful.")
      }
      getTokenVersion(token = keys(4)._1).get.versionsCount should be(5)

      // Force collection once, should collect the versions of the first 3 tokens
      f.mysqlStorage.GarbageCollector.collectAll(100) should be > 0
      getTokenVersion(token = keys(0)._1) should be(None)
      getTokenVersion(token = keys(1)._1) should be(None)
      getTokenVersion(token = keys(2)._1) should be(None)
      getTokenVersion(token = keys(3)._1).get.versionsCount should be(500)
      getTokenVersion(token = keys(4)._1).get.versionsCount should be(5)

      // Force collection again, should collect most but not all versions of the 4th token
      f.mysqlStorage.GarbageCollector.collectAll(100) should be > 0
      getTokenVersion(token = keys(0)._1) should be(None)
      getTokenVersion(token = keys(1)._1) should be(None)
      getTokenVersion(token = keys(2)._1) should be(None)
      getTokenVersion(token = keys(3)._1).get.versionsCount should (be > 0 and be < 500)
      getTokenVersion(token = keys(4)._1).get.versionsCount should be(5)

      // This time all versions of the 4th token should have been collected
      f.mysqlStorage.GarbageCollector.collectAll(100) should be > 0
      getTokenVersion(token = keys(0)._1) should be(None)
      getTokenVersion(token = keys(1)._1) should be(None)
      getTokenVersion(token = keys(2)._1) should be(None)
      getTokenVersion(token = keys(3)._1) should be(None)
      getTokenVersion(token = keys(4)._1).get.versionsCount should be(5)

      def getTokenVersion(token: Long): Option[VersionRecord] = {
        var trx = f.mysqlStorage.createStorageTransaction
        val versions = trx.getTopMostVersions(table1, token, token, Long.MaxValue, 10)
        trx.rollback()

        versions.size should be <= 1
        versions.headOption
      }
    }
  }

  test("force garbage collections should delete all extra versions of a token with multiple access path") {
    withFixture { f =>
      val idGenerator = new TimestampIdGenerator()
      val context = new ExecutionContext(f.storages)
      val k1 = "key"
      val token = context.getToken(k1)

      // Create master table record
      f.exec(t => {
        t.from("mysql").from("table2").set(k1, Map("k1" -> k1))
      }, commit = true, onTimestamp = Timestamp(idGenerator.nextId))

      // Create secondary table records
      createSecondaryRecords(k2 = 0, ver = 5)
      createSecondaryRecords(k2 = 1, ver = 200)
      getSecondaryLoadedVersionCount(k2 = 1) should be < 200

      // GC first key and second key partially
      f.mysqlStorage.GarbageCollector.collectAll(20) should be > 0
      getSecondaryLoadedVersionCount(k2 = 0) should be(0)
      getSecondaryLoadedVersionCount(k2 = 1) should be > 0

      // Create new versions for first key and from now on limit GC to only one key at a time
      createSecondaryRecords(k2 = 0, ver = 5)
      getSecondaryLoadedVersionCount(k2 = 0) should be > 5
      f.mysqlStorage.GarbageCollector.collectAll(200, versionBatchSize = 1) should be > 0

      // Now GC everything remaining
      f.mysqlStorage.GarbageCollector.collectAll(200, versionBatchSize = 1) should be > 0
      f.mysqlStorage.GarbageCollector.collectAll(200, versionBatchSize = 1) should be > 0
      getSecondaryVersionCount should be(0)

      def createSecondaryRecords(k2: Int, ver: Int) {
        println("createRecords k2=%s".format(k2))
        for (i <- 0.until(ver)) {
          f.exec(t => {
            t.from("mysql").from("table2").get(k1).from("table2_1").set(k2.toString, Map("k1" -> k1, "k2" -> i.toString))
          }, commit = true, onTimestamp = Timestamp(idGenerator.nextId))
        }
      }

      def getSecondaryVersionCount: Int = {
        var trx = f.mysqlStorage.createStorageTransaction
        val versions = trx.getTopMostVersions(table2_1, token, token, Long.MaxValue, 1000)
        trx.rollback()

        versions.foldLeft(0)((count, version) => count + version.versionsCount)
      }

      def getSecondaryLoadedVersionCount(k2: Int): Int = {
        var trx = f.mysqlStorage.createStorageTransaction
        val versions = trx.getTopMostVersions(table2_1, token, token, Long.MaxValue, 1000)
        trx.rollback()

        val path = "%s/%d".format(k1, k2)
        versions.find(_.accessPath.toString == path).map(_.versions.size).getOrElse(0)
      }
    }
  }

  test("forced garbage collections with ranges") {
    withFixture { f =>
      val context = new ExecutionContext(f.storages)
      val keys = List.range(0, 20).map(i => {
        val key = "key%d".format(i)
        (context.getToken(key), key)
      }).sorted

      def createRecords(seed: Long, ver: Int = 5) {
        for (((token, key), i) <- keys.zipWithIndex) {
          Seq.range(0, ver).foreach(j => {
            f.exec(t => {
              val t1 = t.from("mysql").from("table1")
              t1.set(key, Map(key -> "%d_%d_%d".format(seed, i, j)))
            }, commit = true, onTimestamp = Timestamp(seed + i * ver + j))
          })
        }
      }

      val ranges = List(TokenRange(1000000001L, 2000000000L), TokenRange(3000000001L, 4000000000L))
      f.mysqlStorage.GarbageCollector.setCollectedRanges(ranges)

      // Create 5 versions for each token/keys tupple
      createRecords(0, 5)
      verifyRecordsCollectedForRanges(List())

      // Force collection once, should collect records of the first range
      f.mysqlStorage.GarbageCollector.collectAll(100) should be > 0
      verifyRecordsCollectedForRanges(ranges.slice(0, 1))

      // Force collection again, should collect records of the second and last range
      f.mysqlStorage.GarbageCollector.collectAll(100) should be > 0
      verifyRecordsCollectedForRanges(ranges)

      // Create 5 more versions for each token/keys tupple
      createRecords(5000, 5)
      verifyRecordsCollectedForRanges(List())

      // Force more collect. Collect should start over from the first range. Do a few small collects on the first range
      // and then the entire range
      f.mysqlStorage.GarbageCollector.collectAll(5) should be > 0
      f.mysqlStorage.GarbageCollector.collectAll(5) should be > 0
      f.mysqlStorage.GarbageCollector.collectAll(100) should be > 0
      verifyRecordsCollectedForRanges(ranges.slice(0, 1))

      // Yet another collect on the second and last range
      f.mysqlStorage.GarbageCollector.collectAll(100) should be > 0
      verifyRecordsCollectedForRanges(ranges)

      // Final collection when there are nothing to be collected
      f.mysqlStorage.GarbageCollector.collectAll(100) should be(0)

      // Just for fun, try with an empty range
      // Create 5 more versions for each token/keys tupple
      createRecords(10000, 5)
      verifyRecordsCollectedForRanges(List())
      f.mysqlStorage.GarbageCollector.setCollectedRanges(List())
      f.mysqlStorage.GarbageCollector.collectAll(100) should be(0)
      verifyRecordsCollectedForRanges(List())

      def verifyRecordsCollectedForRanges(collectedRanges: List[TokenRange]) {
        var trx = f.mysqlStorage.createStorageTransaction
        keys.foreach(entry => {
          val (t, k) = entry
          val versions = trx.getTopMostVersions(table1, t, t, Long.MaxValue, 10)

          if (collectedRanges.exists(_.contains(t))) {
            // tupple is in the collected range, no extra version should be available
            versions.size should be(0)
          } else {
            // not in collected range, some extra version should exists
            versions.size should be > 0
          }
        })
        trx.rollback()
      }
    }
  }

  test("forced garbage collections should ignore versions after current consistent timestamp") {
    withFixture { f =>
      f.exec(_.from("mysql").from("table1").set("k", Map("k" -> "1")), commit = true, onTimestamp = 100L)
      f.exec(_.from("mysql").from("table1").set("k", Map("k" -> "1")), commit = true, onTimestamp = 200L)
      f.exec(_.from("mysql").from("table1").set("k", Map("k" -> "1")), commit = true, onTimestamp = 300L)
      f.exec(_.from("mysql").from("table1").set("k", Map("k" -> "1")), commit = true, onTimestamp = 400L)
      f.exec(_.from("mysql").from("table1").set("k", Map("k" -> "1")), commit = true, onTimestamp = 500L)
      f.exec(_.from("mysql").from("table1").set("k", Map("k" -> "1")), commit = true, onTimestamp = 600L)

      // Force collection, no records collected because there are only 3 versions before the consistent timestamp
      currentConsistentTimestamp = 350L
      f.mysqlStorage.GarbageCollector.collectAll(100) should be(0)

      // Force collection again, all extra records should have been collected
      currentConsistentTimestamp = 600L
      f.mysqlStorage.GarbageCollector.collectAll(100) should be(3)
    }
  }

  test("forced garbage collections should ignore versions after open read transaction iterator") {
    withFixture { f =>
      f.exec(_.from("mysql").from("table1").set("k", Map("k" -> "1")), commit = true, onTimestamp = 100L)
      f.exec(_.from("mysql").from("table1").set("k", Map("k" -> "1")), commit = true, onTimestamp = 200L)
      f.exec(_.from("mysql").from("table1").set("k", Map("k" -> "1")), commit = true, onTimestamp = 300L)
      f.exec(_.from("mysql").from("table1").set("k", Map("k" -> "1")), commit = true, onTimestamp = 400L)
      f.exec(_.from("mysql").from("table1").set("k", Map("k" -> "1")), commit = true, onTimestamp = 500L)
      f.exec(_.from("mysql").from("table1").set("k", Map("k" -> "1")), commit = true, onTimestamp = 600L)

      // Force collection, versions from the open transactions iterator start timestamp are ignored
      val itr = f.mysqlStorage.readTransactions(400L, 600L, Seq(TokenRange.All))
      f.mysqlStorage.GarbageCollector.collectAll(100) should be(1)

      // Force collection again after closing the iterator, all remaining extra record should be collected
      itr.close()
      f.mysqlStorage.GarbageCollector.collectAll(100) should be(2)
    }
  }

  test("getAllLatest should return all latest elements") {
    withFixture { f =>
      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set("key1", Map("k1" -> "v1"))
        t1.set("key2", Map("k1" -> "v1"))
        t1.set("key3", Map("k1" -> "v1"))
      }, commit = true, onTimestamp = Timestamp(1))

      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set("key2", Map("k1" -> "v2"))
        t1.delete("key3")
      }, commit = true, onTimestamp = Timestamp(100))

      val context = new ExecutionContext(f.storages)
      val table1All = f.mysqlStorage.createStorageTransaction(context)
        .getAllLatest(table1, 100)

      assert(table1All.next() === true)
      val recordKey1 = table1All.record
      assert(recordKey1.value.asInstanceOf[MapValue]("k1").toString === "v1")

      assert(table1All.next() === true)
      val recordKey2 = table1All.record
      assert(recordKey2.value.asInstanceOf[MapValue]("k1").toString === "v2")

      assert(table1All.next() === false)

      table1All.close()
    }
  }

  test("getAllLatest should return all latest elements on multi-hierarchical table") {
    withFixture { f =>
      val keys = Seq("key1", "key2", "key3")

      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        keys foreach (t1.set(_, Map("k1" -> "v1")))
      }, commit = true, onTimestamp = Timestamp(0))

      var currentTime = 100
      keys foreach (key => {
        f.exec(t => {
          val t11 = t.from("mysql").from("table1").get(key).from("table1_1")
          t11.set("key1", Map("k1" -> "v1"))
          t11.set("key2", Map("k1" -> "v1"))
          t11.set("key3", Map("k1" -> "v1"))
        }, commit = true, onTimestamp = Timestamp(currentTime))

        currentTime += 10
        f.exec(t => {
          val t11 = t.from("mysql").from("table1").get(key).from("table1_1")
          t11.set("key2", Map("k1" -> "v2"))
          t11.delete("key3")
        }, commit = true, onTimestamp = Timestamp(currentTime))
        currentTime += 10
      })

      f.exec(t => t.from("mysql").from("table1").delete("key3"), commit = true, onTimestamp = Timestamp(currentTime))

      val context = new ExecutionContext(f.storages)
      val table11All = f.mysqlStorage.createStorageTransaction(context)
        .getAllLatest(table1_1, 100)

      assert(table11All.next() === true)
      val recordKey1 = table11All.record
      assert(recordKey1.value.asInstanceOf[MapValue]("k1").toString === "v1")

      assert(table11All.next() === true)
      val recordKey2 = table11All.record
      assert(recordKey2.value.asInstanceOf[MapValue]("k1").toString === "v2")

      assert(table11All.next() === true)
      val recordKey3 = table11All.record
      assert(recordKey3.value.asInstanceOf[MapValue]("k1").toString === "v1")

      assert(table11All.next() === true)
      val recordKey4 = table11All.record
      assert(recordKey4.value.asInstanceOf[MapValue]("k1").toString === "v2")

      assert(table11All.next() === false)

      table11All.close()
    }
  }

  test("genWhereHigherEqualTuple should generate a where string") {
    withFixture { f =>
      val context = new ExecutionContext(f.storages)
      val transaction = f.mysqlStorage.createStorageTransaction(context)

      var rep = transaction.genWhereHigherEqualTuple(Map("a" -> 1))
      rep._1 should be("((a >= ?))")
      rep._2 should be(Seq(1))


      rep = transaction.genWhereHigherEqualTuple(Map("a" -> 1, "b" -> 2, "c" -> 3))
      rep._1 should be("((a > ?)) OR ((a = ?) AND (b > ?)) OR ((a = ?) AND (b = ?) AND (c >= ?))")
      rep._2 should be(Seq(1, 1, 2, 1, 2, 3))
    }
  }

  test("getAllLatest should support fromRecord") {
    withFixture { f =>
      val context = new ExecutionContext(f.storages)
      val keys = Seq.range(0, 40).map(i => {
        val key = "key%d".format(i)
        (context.getToken(key), key)
      }).sorted

      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        keys foreach (tup => t1.set(tup._2, Map(tup._2 -> tup._2)))
      }, commit = true, onTimestamp = Timestamp(0))

      var records = f.mysqlStorage.createStorageTransaction(context).getAllLatest(table1, 20).toList
      var recordsKey = records.map(_.accessPath(0).key)
      recordsKey should be(keys.map(_._2).slice(0, 20).toList)

      records = f.mysqlStorage.createStorageTransaction(context).getAllLatest(table1, 20, optFromRecord = Some(records.last)).toList
      recordsKey = records.map(_.accessPath(0).key)
      recordsKey should be(keys.map(_._2).slice(19, 39).toList)
    }
  }

  test("getAllLatest should support deleted records") {
    withFixture { f =>
      val context = new ExecutionContext(f.storages)
      val keys = Seq.range(0, 40).map(i => {
        val key = "key%d".format(i)
        (context.getToken(key), key)
      }).sorted

      val deletedKeys = keys.filter(_._1 % 2 == 0)
      val createdKeys = keys.filter(_._1 % 2 != 0)

      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        deletedKeys.foreach(tup => t1.delete(tup._2))
        createdKeys.foreach(tup => t1.set(tup._2, Map(tup._2 -> tup._2)))
      }, commit = true, onTimestamp = Timestamp(0))

      var recordsExcludeDeleted = f.mysqlStorage.createStorageTransaction(context).getAllLatest(table1, 50).toList
      recordsExcludeDeleted.size should be(createdKeys.size)

      var recordsIncludeDeleted = f.mysqlStorage.createStorageTransaction(context).getAllLatest(table1, 50, includeDeleted = true).toList
      recordsIncludeDeleted.size should be(keys.size)

      val recordsDeleted = recordsIncludeDeleted.filter(_.value.isNull)
      recordsDeleted.size should be(deletedKeys.size)
    }
  }

  test("getAllLatest with token range") {
    withFixture { f =>
      val context = new ExecutionContext(f.storages)
      val keys = Seq.range(0, 40).map(i => {
        val key = "key%d".format(i)
        (context.getToken(key), key)
      }).sorted

      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        keys foreach (tup => t1.set(tup._2, Map(tup._2 -> tup._2)))
      }, commit = true, onTimestamp = Timestamp(0))

      val ranges = List(TokenRange(0, 1333333333L), TokenRange(1333333334L, 2666666666L),
        TokenRange(2666666667L, TokenRange.MaxToken))

      var allRecordKeys: List[String] = List()
      for (range <- ranges) {
        val records = f.mysqlStorage.createStorageTransaction(context).getAllLatest(table1, 40, range).toList
        val recordsKey = records.map(_.accessPath(0).key)
        recordsKey should be(keys.filter(key => range.contains(key._1)).map(_._2).toList)
        allRecordKeys ++= recordsKey
      }

      allRecordKeys.length should be(40)
    }
  }

  test("getAllLatest with token range and fromToken") {
    withFixture { f =>
      val context = new ExecutionContext(f.storages)
      val keys = Seq.range(0, 40).map(i => {
        val key = "key%d".format(i)
        (context.getToken(key), key)
      }).sorted

      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        keys foreach (tup => t1.set(tup._2, Map(tup._2 -> tup._2)))
      }, commit = true, onTimestamp = Timestamp(0))

      val ranges = List(TokenRange(0, 1333333333L), TokenRange(1333333334L, 2666666666L),
        TokenRange(2666666667L, TokenRange.MaxToken))

      for (range <- ranges) {
        var records = f.mysqlStorage.createStorageTransaction(context).getAllLatest(table1, 40, range).toList
        var recordsKey = records.map(_.accessPath(0).key)
        recordsKey should be(keys.filter(key => range.contains(key._1)).map(_._2).toList)

        val middleIndex = records.size / 2
        records = f.mysqlStorage.createStorageTransaction(context).getAllLatest(table1, 40, range, Some(records(middleIndex))).toList
        recordsKey = records.map(_.accessPath(0).key)
        recordsKey should be(keys.filter(key => range.contains(key._1)).map(_._2).slice(middleIndex, 40).toList)
      }
    }
  }

  test("should truncate all tables at the given timestamp") {
    withFixture { f =>
      f.exec(t => {
        val storage = t.from("mysql")
        val table = storage.from("table1")
        table.set("key1", Map("k" -> "value1"))
        table.set("key3", Map("k" -> "value3"))
        table.get("key3").from("table1_1").set("key3.1", Map("k" -> "value3.1"))
      }, commit = true, onTimestamp = Timestamp(10))

      f.exec(t => {
        val storage = t.from("mysql")
        val table = storage.from("table1")
        table.set("key1", Map("k" -> "value1v2"))
        table.set("key2", Map("k" -> "value2"))
        table.set("key3", Map("k" -> "value3v2"))
        table.get("key3").from("table1_1").set("key3.1", Map("k" -> "value3.1v2"))
        table.get("key3").from("table1_1").get("key3.1").from("table1_1_1").set("key3.1.1", Map("k" -> "value3.1.1v2"))
      }, commit = true, onTimestamp = Timestamp(20))

      def getValues: Seq[Value] = {
        f.exec(t => {
          val storage = t.from("mysql")
          val var1 = storage.from("table1").get("key1")
          val var2 = storage.from("table1").get("key2")
          val var3 = storage.from("table1").get("key3")
          val var3_1 = storage.from("table1").get("key3").from("table1_1").get("key3.1")
          val var3_1_1 = storage.from("table1").get("key3").from("table1_1").get("key3.1").from("table1_1_1").get("key3.1.1")

          t.ret(var1, var2, var3, var3_1, var3_1_1)
        })
      }

      val before = getValues
      before(0) should be(MapValue(Map("k" -> "value1v2")))
      before(1) should be(MapValue(Map("k" -> "value2")))
      before(2) should be(MapValue(Map("k" -> "value3v2")))
      before(3) should be(MapValue(Map("k" -> "value3.1v2")))
      before(4) should be(MapValue(Map("k" -> "value3.1.1v2")))

      val context = new ExecutionContext(f.storages)
      f.mysqlStorage.truncateAt(Timestamp(20), context.getToken("key3"))

      val after = getValues // key1 and key2 are not truncated because of different token
      after(0) should be(MapValue(Map("k" -> "value1v2")))
      after(1) should be(MapValue(Map("k" -> "value2")))
      after(2) should be(MapValue(Map("k" -> "value3")))
      after(3) should be(MapValue(Map("k" -> "value3.1")))
      after(4) should be(NullValue)
    }
  }

  test("getLastTimestamp should return max range timestamp") {
    withFixture { f =>
      val context = new ExecutionContext(f.storages)
      case class Data(key: String, table: String, timestamp: Long = Random.nextLong()) {
        val token = context.getToken(key)
      }

      // Generate test data in two tables: table1 and table2
      val data = 0.until(40).map(i => Data("key%d".format(i), (if (i % 2 == 0) "table1" else "table2")))
      data.foreach(d => {
        f.exec(t => {
          val storage = t.from("mysql")
          val table = storage.from(d.table)
          table.set(d.key, Map("data" -> d.token.toString))
        }, commit = true, onTimestamp = Timestamp(d.timestamp))
      })

      val range1 = TokenRange(0, 1333333333L)
      val range2 = TokenRange(1333333334L, 2666666666L)
      val range3 = TokenRange(2666666667L, TokenRange.MaxToken)

      val maxRange1 = data.filter(d => range1.contains(d.token)).map(_.timestamp).max
      val maxRange2 = data.filter(d => range2.contains(d.token)).map(_.timestamp).max
      val maxRange3 = data.filter(d => range3.contains(d.token)).map(_.timestamp).max
      val maxRange1and3 = math.max(maxRange1, maxRange3)

      f.mysqlStorage.getLastTimestamp(Seq(range1)) should be(Some(Timestamp(maxRange1)))
      f.mysqlStorage.getLastTimestamp(Seq(range2)) should be(Some(Timestamp(maxRange2)))
      f.mysqlStorage.getLastTimestamp(Seq(range3)) should be(Some(Timestamp(maxRange3)))
      f.mysqlStorage.getLastTimestamp(Seq(range1, range3)) should be(Some(Timestamp(maxRange1and3)))

      // Empty ranges should return None
      f.mysqlStorage.getLastTimestamp(Seq(TokenRange(0, 1))) should be(None)
    }
  }

  test("mutation group iterator returns expected groups") {
    withFixture { f =>
      val spyStorage = spy(f.mysqlStorage)
      val context = new ExecutionContext(f.storages)

      f.exec(trx => {
        val storage = trx.from("mysql")
        storage.from("table1").set("k1", Map("k" -> "1"))
      }, commit = true, onTimestamp = 1L)
      val tk1: Long = context.getToken("k1")
      val grp1 = spyStorage.MutationGroup(tk1, 1L, List(Record(table1, tk1, 1L, Map("k" -> "1"), "k1")))

      f.exec(trx => {
        val storage = trx.from("mysql")
        storage.from("table1").set("k2", Map("k" -> "2"))
        storage.from("table2").set("k2", Map("k" -> "2"))
      }, commit = true, onTimestamp = 2L)
      val tk2: Long = context.getToken("k2")
      val grp2 = spyStorage.MutationGroup(tk2, 2L, List(
        Record(table1, tk2, 2L, Map("k" -> "2"), "k2"), Record(table2, tk2, 2L, Map("k" -> "2"), "k2")))

      f.exec(trx => {
        val storage = trx.from("mysql")
        storage.from("table1").set("k3", Map("k" -> "3"))
        storage.from("table1").get("k3").from("table1_1").set("k3.1", Map("k" -> "3.1"))
      }, commit = true, onTimestamp = 3L)
      val tk3: Long = context.getToken("k3")
      val grp3 = spyStorage.MutationGroup(tk3, 3L, List(
        Record(table1, tk3, 3L, Map("k" -> "2"), "k3"), Record(table1_1, tk3, 3L, Map("k" -> "3.1"), "k3", "k3.1")))

      f.exec(trx => {
        val storage = trx.from("mysql")
        storage.from("table1").set("k4", Map("k" -> "4"))
        storage.from("table2").set("k4", Map("k" -> "4"))
      }, commit = true, onTimestamp = 4L)
      val tk4: Long = context.getToken("k4")
      val grp4 = spyStorage.MutationGroup(tk4, 4L, List(
        Record(table1, tk4, 4L, Map("k" -> "4"), "k4"), Record(table2, tk4, 4L, Map("k" -> "4"), "k4")))

      f.exec(trx => {
        val storage = trx.from("mysql")
        storage.from("table1").set("k5", Map("k" -> "5"))
      }, commit = true, onTimestamp = 5L)
      val tk5: Long = context.getToken("k5")
      val grp5 = spyStorage.MutationGroup(tk5, 5L, List(Record(table1, tk5, 5L, Map("k" -> "5"), "k5")))

      f.exec(trx => {
        val storage = trx.from("mysql")
        storage.from("table1").get("k1").from("table1_1").set("k1.1", Map("k" -> "1.1"))
        storage.from("table1").get("k1").from("table1_1").set("k1.2", Map("k" -> "1.2"))
        storage.from("table1").get("k1").from("table1_1").set("k1.3", Map("k" -> "1.3"))
        storage.from("table1").get("k1").from("table1_1").set("k1.4", Map("k" -> "1.4"))
        storage.from("table1").get("k1").from("table1_1").set("k1.5", Map("k" -> "1.5"))
      }, commit = true, onTimestamp = 6L)
      val tk6: Long = context.getToken("k1")
      val grp6 = spyStorage.MutationGroup(tk6, 6L, List(
        Record(table1_1, tk6, 6L, Map("k" -> "1.5"), "k1", "k1.5"), Record(table1_1, tk6, 6L, Map("k" -> "1.4"), "k1", "k1.4"),
        Record(table1_1, tk6, 6L, Map("k" -> "1.3"), "k1", "k1.3"), Record(table1_1, tk6, 6L, Map("k" -> "1.2"), "k1", "k1.2"),
        Record(table1_1, tk6, 6L, Map("k" -> "1.1"), "k1", "k1.1")))

      f.exec(trx => {
        val storage = trx.from("mysql")
        storage.from("table1").set("k7", Map("k" -> "7"))
      }, commit = true, onTimestamp = 7L)
      val tk7: Long = context.getToken("k7")
      val grp7 = spyStorage.MutationGroup(tk7, 7L, List(Record(table1, tk7, 7L, Map("k" -> "7"), "k7")))

      f.exec(trx => {
        val storage = trx.from("mysql")
        storage.from("table1").delete("k8")
      }, commit = true, onTimestamp = 8L)
      val tk8: Long = context.getToken("k8")
      val grp8 = spyStorage.MutationGroup(tk8, 8L, List(Record(table1, tk8, 8L, NullValue, "k8")))

      // After first and before last
      val grp2to7 = new spyStorage.MutationGroupIterator(startTimestamp = 2L, endTimestamp = 7L, List(TokenRange.All), 4, 1, 1, 1)
      grp2to7.toList should be(List(grp2, grp3, grp4, grp5, grp6, grp7))

      // Same from and to
      val grp1to1 = new spyStorage.MutationGroupIterator(startTimestamp = 1L, endTimestamp = 1L, List(TokenRange.All), 4, 2, 1, 1)
      grp1to1.toList should be(List(grp1))

      // Before first
      val grp0to3 = new spyStorage.MutationGroupIterator(startTimestamp = 0L, endTimestamp = 3L, List(TokenRange.All), 4, 2, 1, 1)
      grp0to3.toList should be(List(grp1, grp2, grp3))

      // Beyond last
      val grp5to9 = new spyStorage.MutationGroupIterator(startTimestamp = 5L, endTimestamp = 9L, List(TokenRange.All), 4, 2, 1, 1)
      grp5to9.toList should be(List(grp5, grp6, grp7, grp8))

      // Loading records error
      val grpError = new spyStorage.MutationGroupIterator(startTimestamp = 1L, endTimestamp = 3L, List(TokenRange.All), 4, 2, 1, 1)
      when(spyStorage.createStorageTransaction).thenThrow(new RuntimeException())
      evaluating {
        grpError.next()
      } should produce[RuntimeException]
      reset(spyStorage) // Should fail forever even if no more errors is triggered by spyed storage
      evaluating {
        grpError.next()
      } should produce[RuntimeException]
      evaluating {
        grpError.next()
      } should produce[RuntimeException]
    }
  }

  test("applying mutation group to a transaction should produce the right operations") {
    withFixture { f =>
      val storage = f.mysqlStorage
      val context = new ExecutionContext(f.storages)

      // Setup first mutation group
      val tk1 = context.getToken("k1")
      val ts1 = Timestamp(1)
      val table1_r1 = Record(table1, tk1, ts1, Map("k" -> "1"), "k1")
      val table1_1_r1 = Record(table1_1, tk1, ts1, Map("k" -> "1.1"), "k1", "k1.1")
      val table1_1_1_r1 = Record(table1_1_1, tk1, ts1, Map("k" -> "1.1.1"), "k1", "k1.1", "k1.1.1")
      val table1_1_r2 = Record(table1_1, tk1, ts1, Map("k" -> "1.2"), "k1", "k1.2")
      val grp1 = storage.MutationGroup(tk1, ts1, List(table1_r1, table1_1_r2, table1_1_r1, table1_1_1_r1))

      // Verify database is empty before
      val before = new storage.MutationGroupIterator(startTimestamp = 0L, endTimestamp = 3L, ranges = List(TokenRange.All))
      before.toList should be(List())

      // Generate mutations transaction and apply the operations
      f.exec(trx => {
        // Reverse the expected order of the records. This test that operations are properly resequenced.
        val reverse = storage.MutationGroup(grp1.token, grp1.timestamp, grp1.records.reverse)
        reverse.applyTo(trx)
      }, commit = true, onTimestamp = ts1)

      // Ensure mutations are applied to database
      val after = new storage.MutationGroupIterator(startTimestamp = 0L, endTimestamp = 3L, ranges = List(TokenRange.All)).toList
      //TODO find root cause... (Order is not important for this)
      after.foreach(mutation => mutation.records = mutation.records.sortWith((r1, r2) => r1.table.path.toString() < r2.table.path.toString()))
      after should be(List(grp1))

      // Create and apply a mutation group which deletes the intermediate record (i.e. with a parent and a child)
      val ts2 = Timestamp(2)
      val grp2 = storage.MutationGroup(tk1, ts2, List(
        Record(table1_1_r1.table, table1_1_r1.token, ts2, NullValue, table1_1_r1.accessPath.keys: _*),
        Record(table1_1_1_r1.table, table1_1_1_r1.token, ts2, NullValue, table1_1_1_r1.accessPath.keys: _*))
      )
      f.exec(grp2.applyTo(_), commit = true, onTimestamp = ts2)

      // Ensure mutations beeing applied
      val Seq(v1, v1_1, v1_2) = f.exec(t => {
        val storage = t.from("mysql")
        val var1 = storage.from("table1").get("k1")
        val var1_1 = storage.from("table1").get("k1").from("table1_1").get("k1.1")
        val var1_2 = storage.from("table1").get("k1").from("table1_1").get("k1.2")

        t.ret(var1, var1_1, var1_2)
      })
      v1 should be(table1_r1.value)
      v1_1 should be(NullValue) // Deleted
      v1_2 should be(table1_1_r2.value)
    }
  }

  test("should get tombstone records") {
    withFixture { f =>
      f.exec(t => {
        val storage = t.from("mysql")
        val table = storage.from("table1")
        table.set("key1", Map("k" -> "value1"))
        table.set("key2", Map("k" -> "value2"))
        table.set("key3", Map("k" -> "value3"))
        table.get("key1").from("table1_1").set("key1.1", Map("k" -> "value1.1"))
        table.get("key2").from("table1_1").set("key2.1a", Map("k" -> "value2.1a"))
        table.get("key2").from("table1_1").set("key2.1b", Map("k" -> "value2.1b"))
        table.get("key2").from("table1_1").set("key2.1c", Map("k" -> "value2.1c"))
        table.get("key2").from("table1_1").set("key2.1d", Map("k" -> "value2.1d"))
        table.get("key3").from("table1_1").set("key3.1", Map("k" -> "value3.1"))
      }, commit = true, onTimestamp = Timestamp(100))

      f.exec(t => {
        val storage = t.from("mysql")
        val table = storage.from("table1")
        table.delete("key2")
      }, commit = true, onTimestamp = Timestamp(200))

      f.exec(t => {
        val storage = t.from("mysql")
        val table = storage.from("table1")
        table.delete("key3")
      }, commit = true, onTimestamp = Timestamp(300))

      val context = new ExecutionContext(f.storages)
      f.mysqlStorage.setCurrentConsistentTimestamp((range) => Timestamp(300))

      val all = f.mysqlStorage.createStorageTransaction(context).getTombstoneRecords(table1_1, count = 10,
        TokenRange.All, minTombstoneAge = 0, None)
      all.size should be(5)
      all.map(_.accessPath.keys.last).toSet should be(Set("key2.1a", "key2.1b", "key2.1c", "key2.1d", "key3.1"))

      val key2All = f.mysqlStorage.createStorageTransaction(context).getTombstoneRecords(table1_1, count = 10,
        TokenRange.All, minTombstoneAge = 100, None)
      key2All.size should be(4)
      key2All.map(_.accessPath.keys.last) should be(Seq("key2.1a", "key2.1b", "key2.1c", "key2.1d"))

      val key2First2 = f.mysqlStorage.createStorageTransaction(context).getTombstoneRecords(table1_1, count = 2,
        TokenRange.All, minTombstoneAge = 100, None)
      key2First2.size should be(2)
      key2First2.map(_.accessPath.keys.last) should be(Seq("key2.1a", "key2.1b"))

      val key2Reminder = f.mysqlStorage.createStorageTransaction(context).getTombstoneRecords(table1_1, count = 10,
        TokenRange.All, minTombstoneAge = 100, Some(key2First2.last))
      key2Reminder.size should be(3)
      key2Reminder.map(_.accessPath.keys.last) should be(Seq("key2.1b", "key2.1c", "key2.1d"))
    }
  }

  test("should delete tombstones and ancestors records") {
    withFixture { f =>
      f.exec(t => {
        val storage = t.from("mysql")
        val table = storage.from("table1")
        table.set("key1", Map("k" -> "value1"))
        table.set("key2", Map("k" -> "value2"))
        table.set("key3", Map("k" -> "value3"))
        table.get("key1").from("table1_1").set("key1.1", Map("k" -> "value1.1"))
        table.get("key2").from("table1_1").set("key2.1a", Map("k" -> "value2.1a"))
        table.get("key2").from("table1_1").set("key2.1b", Map("k" -> "value2.1b"))
        table.get("key2").from("table1_1").set("key2.1c", Map("k" -> "value2.1c"))
        table.get("key2").from("table1_1").set("key2.1d", Map("k" -> "value2.1d"))
        table.get("key3").from("table1_1").set("key3.1", Map("k" -> "value3.1"))
      }, commit = true, onTimestamp = Timestamp(100))

      f.exec(t => {
        val storage = t.from("mysql")
        val table = storage.from("table1")
        table.get("key2").from("table1_1").delete("key2.1a")
        table.get("key2").from("table1_1").delete("key2.1b")
        table.get("key2").from("table1_1").delete("key2.1c")
        table.get("key2").from("table1_1").delete("key2.1d")
      }, commit = true, onTimestamp = Timestamp(200))

      f.exec(t => {
        val storage = t.from("mysql")
        val table = storage.from("table1")
        table.get("key3").from("table1_1").delete("key3.1")
      }, commit = true, onTimestamp = Timestamp(300))

      f.exec(t => {
        val storage = t.from("mysql")
        val table = storage.from("table1")
        table.get("key3").from("table1_1").set("key3.1", Map("k" -> "value3.1bis"))
      }, commit = true, onTimestamp = Timestamp(400))

      val context = new ExecutionContext(f.storages)
      f.mysqlStorage.setCurrentConsistentTimestamp((range) => Timestamp(400))
      val allBefore = f.mysqlStorage.createStorageTransaction(context).getTombstoneRecords(table1_1, count = 10,
        TokenRange.All, minTombstoneAge = 0, None)
      allBefore.size should be(5)
      allBefore.map(_.accessPath.keys.last).toSet should be(Set("key2.1a", "key2.1b", "key2.1c", "key2.1d", "key3.1"))

      val key2All = f.mysqlStorage.createStorageTransaction(context).getTombstoneRecords(table1_1, count = 10,
        TokenRange.All, minTombstoneAge = 200, None)
      key2All.size should be(4)
      key2All.map(_.accessPath.keys.last) should be(Seq("key2.1a", "key2.1b", "key2.1c", "key2.1d"))

      // Delete first key2 tombstone
    {
      val transaction = f.mysqlStorage.createStorageTransaction(context)
      transaction.deleteTombstoneAndOlder(key2All.head)
      transaction.commit()
    }

      val key2AfterHeadDelete = f.mysqlStorage.createStorageTransaction(context).getTombstoneRecords(table1_1, count = 10,
        TokenRange.All, minTombstoneAge = 200, None)
      key2AfterHeadDelete.size should be(3)
      key2AfterHeadDelete.map(_.accessPath.keys.last) should be(Seq("key2.1b", "key2.1c", "key2.1d"))

      // Delete all tombstones
      allBefore.foreach {
        record =>
          val transaction = f.mysqlStorage.createStorageTransaction(context)
          transaction.deleteTombstoneAndOlder(record)
          transaction.commit()
      }

      val allAfter = f.mysqlStorage.createStorageTransaction(context).getTombstoneRecords(table1_1, count = 10,
        TokenRange.All, minTombstoneAge = 0, None)
      allAfter.size should be(0)

      val Seq(value3) = f.exec(t => {
        val storage = t.from("mysql")
        val table = storage.from("table1")
        val value3 = table.get("key3").from("table1_1").get("key3.1")
        t.returns(value3)
      }, commit = true)
      assert(value3.asInstanceOf[MapValue]("key3.1").equalsValue("value3.1bis"))
    }
  }

  test("test composite key") {
    CompositeKey("abc", "0", "1", "2").compareTo(CompositeKey("abc", "0", "1", "2")) should be(0)
    CompositeKey("abc", "0", "1", "2").compareTo(CompositeKey("abc", "0", "1", "1")) should be > 0
    CompositeKey("abc", "0", "1", "2").compareTo(CompositeKey("abc", "0", "1")) should be > 0
    CompositeKey("abc", "0", "1", "2").compareTo(CompositeKey("abc", "0", "1", "3")) should be < 0
    CompositeKey("abc", "0", "1", "2").compareTo(CompositeKey("abc", "0", "1", "2", "3")) should be < 0
    CompositeKey("abc", "0", "1", "2").compareTo(CompositeKey("abc", "0", "1", "1", "1")) should be > 0

    CompositeKey("abc", "0", "1", "2", "3").head should be(CompositeKey("abc"))
    CompositeKey("abc", "0", "1", "2", "3").parent should be(CompositeKey("abc", "0", "1", "2"))
    CompositeKey("abc", "0").ancestors should be(List(CompositeKey("abc")))
    CompositeKey("abc").ancestors should be(Nil)
    CompositeKey(Seq[String]():_*).ancestors should be(Nil)

    CompositeKey(table1_1_1, "0", "1", "2").head should be(CompositeKey(table1, "0"))
    CompositeKey(table1_1_1, "0", "1", "2").parent should be(CompositeKey(table1_1, "0", "1"))
    CompositeKey(table1_1_1, "0", "1", "2").ancestors should be(List(CompositeKey(table1_1, "0", "1"), CompositeKey(table1, "0")))
    CompositeKey(table1, "0").ancestors should be(Nil)
  }

  test("test composite key subset") {
    import collection.JavaConversions._
    import java.util

    val set = new util.TreeSet(
      List(CompositeKey(table2_1, "2", "1"), CompositeKey(table1, "9"),
        CompositeKey(table1, "0"), CompositeKey(table1_1, "0", "0")))
    set.toList should be(List(
      CompositeKey(table1, "0"), CompositeKey(table1_1, "0", "0"),
      CompositeKey(table1, "9"), CompositeKey(table2_1, "2", "1")))

    val k1 = CompositeKey(table1_1_1, "0", "1", "2")
    set.subSet(k1.head, true, k1.parent, true).toList should be(List(CompositeKey(table1, "0"), CompositeKey(table1_1, "0", "0")))

    val k2 = CompositeKey(table2_1_1, "2", "1", "0")
    set.subSet(k2.head, true, k2.parent, true).toList should be(List(CompositeKey(table2_1, "2", "1")))

    set.subSet(CompositeKey(table1_1, "3", "5"), true, CompositeKey(table1_1, "5", "3"), true).toList should be(Nil)
  }
}