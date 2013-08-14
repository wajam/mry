package com.wajam.mry.entity

import com.wajam.mry.execution.{Value, ListValue, StringValue, MapValue}
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestEntity extends FunSuite with ShouldMatchers {

  import com.wajam.mry.entity.TestEntity.TestObject

  test("No opt fields set") {
    val fromModel = new TestObject {
      id.set(3463746)
      url.set("http://www.something.com")
      title.set("title")
    }

    val toModel = (new TestObject).fromMry(fromModel.toMry)
    val map = toModel.toMry.asInstanceOf[MapValue]
    map.mapValue.keys should not contain("geo")
    map.mapValue.keys should not contain("description")

    map.mapValue("tags").asInstanceOf[ListValue].listValue should be ('empty)
  }

  test("geo set") {
    val fromModel = new TestObject {
      id.set(3463746)
      url.set("http://www.something.com")
      title.set("title")

      geo.define {
        geo.latitude.set(44.1)
        geo.longitude.set(23.11)
      }
    }

    val toModel = (new TestObject).fromMry(fromModel.toMry)
    val map = toModel.toMry.asInstanceOf[MapValue]
    map.mapValue.keys should contain("geo")
    map.mapValue.keys should not contain("description")

    val geoMap = map.mapValue("geo").asInstanceOf[MapValue]
    geoMap.mapValue.keys should contain("latitude")
    geoMap.mapValue.keys should contain("longitude")

    map.mapValue("tags").asInstanceOf[ListValue].listValue should be ('empty)
  }

  test("description set") {
    val fromModel = new TestObject {
      id.set(3463746)
      url.set("http://www.something.com")
      title.set("title")

      description.set(Some("test"))

    }

    val toModel = (new TestObject).fromMry(fromModel.toMry)
    val map = toModel.toMry.asInstanceOf[MapValue]
    map.mapValue.keys should not contain("geo")
    map.mapValue.keys should contain("description")

    val StringValue(description) = map.mapValue("description")
    description should equal("test")

    map.mapValue("tags").asInstanceOf[ListValue].listValue should be ('empty)
  }

  test("tags set") {
    val fromModel = new TestObject {
      id.set(3463746)
      url.set("http://www.something.com")
      title.set("title")

      tags.set("a" :: "b" :: Nil)
    }

    val toModel = (new TestObject).fromMry(fromModel.toMry)
    val map = toModel.toMry.asInstanceOf[MapValue]
    map.mapValue.keys should not contain("geo")
    map.mapValue.keys should not contain("description")


    map.mapValue("tags").asInstanceOf[ListValue].listValue.size should equal(2)
  }

  test("parse MapValue") {
    import com.wajam.mry.execution.Implicits._
    val map = MapValue(Map[String, Value](
      "id" -> 1L,
      "title" -> "asdf",
      "url" -> "asdf",
      "description" -> "Some description",
      "tags" -> Seq("asd","fgh"),
      "geo" -> MapValue(Map[String, Value](
        "latitude" -> 45.5175d,
        "longitude" -> -73.5805d
      ))
    ))

    val fromMry = new TestObject().fromMry(map)
    fromMry.id.get should equal(1L)
    fromMry.title.get should equal("asdf")
    fromMry.url.get should equal("asdf")
    fromMry.description.get should be('defined)
    fromMry.description.get.get should equal("Some description")
    fromMry.tags.list should equal(Seq("asd","fgh"))

    val geoMap = fromMry.toMry.asInstanceOf[MapValue].mapValue("geo").asInstanceOf[MapValue]
    geoMap.mapValue.keys should contain("latitude")
    geoMap.mapValue.keys should contain("longitude")
  }
}

object TestEntity {
  class TestObject extends Entity {
    val modelName = "FolloweeObject"

    val id = Field[Long](this, "id")

    val url = Field[String](this, "url")
    val title = Field[String](this, "title")
    val description = Field[Option[String]](this, "description")
    val tags = ListField[String](this, "tags")

    val geo = new OptionalFieldsGroup(this, "geo") {
      val latitude = Field[Double](this, "latitude")
      val longitude = Field[Double](this, "longitude")
    }

    def key = id.get.toString

  }
}
