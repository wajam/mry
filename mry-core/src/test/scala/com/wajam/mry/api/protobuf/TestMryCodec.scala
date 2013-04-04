package com.wajam.mry.api.protobuf

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import com.wajam.mry.api.MryCodec
import com.wajam.mry.execution._
import com.wajam.mry.execution.Implicits._

@RunWith(classOf[JUnitRunner])
class TestMryCodec extends FunSuite with BeforeAndAfter with ShouldMatchers {

  test("can encode null") {

    val n = null

    val codec = new MryCodec()

    val bytes = codec.encode(n)
    val n2 = codec.decode(bytes)

    n2 should equal(null)
  }

  test("can encode transaction") {

    val t = new Transaction((b) => b.returns(b.from("B").get(1000)))

    val codec = new MryCodec()

    val bytes = codec.encode(t)
    val t2 = codec.decode(bytes)

    t.equalsContent(t2) should equal (true)
  }

  test("can encode seq[Values]") {

    val s = Seq(new IntValue(5))

    val codec = new MryCodec()

    val bytes = codec.encode(s)
    val s2 = codec.decode(bytes)

    s should be (s2)
  }

}
