package com.wajam.mry.api.protobuf

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import com.wajam.mry.api.HybridCodec
import com.wajam.mry.execution._
import com.wajam.mry.execution.Implicits._

@RunWith(classOf[JUnitRunner])
class TestHybridCodec extends FunSuite with BeforeAndAfter with ShouldMatchers {

  private def compareTwoProtocols(mA: HybridCodec.TranslationMode.Value, mB: HybridCodec.TranslationMode.Value) = {

    val t = new Transaction((b) => b.returns(b.from("B").get(1000)))

    val codecA = new HybridCodec(mA)

    val codecB = new HybridCodec(mB)

    val bytes = codecA.encode(t)
    val t2 = codecB.decode(bytes)

    assert(t.equalsContent(t2))
  }

  test("can encode using mry and can decode using both") {
    compareTwoProtocols(HybridCodec.TranslationMode.BothThenMry, HybridCodec.TranslationMode.BothThenMry)
    compareTwoProtocols(HybridCodec.TranslationMode.BothThenMry, HybridCodec.TranslationMode.BothThenJava)
  }

  test("can encode using java and can decode using both") {
    compareTwoProtocols(HybridCodec.TranslationMode.BothThenJava, HybridCodec.TranslationMode.BothThenMry)
    compareTwoProtocols(HybridCodec.TranslationMode.BothThenJava, HybridCodec.TranslationMode.BothThenJava)
  }

  test("can't encode using mry and can decode using java") {

    intercept[java.io.StreamCorruptedException] {
     compareTwoProtocols(HybridCodec.TranslationMode.MryThenMry, HybridCodec.TranslationMode.JavaThenJava)
    }

    intercept[java.io.StreamCorruptedException] {
      compareTwoProtocols(HybridCodec.TranslationMode.BothThenMry, HybridCodec.TranslationMode.JavaThenJava)
    }
  }

  test("can't encode using java and can decode using mry") {
    intercept[com.google.protobuf.InvalidProtocolBufferException ] {
      compareTwoProtocols(HybridCodec.TranslationMode.JavaThenJava, HybridCodec.TranslationMode.MryThenMry)
    }

    intercept[com.google.protobuf.InvalidProtocolBufferException ] {
      compareTwoProtocols(HybridCodec.TranslationMode.BothThenJava, HybridCodec.TranslationMode.MryThenMry)
    }
  }
}