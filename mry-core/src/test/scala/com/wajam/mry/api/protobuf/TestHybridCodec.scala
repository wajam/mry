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

  private def compareTwoProtocols(mA: HybridCodec.TransitionMode.Value, mB: HybridCodec.TransitionMode.Value) = {

    val t = new Transaction((b) => b.returns(b.from("B").get(1000)))

    val codecA = new HybridCodec(mA)

    val codecB = new HybridCodec(mB)

    val bytes = codecA.encode(t)
    val t2 = codecB.decode(bytes)

    assert(t.equalsContent(t2))
  }

  test("can encode using mry and can decode using both") {
    compareTwoProtocols(HybridCodec.TransitionMode.DecBothEncMry, HybridCodec.TransitionMode.DecBothEncMry)
    compareTwoProtocols(HybridCodec.TransitionMode.DecBothEncMry, HybridCodec.TransitionMode.DecBothEncJava)
  }

  test("can encode using java and can decode using both") {
    compareTwoProtocols(HybridCodec.TransitionMode.DecBothEncJava, HybridCodec.TransitionMode.DecBothEncMry)
    compareTwoProtocols(HybridCodec.TransitionMode.DecBothEncJava, HybridCodec.TransitionMode.DecBothEncJava)
  }

  test("can't encode using mry and can decode using java") {

    intercept[java.io.StreamCorruptedException] {
     compareTwoProtocols(HybridCodec.TransitionMode.DecMryEncMry, HybridCodec.TransitionMode.DecJavaEncJava)
    }

    intercept[java.io.StreamCorruptedException] {
      compareTwoProtocols(HybridCodec.TransitionMode.DecBothEncMry, HybridCodec.TransitionMode.DecJavaEncJava)
    }
  }

  test("can't encode using java and can decode using mry") {
    intercept[com.google.protobuf.InvalidProtocolBufferException ] {
      compareTwoProtocols(HybridCodec.TransitionMode.DecJavaEncJava, HybridCodec.TransitionMode.DecMryEncMry)
    }

    intercept[com.google.protobuf.InvalidProtocolBufferException ] {
      compareTwoProtocols(HybridCodec.TransitionMode.DecBothEncJava, HybridCodec.TransitionMode.DecMryEncMry)
    }
  }
}