package com.wajam.mry.api.protobuf

import com.wajam.mry.execution.Transaction
import com.wajam.mry.execution.Implicits._

class ProtobufTranslatorBenchmark(length: Int) {

  var trx: Transaction = null
  var trxBytes: Array[Byte] = null

  private def randomTransaction(opCount: Int) = {
    val t = new Transaction
    val v = t.from("followees").get(Long.MaxValue)

    for (i <- 1 to opCount)
      v.from("followeers").delete(Long.MaxValue)

    t
  }

  def setUp() {
    val t = new ProtobufTranslator()
    trx = randomTransaction(length)
    trxBytes = t.encodeTransaction(trx)
  }

  def encodingSize() = {
    trxBytes.length
  }

  def timeProtobufEncode(reps: Int) {
    val t = new ProtobufTranslator()

    for (_ <- (1 to reps)) {
      t.encodeTransaction(trx)
    }
  }

  def timeProtobufDecode(reps: Int) {
    val t = new ProtobufTranslator()
    for (_ <- (1 to reps)) {
      t.decodeTransaction(trxBytes)
    }
  }
}
