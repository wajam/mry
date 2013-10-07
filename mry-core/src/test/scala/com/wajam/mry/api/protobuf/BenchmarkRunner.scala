package com.wajam.mry.api.protobuf

import com.wajam.mry.execution.Transaction
import com.wajam.mry.execution.Implicits._


object BenchmarkRunner extends App {

  val repetition = Seq(10, 100, 1000, 2500, 5000, 10000, 50000, 100000)

  private def randomTransaction(opCount: Int) = {
    val t = new Transaction
    val v = t.from("followees").get(Long.MaxValue)

    for (i <- 1 to opCount)
      v.from("followeers").delete(Long.MaxValue)

    t
  }

  def timed(method: () => Unit): Long = {

    val startTime = System.currentTimeMillis()

    method()

    val endTime = System.currentTimeMillis()

    endTime - startTime
  }

  def warmUp(warmRepeat: Int, warmCount: Int) {
      System.out.println(s"Warming up...")

      for(i <- 1 to warmRepeat) {
        val trx = randomTransaction(warmCount)
        val t = new ProtobufTranslator()

        val bytes = t.encodeTransaction(trx)
        t.decodeTransaction(bytes)
      }
  }


  def runTest(name: String, value: Int, fct: () => Unit, size: () => Int) = {
    System.out.println(s"Testing $name with value: $value...")

    val runMs = timed (fct)

    System.out.println(s"Run took ${runMs}ms, size was: ${size()/1024}KB.")
    System.out.println(f"t/n ratio: ${runMs*1.0/value}%2.2f.")
  }

  def runTests(value: Int) = {

    System.out.println(s"--Encode/Decode pair with $value--")

    val t = new ProtobufTranslator()

    System.out.println(s"Setting up for: $value...")

    var trx: Transaction = null
    val setupMs = timed ( () => {
        trx = randomTransaction(value)
      }
    )

    System.out.println(s"Setup took ${setupMs}ms.")

    var bytes: Array[Byte] = null

    runTest(
      "Encoding",
      value,
      () =>  { bytes = t.encodeTransaction(trx) },
      () => {bytes.length}
    )

    runTest(
      "Decoding",
      value,
      () =>  { t.decodeTransaction(bytes) },
      () => {bytes.length}
    )

    System.out.println()
  }

  // Warmup
  warmUp(10000, 10)

  // Real test
  val repeat = 1

  repetition.foreach { value =>
    runTests(value)
  }

}
