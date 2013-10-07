package com.wajam.mry.api.protobuf

object BenchmarkRunner extends App {

  val repetition = Seq(10, 100, 1000, 2500, 5000, 10000, 50000, 100000)

  def warmUp(name: String, repeat: Int, method: (Int) => Unit) = {
    method(repeat)
  }

  def runTest(name: String, repeat: Int, value: Int, method: (Int) => Unit, sizef: () => Int) = {

    System.out.println(s"Testing $name with value: $value...")

    val startTime = System.currentTimeMillis()

    method(repeat)

    val endTime = System.currentTimeMillis()

    val ms = endTime - startTime
    val size = sizef()
    val ratio = value * 1.0/ms

    System.out.println(s"Took ${ms}ms, size was: ${size/1024}KB.")
    System.out.println(f"t/m ratio: ${ms*1.0/value}%2.2f.")
  }

  // Warmup
  val warmRepeat = 10000

  {
    System.out.println(s"Warming up...")
    val b = new ProtobufTranslatorBenchmark(repetition(0))
    b.setUp()
    warmUp("Encoding", warmRepeat, b.timeProtobufEncode _)
    warmUp("Decoding", warmRepeat, b.timeProtobufDecode _)
    System.out.println()
  }

  // Real test
  val repeat = 1

  repetition.foreach { r =>
    System.out.println(s"--Encode/Decode pair with $r--")
    val b = new ProtobufTranslatorBenchmark(r)
    b.setUp()
    runTest("Encoding", repeat, r, b.timeProtobufEncode _, b.encodingSize _)
    runTest("Decoding", repeat, r, b.timeProtobufDecode _, b.encodingSize _)
    System.out.println()
  }

}
