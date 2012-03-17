package com.appaquet.mry.execution

import org.scalatest.FunSuite
import com.appaquet.mry.execution.Implicits._

/**
 * DESCRIPTION HERE
 *
 * Author: Andre-Philippe Paquet < app@quet.ca >
 */

class TestTransaction extends FunSuite {
  test("execute") {
    val t = new Transaction()
    val v = t.Get("test")
  }
}
