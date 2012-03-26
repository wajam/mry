package com.appaquet.mry.storage

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestMemoryStorage extends TestStorageSuite {
  val storage = new MemoryStorage

  testStorage(storage)
}
