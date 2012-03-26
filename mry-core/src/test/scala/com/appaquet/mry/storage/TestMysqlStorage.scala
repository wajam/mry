package com.appaquet.mry.storage

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

/**
 * Test MySQL storage
 */
@RunWith(classOf[JUnitRunner])
class TestMysqlStorage extends TestStorageSuite {
  val storage = new MysqlStorage("localhost", "mry", "mry", "mry")

  testStorage(storage)
}
