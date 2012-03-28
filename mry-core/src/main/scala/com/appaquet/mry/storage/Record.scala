package com.appaquet.mry.storage

import com.appaquet.mry.execution.Timestamp

/**
 * Record (row/document) stored in a storage
 */
class Record(var value: Array[Byte] = Array[Byte](), var key: String = "", var timestamp: Timestamp = Timestamp(0)) {

  import Record._

  def stringValue: String = value
}

object Record {
  implicit def string2record(data: String): Record = new Record(data)

  implicit def string2bytes(data: String): Array[Byte] = data.getBytes("UTF8")

  implicit def bytes2string(data: Array[Byte]): String = new String(data, "UTF8")
}
