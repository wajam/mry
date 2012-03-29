package com.wajam.mry.execution

import com.wajam.mry.model.Table
import com.wajam.mry.storage.Record

/**
 * Value (string, int, record) used in transaction operations
 */
abstract class Value extends Object with OperationSource {
}

class NullValue() extends Value {
}

class StringValue(var value: String) extends Value {
  override def toString = value
}

class TableValue(var table: Table) extends Value {
  override def execGet(context: ExecutionContext, key: Object, into: Variable) {
    val record = context.storageTransaction.get(table, Seq(key.toString))
    into.value = record match {
      case None =>
        new NullValue
      case Some(r) =>
        new RecordValue(r)
    }
  }

  override def execSet(context: ExecutionContext, key: Object, value: Object, into: Variable) {
    context.storageTransaction.set(table, Seq(key.toString), value.toString)
  }
}

class RecordValue(var record: Record) extends Value {
  override def toString = record.stringValue
}


