package com.wajam.mry.storage.mysql

import com.wajam.mry.execution.{ListValue, ExecutionContext, Value}
import com.wajam.nrv.Logging

/**
 * MRY value representing multiple values
 */
class MultipleRecordValue(storage: MysqlStorage, context: ExecutionContext, table: Table, token: Long, accessPrefix: AccessPath) extends Value with Logging {

  val (transaction, iterator) = context.dryMode match {
    case false =>
      val transaction = context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction]
      val iterator = transaction.getMultiple(table, token, context.timestamp.get, accessPrefix)
      (transaction, Some(iterator))

    case true =>
      (null, None)
  }

  val innerValue = {
    this.iterator match {
      case Some(iter) =>
        try {
          var list = List[Value]()
          while (iter.next()) {
            val record = iter.record
            list :+= new RecordValue(storage, context, table, token, record.accessPath, Some(transaction), Some(record))
          }

          new ListValue(list)
        } finally {
          iter.close()
        }

      case None =>
        new ListValue(Seq())
    }
  }

  // Serialized version of this record is the inner map or null
  override def serializableValue = this.innerValue.serializableValue
}
