package com.wajam.mry.storage.mysql

import com.wajam.mry.execution._
import com.wajam.mry.storage.{StorageTransaction, StorageException}


/**
 * MRY value representing a mysql record
 */
class RecordValue(storage: MysqlStorage, context: ExecutionContext, table: Table, token: Long,
                  prefixKeys: Seq[String], var optTransaction:Option[MysqlTransaction] = None,
                  var optRecord:Option[Record] = None) extends Value {

  if (optTransaction.isEmpty) {
    if (!context.dryMode) {
      optTransaction = Some(context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction])
    }
  }


  if (optRecord.isEmpty && !context.dryMode) {
    optRecord = optTransaction.get.get(table, token, context.timestamp, prefixKeys)
  }

  val innerValue = {
    this.optRecord match {
      case Some(r) =>
        r.value
      case None =>
        new NullValue
    }
  }

  // Serialized version of this record is the inner map or null
  override def serializableValue = this.innerValue

  // Operations are executed on record data (map or null)
  override def proxiedSource: Option[OperationSource] = Some(this.innerValue)

  override def execFrom(context: ExecutionContext, into: Variable, keys: Object*) {
    val tableName = param[StringValue](keys, 0).strValue
    val optTable = table.getTable(tableName)

    optTable match {
      case Some(t) =>
        into.value = new TableValue(storage, t, prefixKeys)

      case None =>
        throw new StorageException("Non existing table %s".format(tableName))

    }
  }
}


