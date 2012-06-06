package com.wajam.mry.storage.mysql

import com.wajam.mry.execution._
import com.wajam.mry.storage.StorageException


/**
 * MRY value representing a mysql record
 */
class RecordValue(storage: MysqlStorage, context: ExecutionContext, table: Table, token: Long, prefixKeys: Seq[String]) extends Value {
  val transaction = context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction]

  val record = context.dryMode match {
    case false => transaction.get(table, token, context.timestamp, prefixKeys)
    case true => None
  }

  val innerValue = {
    this.record match {
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


