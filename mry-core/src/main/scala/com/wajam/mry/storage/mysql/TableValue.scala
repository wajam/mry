package com.wajam.mry.storage.mysql

import com.wajam.mry.execution._

/**
 * MRY value representing a mysql table
 */
class TableValue(storage: MysqlStorage, table: Table, prefixKeys: Seq[String] = Seq()) extends Value {
  override def execGet(context: ExecutionContext, into: Variable, keys: Object*) {
    if (keys.length > 0) {
      // get a specific row

      val key = param[StringValue](keys, 0).strValue
      val keysSeq = prefixKeys ++ Seq(key)

      val token = context.getToken(keysSeq(0))
      context.useToken(token)

      into.value = new RecordValue(storage, context, table, token, keysSeq)

    } else {
      // get multiple rows

      if (prefixKeys.size == 0)
        throw new InvalidParameter("Can't get multiple rows (no key) on first level tables")

      // get token from first parameter
      val keysSeq = prefixKeys
      val token = context.getToken(keysSeq(0))
      into.value = new MultipleRecordValue(storage, context, table, token, prefixKeys)
    }
  }

  override def execSet(context: ExecutionContext, into: Variable, data: Object*) {
    val key = param[StringValue](data, 0).strValue
    val mapVal = param[MapValue](data, 1)
    val keysSeq = prefixKeys ++ Seq(key)

    val token = context.getToken(keysSeq(0))
    context.useToken(token)
    context.isMutation = true

    if (!context.dryMode) {
      val transaction = context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction]

      val record = new Record
      record.value = mapVal
      transaction.set(table, token, context.timestamp, keysSeq, Some(record))
    }
  }

  override def execDelete(context: ExecutionContext, into: Variable, data: Object*) {
    val key = param[StringValue](data, 0).strValue
    val keysSeq = prefixKeys ++ Seq(key)

    val token = context.getToken(keysSeq(0))
    context.useToken(token)
    context.isMutation = true

    if (!context.dryMode) {
      val transaction = context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction]
      transaction.set(table, token, context.timestamp, keysSeq, None)
    }
  }

}


