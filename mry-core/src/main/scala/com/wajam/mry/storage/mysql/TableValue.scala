package com.wajam.mry.storage.mysql

import com.wajam.mry.execution._

/**
 * MRY value representing a mysql table
 */
class TableValue(storage: MysqlStorage, table: Table, prefixKeys: Seq[String] = Seq()) extends Value {
  override def execGet(context: ExecutionContext, into: Variable, keys: Object*) {
    // TODO: get with no keys = multiple row

    val key = param[StringValue](keys, 0).strValue
    val keysSeq = prefixKeys ++ Seq(key)

    val token = context.getToken(keysSeq(0))
    context.useToken(token)

    if (!context.dryMode) {
      into.value = new RecordValue(storage, context, table, token, keysSeq)
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
      transaction.set(table, token, keysSeq, Some(new transaction.Record(mapVal)))
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
      transaction.set(table, token, keysSeq, None)
    }
  }

}


