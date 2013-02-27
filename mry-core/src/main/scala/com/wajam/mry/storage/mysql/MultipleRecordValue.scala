package com.wajam.mry.storage.mysql

import com.wajam.mry.execution._
import com.wajam.nrv.Logging
import com.wajam.nrv.utils.timestamp.Timestamp

abstract class MysqlStorageMultipleValue[RecordType](storage: MysqlStorage,
                                               context: ExecutionContext,
                                               table: Table,
                                               token: Long,
                                               accessPrefix: AccessPath)
  extends Value with Logging {

  protected var loaded = false
  protected var optOffset: Option[Long] = None
  protected var optCount: Option[Long] = None

  val transaction = context.dryMode match {
    case false => context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction]
    case true => null
  }

  lazy val iterator: Option[RecordIterator[RecordType]] = {
    if (!context.dryMode) {
      loaded = true
      Some(loadRecords())
    } else {
      None
    }
  }

  lazy val innerValue = {
    this.iterator match {
      case Some(iter) =>
        try {
          var list = List[Value]()
          while (iter.next()) {
            list :+= createValue(iter.record)
          }
          new ListValue(list)
        } finally {
          iter.close()
        }

      case None =>
        new ListValue(Seq())
    }
  }

  override def execLimit(context: ExecutionContext, into: Variable, keys: Object*) {
    if (loaded) {
      val newRec = new MultipleRecordValue(storage, context, table, token, accessPrefix)
      newRec.execLimit(context, into, keys: _*)
      into.value = newRec
    } else {
      if (keys.length == 1) {
        optCount = Some(param[IntValue](keys, 0).intValue)
      } else if (keys.length == 2) {
        optOffset = Some(param[IntValue](keys, 0).intValue)
        optCount = Some(param[IntValue](keys, 1).intValue)
      }
      into.value = this
    }
  }

  protected def loadRecords(): RecordIterator[RecordType]

  protected def createValue(record: RecordType): Value

  protected def createMultipleValue: MysqlStorageMultipleValue[RecordType]

  override def serializableValue = this.innerValue.serializableValue
}
/**
 * MRY value representing multiple values
 */
class MultipleRecordValue(storage: MysqlStorage, context: ExecutionContext, table: Table, token: Long, accessPrefix: AccessPath)
  extends MysqlStorageMultipleValue[Record](storage, context, table, token, accessPrefix) with Logging {

  protected def loadRecords() = {
    transaction.getMultiple(table, token, context.timestamp.get, accessPrefix, optOffset = optOffset, optCount = optCount)
  }

  protected def createValue(record: Record) = {
    new RecordValue(storage, context, table, token, record.accessPath, Some(transaction), Some(record))
  }

  protected def createMultipleValue = new MultipleRecordValue(storage, context, table, token, accessPrefix)
}

class MultipleKeyValue(storage: MysqlStorage, context: ExecutionContext, table: Table, token: Long, accessPrefix: AccessPath)
  extends MysqlStorageMultipleValue[Key](storage, context, table, token, accessPrefix) with Logging {

  protected def loadRecords() = {
    transaction.getKeys(table, token, context.timestamp.get, accessPrefix, optOffset = optOffset, optCount = optCount)
  }

  protected def createValue(record: Key) = {
    new KeyValue(storage, context, table, token, accessPrefix, Some(transaction), Some(record))
  }

  protected def createMultipleValue = new MultipleKeyValue(storage, context, table, token, accessPrefix)
}
