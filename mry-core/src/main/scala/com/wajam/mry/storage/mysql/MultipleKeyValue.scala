package com.wajam.mry.storage.mysql

import com.wajam.mry.execution._
import com.wajam.nrv.Logging
import scala.Some

/**
 * 
 */
class MultipleKeyValue (storage: MysqlStorage, context: ExecutionContext, table: Table, token: Long,
                        accessPrefix: AccessPath) extends Value with Logging {
  private var loaded = false
  private var optOffset: Option[Long] = None
  private var optCount: Option[Long] = None

  val transaction = context.dryMode match {
    case false =>
      context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction]

    case true =>
      null
  }

  lazy val iterator: Option[KeyIterator] = {
    if (!context.dryMode) {
      loaded = true
      Some(transaction.getKeys(table, token, context.timestamp.get,
        accessPrefix, optOffset = optOffset, optCount = optCount))
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
            val record = iter.key
            list :+= new KeyValue(storage, context, table, token, accessPrefix, Some(transaction), Some(record))
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
      val newRec = new MultipleKeyValue(storage, context, table, token, accessPrefix)
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

  // Serialized version of this record is the inner map or null
  override def serializableValue = this.innerValue.serializableValue
}