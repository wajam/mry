package com.wajam.mry.storage

import com.wajam.mry.execution.{ExecutionContext, Value, Timestamp}


/**
 * Storage engine used by executed transaction
 */
abstract class Storage(var name: String) {
  def getStorageValue(context: ExecutionContext): Value

  def getStorageTransaction(context: ExecutionContext): StorageTransaction

  def nuke()

  def close()
}
