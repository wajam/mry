package com.wajam.mry.storage

import com.wajam.mry.execution.{ExecutionContext, Value}


/**
 * Storage engine used by executed transaction
 */
trait Storage {
  def name: String

  def getStorageValue(context: ExecutionContext): Value

  def createStorageTransaction(context: ExecutionContext): StorageTransaction

  def nuke()

  def start()

  def stop()
}
