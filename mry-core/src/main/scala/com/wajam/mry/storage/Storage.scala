package com.wajam.mry.storage

import com.wajam.mry.model.Model
import com.wajam.mry.execution.Timestamp

/**
 * Storage engine used to store records
 */
abstract class Storage {
  def getTransaction(time: Timestamp): StorageTransaction

  def syncModel(model: Model)

  def nuke()
}
