package com.appaquet.mry.storage

import com.appaquet.mry.model.Model
import com.appaquet.mry.execution.Timestamp

/**
 * Storage engine used to store records
 */
abstract class Storage {
  def getTransaction(time:Timestamp): StorageTransaction

  def syncModel(model: Model)

  def nuke()
}
