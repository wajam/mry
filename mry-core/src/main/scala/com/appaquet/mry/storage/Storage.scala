package com.appaquet.mry.storage

import com.appaquet.mry.model.Model

/**
 * Storage engine used to store records
 */
abstract class Storage {
  def getTransaction: StorageTransaction

  def syncModel(model: Model)

  def nuke()
}
