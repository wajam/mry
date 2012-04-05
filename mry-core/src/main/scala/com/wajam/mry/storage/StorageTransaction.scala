package com.wajam.mry.storage

/**
 * Storage operations transactions with ACID properties
 */
abstract class StorageTransaction {
  def rollback()

  def commit()
}
