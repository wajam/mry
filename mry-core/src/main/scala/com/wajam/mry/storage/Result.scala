package com.wajam.mry.storage

/**
 * Iterable result of the storage
 */
abstract class Result[A] extends Iterator[A] {
  def close()
}
