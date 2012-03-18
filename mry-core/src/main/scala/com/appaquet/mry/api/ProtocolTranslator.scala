package com.appaquet.mry.api

import com.appaquet.mry.execution.Transaction

/**
 * Transaction translators, used to translate protocol specific transactions to
 * mry objects
 */
trait ProtocolTranslator[T] {
  def translateTransaction(transaction: T): Transaction

  def translateReturn(transaction: Transaction): T
}
