package com.appaquet.mry.api

import com.appaquet.mry.execution.Transaction


/**
 * DESCRIPTION HERE
 *
 * Author: Andre-Philippe Paquet < app@quet.ca >
 */

trait ProtocolTranslator[T] {
  def translateTransaction(transaction: T): Transaction

  def translateReturn(transaction: Transaction): T
}
