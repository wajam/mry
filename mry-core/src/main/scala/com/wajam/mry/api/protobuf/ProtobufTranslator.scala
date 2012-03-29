package com.wajam.mry.api.protobuf

import com.wajam.mry.api.ProtocolTranslator
import com.wajam.mry.api.protobuf.Transaction.PTransaction
import com.wajam.mry.execution.{Transaction => MryTransaction}

/**
 * Protocol buffers translator
 */
class ProtobufTranslator extends ProtocolTranslator[PTransaction] {
  def translateTransaction(transaction: PTransaction): MryTransaction = {
    null
  }

  def translateReturn(transaction: MryTransaction): PTransaction = {
    null
  }
}
