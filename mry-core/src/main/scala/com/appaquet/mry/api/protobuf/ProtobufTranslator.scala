package com.appaquet.mry.api.protobuf

import com.appaquet.mry.api.ProtocolTranslator
import com.appaquet.mry.api.protobuf.Transaction.PTransaction
import com.appaquet.mry.execution.{Transaction => MryTransaction}

/**
 * DESCRIPTION HERE
 *
 * Author: Andre-Philippe Paquet < app@quet.ca >
 */

class ProtobufTranslator extends ProtocolTranslator[PTransaction] {
  def translateTransaction(transaction: PTransaction):MryTransaction = {
    null
  }

  def translateReturn(transaction: MryTransaction):PTransaction = {
    null
  }
}
