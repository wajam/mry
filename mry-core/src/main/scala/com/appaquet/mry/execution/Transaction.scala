package com.appaquet.mry.execution

/**
 * DESCRIPTION HERE
 *
 * Author: Andre-Philippe Paquet < app@quet.ca >
 */

class Transaction(var id:Int = 0) extends Block with ExecutionSource {
  def sourceBlock = this

  override def execGetTable(name: String, into: Variable) {
  }
}
