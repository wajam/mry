package com.appaquet.mry.execution

/**
 * Transaction (block of operations) executed on storage
 */
class Transaction(var id:Int = 0) extends Block with ExecutionSource {
  def sourceBlock = this

  override def execGetTable(name: String, into: Variable) {
  }
}
