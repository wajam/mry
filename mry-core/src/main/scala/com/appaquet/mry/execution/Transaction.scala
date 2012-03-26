package com.appaquet.mry.execution

/**
 * Transaction (block of operations) executed on storage
 */
class Transaction(var id:Int = 0) extends Block with OperationApi with OperationSource {
  def sourceBlock = this

  override def execFromTable(context: ExecutionContext, name: String, into: Variable) {
    into.value = new TableValue(context.model.getTable(name).get)
  }

  override def execReturn(context: ExecutionContext, from: Seq[Variable]) {
    context.returnValues = for (variable <- from) yield variable.value
  }
}
