package com.appaquet.mry.execution


/**
 * Represents an operation executed within a transaction
 */
abstract class Operation(var source: OperationSource) extends Executable {
}

object Operation {
  class Return(source: OperationSource, from: Seq[Variable]) extends Operation(source) {
    def execute(context: ExecutionContext) {
      source.execReturn(context, from)
    }

    def reset() {}
  }

  class GetTable(source: OperationSource, name: String, into: Variable) extends Operation(source) {
    def execute(context: ExecutionContext) {
      source.execFromTable(context, name, into)
    }

    def reset() {}
  }

  class Get(source: OperationSource, key: Object, into: Variable) extends Operation(source) {
    def execute(context: ExecutionContext) {
      source.execGet(context, key, into)
    }

    def reset() {}
  }
  
  class Set(source: OperationSource, key: Object, value:Object, into: Variable) extends Operation(source) {
    def execute(context: ExecutionContext) {
      source.execSet(context, key, value, into)
    }

    def reset() {}
  }
}
