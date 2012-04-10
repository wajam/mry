package com.wajam.mry.execution

/**
 * Represents an operation executed within a transaction
 */
abstract class Operation(var source: OperationSource) extends Executable with Serializable {
}

object Operation {

  class Return(source: OperationSource, from: Seq[Variable]) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execReturn(context, from)
    }

    def reset() {}
  }

  class From(source: OperationSource, into: Variable, keys: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execFrom(context, into, keys: _*)
    }

    def reset() {}
  }

  class Get(source: OperationSource, into: Variable, keys: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execGet(context, into, keys: _*)
    }

    def reset() {}
  }

  class Set(source: OperationSource, into: Variable, value: Object, keys: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execSet(context, into, value, keys: _*)
    }

    def reset() {}
  }

}
