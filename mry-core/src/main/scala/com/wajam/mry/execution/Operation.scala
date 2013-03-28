package com.wajam.mry.execution

import com.wajam.nrv.utils.ContentEquals

/**
 * Represents an operation executed within a transaction
 */
abstract class Operation(var source: OperationSource) extends Executable with ContentEquals with Serializable {
}

object Operation {

  // TODO: To be replaced by proper hierachy when Java serialization in no longer used.
  type WithIntoAndData = {def into: Variable; def data: Seq[Object]}
  type WithIntoAndKeys = {def into: Variable; def keys: Seq[Object]}
  type WithFrom = {def from: Seq[Variable]}

  class Return(source: OperationSource, val from: Seq[Variable]) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execReturn(context, from)
    }

    def reset() {}

    override def equalsContent(obj: Any): Boolean = {
      obj match {
        case o: Return =>
          from.forall((v) => o.from.exists(v.equalsContent(_)))

        case _ => false
      }
    }
  }

  class From(source: OperationSource, val into: Variable, val keys: Object*) extends Operation(source) with Serializable {

    def execute(context: ExecutionContext) {
      source.execFrom(context, into, keys: _*)
    }

    def reset() {}

    override def equalsContent(obj: Any): Boolean = {
      obj match {
        case o: From =>
          into.equalsContent(o.into) &&
            keys.forall((k) => o.keys.exists(k.equalsContent(_)))

        case _ => false
      }
    }
  }

  class Get(source: OperationSource, val into: Variable, val keys: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execGet(context, into, keys: _*)
    }

    override def equalsContent(obj: Any): Boolean = {
      obj match {
        case o: Get =>
          into.equalsContent(o.into) &&
            keys.forall((k) => o.keys.exists(k.equalsContent(_)))

        case _ => false
      }
    }

    def reset() {}
  }

  class Set(source: OperationSource, val into: Variable, val data: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execSet(context, into, data: _*)
    }

    def reset() {}

    override def equalsContent(obj: Any): Boolean = {
      obj match {
        case o: Set =>
          into.equalsContent(o.into) &&
            data.forall((k) => o.data.exists(k.equalsContent(_)))

        case _ => false
      }
    }
  }

  class Delete(source: OperationSource, val into: Variable, val data: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execDelete(context, into, data: _*)
    }

    def reset() {}

    override def equalsContent(obj: Any): Boolean = {
      obj match {
        case o: Delete =>
          into.equalsContent(o.into) &&
            data.forall((k) => o.data.exists(k.equalsContent(_)))

        case _ => false
      }
    }
  }

  class Limit(source: OperationSource, val into: Variable, val keys: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execLimit(context, into, keys: _*)
    }

    def reset() {}

    override def equalsContent(obj: Any): Boolean = {
      obj match {
        case o: Limit =>
          into.equalsContent(o.into) &&
            keys.forall((k) => o.keys.exists(k.equalsContent(_)))

        case _ => false
      }
    }
  }

  class Projection(source: OperationSource, val into: Variable, val keys: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execProjection(context, into, keys: _*)
    }

    def reset() {}

    override def equalsContent(obj: Any): Boolean = {
      obj match {
        case o: Projection =>
          into.equalsContent(o.into) &&
            keys.forall((k) => o.keys.exists(k.equalsContent(_)))

        case _ => false
      }
    }
  }

}

