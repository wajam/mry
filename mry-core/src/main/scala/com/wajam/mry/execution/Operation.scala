package com.wajam.mry.execution

import com.wajam.nrv.utils.ContentEquals

/**
 * Represents an operation executed within a transaction
 */
@SerialVersionUID(-6101323863732863722L)
abstract class Operation(var source: OperationSource) extends Executable with ContentEquals with Serializable {
}

object Operation {

  @SerialVersionUID(8422228565382223858L)
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

  @SerialVersionUID(8632712547655708378L)
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

  @SerialVersionUID(705646469709308372L)
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

  @SerialVersionUID(8505688065416500637L)
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

  @SerialVersionUID(5829527297848291007L)
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

  @SerialVersionUID(-2116184285973412485L)
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

  @SerialVersionUID(5345979382788068144L)
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

  class Filter(source: OperationSource, val into: Variable, val key: Object, val filter: MryFilters.MryFilter, val value: Object) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execFiltering(context, into, key, filter, value)
    }

    def reset() {}

    override def equalsContent(obj: Any): Boolean = {
      obj match {
        case o: Filter =>
          into.equalsContent(o.into) &&
          key.equalsContent(o.key) &&
          value.equalsContent(o.value) &&
          filter == o.filter

        case _ => false
      }
    }
  }

}

