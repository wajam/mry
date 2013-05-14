package com.wajam.mry.execution

object MryFilters extends Enumeration {
  type MryFilter = Value

  val Equals = Value(1)
  val GreaterThan = Value(2)
  val GreaterThanOrEqual = Value(3)
  val LesserThan = Value(4)
  val LesserThanOrEqual = Value(5)

  def apply(filter: MryFilters.MryFilter): Filter = {
    filter match {
      case Equals => EqualsFilter
      case GreaterThan => GreaterThanFilter
      case GreaterThanOrEqual => GreaterThanOrEqualFilter
      case LesserThan => LesserThanFilter
      case LesserThanOrEqual => LesserThanOrEqualFilter
    }
  }

  sealed trait Filter {
    def execute(left: Object, right: Object): Boolean

    protected def throwMatchError(left: com.wajam.mry.execution.Value,
                                right: com.wajam.mry.execution.Value): Boolean = {
      throw new RuntimeException("Unsupported type for filter: left:%s, right:%s".format(left.getClass, right.getClass))
    }
  }

  case object EqualsFilter extends Filter {
    def execute(left: Object, right: Object) = {
      left.value.equalsValue(right.value)
    }
  }

  case object GreaterThanFilter extends Filter {
    def execute(left: Object, right: Object) = {
      (left.value, right.value) match {
        case (l: IntValue, r: IntValue) => l.intValue > r.intValue
        case (l, r) => throwMatchError(l, r)
      }
    }
  }

  case object GreaterThanOrEqualFilter extends Filter {
    def execute(left: Object, right: Object) = {
      (left.value, right.value) match {
        case (l: IntValue, r: IntValue) => l.intValue >= r.intValue
        case (l, r) => throwMatchError(l, r)
      }
    }
  }

  case object LesserThanFilter extends Filter {
    def execute(left: Object, right: Object) = {
      (left.value, right.value) match {
        case (l: IntValue, r: IntValue) => l.intValue < r.intValue
        case (l, r) => throwMatchError(l, r)
      }
    }
  }

  case object LesserThanOrEqualFilter extends Filter {
    def execute(left: Object, right: Object) = {
      (left.value, right.value) match {
        case (l: IntValue, r: IntValue) => l.intValue <= r.intValue
        case (l, r) => throwMatchError(l, r)
      }
    }
  }

}
