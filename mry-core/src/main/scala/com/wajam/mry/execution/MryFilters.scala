package com.wajam.mry.execution

object MryFilters extends Enumeration {
  type MryFilter = Value

  val Equals = Value(1, "eq")
  val GreaterThan = Value(2, "gt")
  val GreaterThanOrEqual = Value(3, "gte")
  val LesserThan = Value(4, "lt")
  val LesserThanOrEqual = Value(5, "lte")

  def applyFilter(left: Object, filter: MryFilters.MryFilter, right: Object): Boolean = {
    val operator = filter match {
      case Equals => EqualsFilter()
      case GreaterThan => GreaterThanFilter()
      case GreaterThanOrEqual => GreaterThanOrEqualFilter()
      case LesserThan => LesserThanFilter()
      case LesserThanOrEqual => LesserThanOrEqualFilter()
    }

    operator.execute(left, right)
  }

  trait Filter {
    def execute(left: Object, right: Object): Boolean

    protected def throwMatchError(left: com.wajam.mry.execution.Value,
                                right: com.wajam.mry.execution.Value): Boolean = {
      throw new RuntimeException("Unsupported type for filter: left:%s, right:%s".format(left.getClass, right.getClass))
    }
  }

  case class EqualsFilter() extends Filter {
    def execute(left: Object, right: Object) = {
      left.value.equalsValue(right.value)
    }
  }

  case class GreaterThanFilter() extends Filter {
    def execute(left: Object, right: Object) = {
      (left.value, right.value) match {
        case (l: IntValue, r: IntValue) => l.intValue > r.intValue
        case (l, r) => throwMatchError(l, r)
      }
    }
  }

  case class GreaterThanOrEqualFilter() extends Filter {
    def execute(left: Object, right: Object) = {
      (left.value, right.value) match {
        case (l: IntValue, r: IntValue) => l.intValue >= r.intValue
        case (l, r) => throwMatchError(l, r)
      }
    }
  }

  case class LesserThanFilter() extends Filter {
    def execute(left: Object, right: Object) = {
      (left.value, right.value) match {
        case (l: IntValue, r: IntValue) => l.intValue < r.intValue
        case (l, r) => throwMatchError(l, r)
      }
    }
  }

  case class LesserThanOrEqualFilter() extends Filter {
    def execute(left: Object, right: Object) = {
      (left.value, right.value) match {
        case (l: IntValue, r: IntValue) => l.intValue <= r.intValue
        case (l, r) => throwMatchError(l, r)
      }
    }
  }

}
