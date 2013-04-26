package com.wajam.mry.execution

object MryFilters extends Enumeration {
  type MryFilter = Value

  val Equals = Value(1, "eq")
  val GreaterThan = Value(2, "gt")
  val GreaterThanOrEqual = Value(3, "gte")
  val LesserThan = Value(4, "lt")
  val LesserThanOrEqual = Value(5, "lte")
  
  private def throwMatchError(left: com.wajam.mry.execution.Value,
                              right: com.wajam.mry.execution.Value): Boolean = {
    throw new RuntimeException("Unsupported type for filter: left:%s, right:%s".format(left.getClass, right.getClass))  
  }

  private val operatorsFunctions: Map[MryFilter, (Object, Object) => Boolean] = Map(
    Equals -> ((left: Object, right: Object) => {
      left.value.equalsValue(right.value)
    }),

    GreaterThan -> ((left: Object, right: Object) => {
      (left.value, right.value) match {
        case (l: IntValue, r: IntValue) => l.intValue > r.intValue
        case (l, r) => throwMatchError(l, r)
      }
    }),

    GreaterThanOrEqual -> ((left: Object, right: Object) => {
      (left.value, right.value) match {
        case (l: IntValue, r: IntValue) => l.intValue >= r.intValue
        case (l, r) => throwMatchError(l, r)
      }
    }),

    LesserThan -> ((left: Object, right: Object) => {
      (left.value, right.value) match {
        case (l: IntValue, r: IntValue) => l.intValue < r.intValue
        case (l, r) => throwMatchError(l, r)
      }
    }),

    LesserThanOrEqual -> ((left: Object, right: Object) => {
      (left.value, right.value) match {
        case (l: IntValue, r: IntValue) => l.intValue <= r.intValue
        case (l, r) => throwMatchError(l, r)
      }
    })
  )

  def applyFilter(left: Object, filter: MryFilters.MryFilter, right: Object): Boolean = {
    val operator = operatorsFunctions(filter)

    operator(left, right)
  }
}