package com.wajam.mry.execution

object MryFilters extends Enumeration {
  type MryFilter = Value

  val Equals = Value("eq")

  private val operatorsFunctions: Map[String, (Object, Object) => Boolean] = Map(
    Equals.toString -> ((left: Object, right: Object) => {
      left.value.equalsValue(right.value)
    })
  )

  def applyFilter(left: Object, filter: MryFilters.MryFilter, right: Object): Boolean = {
    val operator = operatorsFunctions(filter.toString)

    operator(left, right)
  }
}