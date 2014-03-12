package com.wajam.mry.storage.mysql.cache

import com.yammer.metrics.scala.Instrumented
import com.wajam.mry.storage.mysql.Table

trait CacheMetrics extends Instrumented {
  lazy val hitsMeter = new TableMetrics(descendants = true, scope => metrics.meter("hits", "hits", scope))
  lazy val missesMeter = new TableMetrics(descendants = true, scope => metrics.meter("misses", "misses", scope))

  lazy val evictionExpiredCounter = new TableMetrics(descendants = false, scope => metrics.counter("eviction-expired", scope))
  lazy val evictionSizeCounter = new TableMetrics(descendants = false, scope => metrics.counter("eviction-size", scope))

  val cacheCurrentSizeGauge = new TableGauges[Int]("cache-current-size")
  val cacheMaxSizeGauge = new TableGauges[Int]("cache-max-size")

  protected def resetMetrics(): Unit = {
    cacheCurrentSizeGauge.reset()
    cacheMaxSizeGauge.reset()
  }

  class TableMetrics[T](descendants: Boolean, newMetric: (String) => T) {
    private var cacheMetrics: Map[String, T] = Map()

    def apply(table: Table): Seq[T] = {
      val seq = getOrAddMetric(CacheMetrics.TotalScope) :: getOrAddMetric(table.getTopLevelTable.name) :: Nil
      if (descendants) getOrAddMetric(table.toString) :: seq else seq
    }

    private def getOrAddMetric(scope: String): T = {
      cacheMetrics.get(scope) match {
        case Some(metric) => metric
        case None => {
          val metric = newMetric(scope)
          cacheMetrics += scope -> metric
          metric
        }
      }
    }
  }

  class TableGauges[T: Numeric](name: String) {
    private var cacheGauges: Map[String, Function0[T]] = Map()

    def addTable(table: Table, value: => T): Unit = {
      addGauge(table.getTopLevelTable.name, value)
      metrics.gauge(name, CacheMetrics.TotalScope) { cacheGauges.values.map(_()).sum }
    }

    def reset() = {
      removeGauge(CacheMetrics.TotalScope)
      cacheGauges.keys.foreach(removeGauge)
      cacheGauges = Map()
    }

    private def addGauge(scope: String, value: => T): Unit = {
      metrics.gauge(name, scope) { value }
      cacheGauges += scope -> (() => value)
    }

    private def removeGauge(scope: String) = metricsRegistry.removeMetric(metrics.klass, name, scope)
  }

}

object CacheMetrics {
  val TotalScope = "total"
}
