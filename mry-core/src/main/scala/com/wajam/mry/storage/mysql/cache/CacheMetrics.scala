package com.wajam.mry.storage.mysql.cache

import com.yammer.metrics.scala.Instrumented
import com.wajam.mry.storage.mysql.Table
import com.yammer.metrics.core.{Gauge, MetricName}

trait CacheMetrics extends Instrumented {
  lazy val hitsMeter = new TableMetrics(ancestor = false, scope => metrics.meter("hits", "hits", scope))
  lazy val missesMeter = new TableMetrics(ancestor = false, scope => metrics.meter("misses", "misses", scope))

  lazy val evictionExpiredCounter = new TableMetrics(ancestor = true, scope => metrics.counter("eviction-expired", scope))
  lazy val evictionSizeCounter = new TableMetrics(ancestor = true, scope => metrics.counter("eviction-size", scope))

  val cacheCurrentSizeGauge = new TableGauges[Int]("cache-current-size")
  val cacheMaxSizeGauge = new TableGauges[Int]("cache-max-size")

  protected def resetMetrics(): Unit = {
    cacheCurrentSizeGauge.reset()
    cacheMaxSizeGauge.reset()
  }

  class TableMetrics[T](ancestor: Boolean, newMetric: (String) => T) {
    private var cacheMetrics: Map[String, T] = Map()

    def apply(table: Table): Seq[T] = {
      val totalMetric = getOrAddMetric(CacheMetrics.TotalScope)
      val tableMetric = if (ancestor) getOrAddMetric(table.getTopLevelTable.name)  else getOrAddMetric(table.toString)
      Seq(totalMetric, tableMetric)
    }

    def get(scope: String): Option[T] = cacheMetrics.get(scope)

    private def getOrAddMetric(scope: String): T = {
      cacheMetrics.get(scope).getOrElse {
        val metric = newMetric(scope)
        cacheMetrics += scope -> metric
        metric
      }
    }
  }

  class TableGauges[T: Numeric](name: String) {
    private var cacheGauges: Map[String, () => T] = Map()

    def addTable(table: Table, value: => T): Unit = {
      addGauge(table.getTopLevelTable.name, value)
      metrics.gauge(name, CacheMetrics.TotalScope) { cacheGauges.values.map(_()).sum }
    }

    def get(scope: String): Option[Gauge[T]] = {
      val metric = Option(metricsRegistry.allMetrics().get(new MetricName(metrics.klass, name, scope)))
      metric.collect{ case gauge: Gauge[T] => gauge}
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
