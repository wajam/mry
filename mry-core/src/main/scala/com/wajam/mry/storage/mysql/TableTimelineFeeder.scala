package com.wajam.mry.storage.mysql

import com.wajam.nrv.Logging
import com.wajam.spnl.feeder.CachedDataFeeder
import com.wajam.spnl.{TaskData, TaskContext}
import com.wajam.mry.storage.mysql.TimelineSelectMode.{FromTimestamp, AtTimestamp}
import com.wajam.nrv.service.TokenRange
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Table mutation timeline task feeder
 */
class TableTimelineFeeder(val name: String, storage: MysqlStorage, table: Table, tokenRanges: List[TokenRange],
                          val batchSize: Int = 100)
  extends CachedDataFeeder with Logging {

  private val contextTimestampGauge = metrics.gauge("context-timestamp", name) {
    val contexts = TimelineFeederContextCache.contexts(name)
    if (contexts.isEmpty) 0 else contexts.map(_.data.getOrElse("from_timestamp", 0).toString.toLong).min
  }

  var context: TaskContext = null
  var currentTimestamps: List[(Timestamp, Seq[String])] = Nil
  var lastElement: Option[(Timestamp, Seq[String])] = None
  var lastSelectMode: TimelineSelectMode = FromTimestamp

  def init(context: TaskContext) {
    this.context = context
    TimelineFeederContextCache.register(name, context)
  }

  def loadMore() = {
    debug("Getting new batch for table {}", table.depthName("_"))

    // get starting timestamp
    val timestampCursor: Timestamp = lastElement match {
      case Some((ts, keys)) => {
        if (lastSelectMode == AtTimestamp)
        // All records loaded in the previous call where at the same timestamp.
        // Increment the last record timestamp to not load the same records forever.
          Timestamp(ts.value + 1)
        else ts
      }
      case None => {
        Timestamp(context.data.get("from_timestamp").getOrElse("0").toString.toLong)
      }
    }

    var transaction: MysqlTransaction = null
    try {
      transaction = storage.createStorageTransaction
      var mutations = transaction.getTimeline(table, timestampCursor, batchSize, tokenRanges, FromTimestamp)

      if (mutations.size > 1 && mutations.head.newTimestamp == mutations.last.newTimestamp) {
        // The whole batch has the same timestamp and has thus been inserted together in the same transaction.
        // Reload everything at that timestamp in case some records where drop by the batch size limit.
        mutations = transaction.getTimeline(table, mutations.head.newTimestamp, 0, tokenRanges, AtTimestamp)
        lastSelectMode = AtTimestamp
      } else if (!mutations.isEmpty) {
        lastSelectMode = FromTimestamp

        // Filter out already processed records
        for ((ts, keys) <- lastElement) {
          val foundLastElement = mutations.find(mut => mut.newTimestamp == ts && mut.accessPath.keys == keys)

          val before = mutations.size
          if (foundLastElement.isDefined) {
            var found = false
            mutations = mutations.filter(mut => {
              if (mut.newTimestamp == ts && mut.accessPath.keys == keys) {
                found = true
                false
              } else {
                found
              }
            })
          }
          val after = mutations.size
          debug("Before {} after {}", before.asInstanceOf[Object], after.asInstanceOf[Object])
        }
      }

      mutations.map { mr =>
        TaskData(
          token = mr.token,
          fields = Map(
            "keys" -> mr.accessPath.keys,
            "old_timestamp" -> mr.oldTimestamp,
            "old_value" -> mr.oldValue,
            "new_timestamp" -> mr.newTimestamp,
            "new_value" -> mr.newValue
          )
        )
      }
    } catch {
      case e: Exception =>
        log.error("An exception occured while loading more elements from table {}", table.depthName("_"), e)
        lastSelectMode = FromTimestamp
        Seq()
    } finally {
      if (transaction != null)
        transaction.commit()
    }
  }

  override def next(): Option[TaskData] = {
    val optElem = super.next()
    for (elem <- optElem) {
      val (timestamp, keys) = getTimestampKey(elem)
      lastElement = Some((timestamp, keys))
      currentTimestamps ::=(timestamp, keys)
    }

    optElem
  }

  def ack(data: TaskData) {
    currentTimestamps = currentTimestamps filter (_ != getTimestampKey(data))
    if (currentTimestamps.isEmpty) {
      for ((ts, _) <- lastElement) context.data += ("from_timestamp" -> ts.value.toString)
    } else {
      context.data += ("from_timestamp" -> currentTimestamps.map(_._1).min.value.toString)
    }
  }

  def kill() {
    TimelineFeederContextCache.unregister(name, context)
  }

  private def getTimestampKey(data: TaskData) = {
    val timestamp = data.fields("new_timestamp").asInstanceOf[Timestamp]
    val keys = data.fields("keys").asInstanceOf[Seq[String]]
    (timestamp, keys)
  }
}

/**
 * Singleton object used to cache task contexts per feeder name for metrics. This object is thread safe.
 */
private[mysql] object TimelineFeederContextCache {

  private object Lock

  private var contextsCache = Map[String, List[TaskContext]]()

  def register(name: String, context: TaskContext) {
    Lock.synchronized {
      val cachedContexts = contextsCache.getOrElse(name, List())
      contextsCache += (name -> (context :: cachedContexts))
    }
  }

  def unregister(name: String, context: TaskContext) {
    Lock.synchronized {
      val cachedContexts = contextsCache.getOrElse(name, List())
      contextsCache += (name -> cachedContexts.filterNot(_ == context))
    }
  }

  /**
   * Returns the list of contexts registered with the specified feeder name
   */
  def contexts(name: String): List[TaskContext] = {
    contextsCache.getOrElse(name, List())
  }
}
