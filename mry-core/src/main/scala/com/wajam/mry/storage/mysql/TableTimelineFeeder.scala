package com.wajam.mry.storage.mysql

import com.wajam.nrv.Logging
import com.wajam.spnl.feeder.CachedDataFeeder
import com.wajam.spnl.TaskContext
import com.wajam.mry.storage.mysql.TimelineSelectMode.{FromTimestamp, AtTimestamp}
import com.wajam.nrv.utils.CurrentTime
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.service.TokenRange

/**
 * Table mutation timeline task feeder
 */
class TableTimelineFeeder(storage: MysqlStorage, table: Table, tokenRanges: List[TokenRange], val batchSize: Int = 100)
  extends CachedDataFeeder with CurrentTime with Logging {
  var context: TaskContext = null

  var currentTimestamps: List[(Timestamp, Seq[String])] = Nil
  var lastElement: Option[(Timestamp, Seq[String])] = None
  var lastSelectMode: TimelineSelectMode =  FromTimestamp

  def init(context: TaskContext) {
    this.context = context
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
      lastSelectMode = FromTimestamp

      if (mutations.size > 1 && mutations.head.newTimestamp == mutations.last.newTimestamp) {
        // The whole batch has the same timestamp and has thus been inserted together in the same transaction.
        // Reload everything at that timestamp in case some records where drop by the batch size limit.
        mutations = transaction.getTimeline(table, mutations.head.newTimestamp, 0, tokenRanges, AtTimestamp)
        lastSelectMode = AtTimestamp
      } else {
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

      mutations map (mr => Map(
        "keys" -> mr.accessPath.keys,
        "token" -> mr.token.toString,
        "old_timestamp" -> mr.oldTimestamp,
        "old_value" -> mr.oldValue,
        "new_timestamp" -> mr.newTimestamp,
        "new_value" -> mr.newValue
      ))
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

  override def next(): Option[Map[String, Any]] = {
    val optElem = super.next()
    for (elem <- optElem) {
      val (timestamp, keys) = getTimestampKey(elem)
      lastElement = Some((timestamp, keys))
      currentTimestamps ::=(timestamp, keys)
    }

    optElem
  }

  def ack(data: Map[String, Any]) {
    currentTimestamps = currentTimestamps filter (_ != getTimestampKey(data))
    if (currentTimestamps.isEmpty) {
      for ((ts, _) <- lastElement) context.data += ("from_timestamp" -> ts.value.toString)
    } else {
      context.data += ("from_timestamp" -> currentTimestamps.map(_._1).min.value.toString)
    }
  }

  def kill() {}

  private def getTimestampKey(data: Map[String, Any]) = {
    val timestamp = data("new_timestamp").asInstanceOf[Timestamp]
    val keys = data("keys").asInstanceOf[Seq[String]]
    (timestamp, keys)
  }

}
