package com.wajam.mry.storage.mysql

import com.wajam.nrv.Logging
import com.wajam.spnl.feeder.CachedDataFeeder
import com.wajam.spnl.TaskContext
import com.wajam.scn.Timestamp
import com.wajam.mry.storage.mysql.TimelineSelectMode.{FromTimestamp, AtTimestamp}
import com.wajam.nrv.utils.CurrentTime

/**
 * Table mutation timeline task feeder
 */
class TableTimelineFeeder(storage: MysqlStorage, table: Table, emptyThrottlePeriod: Int = 1000)
  extends CachedDataFeeder with CurrentTime with Logging {
  val BATCH_SIZE = 100

  var context: TaskContext = null

  var lastEmptyTimestamp = currentTime
  var currentTimestamps: List[(Timestamp, Seq[String])] = Nil
  var lastElement: Option[(Timestamp, Seq[String])] = None

  def init(context: TaskContext) {
    this.context = context
  }

  def loadMore() = {
    if (lastEmptyTimestamp + emptyThrottlePeriod < currentTime) {
      debug("Getting new batch for table {}", table.depthName("_"))

      // get starting timestamp
      val timestampCursor: Timestamp = lastElement match {
        case Some((ts, keys)) => ts
        case None => Timestamp(context.data.get("from_timestamp").getOrElse("0").toLong)
      }

      var transaction: MysqlTransaction = null
      try {
        transaction = storage.createStorageTransaction
        debug("Getting timeline from timestamp {}", timestampCursor)
        var mutations = transaction.getTimeline(table, timestampCursor, BATCH_SIZE, FromTimestamp)

        // filter out already processed records
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

        if (mutations.isEmpty) {
          // TODO: Continue to increment?
          this.context.data += ("from_timestamp" -> (timestampCursor.value + 1).toString)
          this.lastElement = None
          lastEmptyTimestamp = currentTime
        } else if (mutations.size > 1 && mutations.head.newTimestamp == mutations.last.newTimestamp) {
          // The whole batch has the same timestamp and has thus been commited together in the same transaction.
          // Relead everything at that timestamp in case some records where drop by the load size limit.
          mutations = transaction.getTimeline(table, timestampCursor, 0, AtTimestamp)
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
          Seq()
      } finally {
        if (transaction != null)
          transaction.commit()
      }
    } else {
      Seq()
    }
  }

  override def next(): Option[Map[String, Any]] = {
    val optElem = super.next()
    for (elem <- optElem) {
      val (timestamp, keys) = getTimestampKey(elem)
      this.lastElement = Some((timestamp, keys))
      currentTimestamps ::=(timestamp, keys)
    }

    optElem
  }

  def ack(data: Map[String, Any]) {
    currentTimestamps = currentTimestamps filterNot (_ != getTimestampKey(data))
    if (currentTimestamps.isEmpty) {
      for ((ts, _) <- this.lastElement) context.data += ("from_timestamp" -> ts.value.toString)
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
