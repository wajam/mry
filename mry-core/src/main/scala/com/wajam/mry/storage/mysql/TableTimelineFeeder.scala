package com.wajam.mry.storage.mysql

import collection.mutable.Queue
import com.wajam.nrv.Logging
import com.wajam.mry.execution.Timestamp
import com.wajam.spnl.{TaskContext, Task, Feeder}

/**
 * Table mutation timeline task feeder
 */
class TableTimelineFeeder(storage: MysqlStorage, table: Table) extends Feeder with Logging {
  val BATCH_SIZE = 100
  val WAIT_BATCH_NO_RESULT = 100 // wait for 100 ms before loading next batch if there were no result

  var mutationsCache = new Queue[MutationRecord]()
  var context:TaskContext = null

  var lastElement: Option[(Timestamp, Seq[String])] = None

  def init(context: TaskContext) {
    this.context = context
  }

  def loadMore() {
    debug("Getting new batch for table {}", table.depthName("_"))

    // TODO: should make sure we don't call too often

    // get starting timestamp
    val timestampCursor = lastElement match {
      case Some((ts, keys)) =>
        context.data += ("from_timestamp" -> ts.value.toString)
        ts
      case None =>
        Timestamp(context.data.get("from_timestamp").getOrElse("0").toLong)
    }

    var transaction: MysqlTransaction = null
    try {
      transaction = storage.getStorageTransaction
      debug("Getting timeline from timestamp {}", timestampCursor)
      var mutations = transaction.getTimeline(table, timestampCursor, BATCH_SIZE)

      // filter out already processed records
      if (this.lastElement.isDefined) {
        val (ts, keys) = lastElement.get
        val foundLastElement = mutations.find(mut => mut.newTimestamp == ts && mut.keys == keys)

        val before = mutations.size
        if (foundLastElement.isDefined) {
          var found = false
          mutations = mutations.filter(mut => {
            if (mut.newTimestamp == ts && mut.keys == keys) {
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


      if (!mutations.isEmpty) {
        mutationsCache ++= mutations
      } else {
        this.context.data += ("from_timestamp" -> (timestampCursor.value + 1).toString)
        this.lastElement = None
      }
    } finally {
      if (transaction != null)
        transaction.commit()
    }
  }

  def next(): Option[Map[String, Any]] = {
    if (!mutationsCache.isEmpty) {
      val mr = mutationsCache.dequeue()
      this.lastElement = Some((mr.newTimestamp, mr.keys))

      Some(Map(
        "keys" -> mr.keys,
        "old_timestamp" -> mr.oldTimestamp,
        "old_value" -> mr.oldValue,
        "new_timestamp" -> mr.newTimestamp,
        "new_value" -> mr.newValue
      ))
    } else {
      this.loadMore()
      None
    }
  }

  def kill() {

  }
}
