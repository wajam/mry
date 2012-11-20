package com.wajam.mry.storage.mysql

import com.wajam.nrv.Logging
import com.wajam.spnl.feeder.CachedDataFeeder
import com.wajam.spnl.TaskContext
import com.wajam.scn.Timestamp

/**
 * Table mutation timeline task feeder
 */
class TableTimelineFeeder(storage: MysqlStorage, table: Table) extends CachedDataFeeder with Logging {
  val BATCH_SIZE = 100
  val WAIT_BATCH_NO_RESULT = 100 // wait for 100 ms before loading next batch if there were no result

  var context: TaskContext = null

  var lastElement: Option[(Timestamp, Seq[String])] = None

  def init(context: TaskContext) {
    this.context = context
  }

  def loadMore() = {
    debug("Getting new batch for table {}", table.depthName("_"))

    // TODO: should make sure we don't call too often

    // get starting timestamp
    val timestampCursor: Timestamp = lastElement match {
      case Some((ts, keys)) =>
        context.data += ("from_timestamp" -> ts.value.toString)
        ts
      case None =>
        Timestamp(context.data.get("from_timestamp").getOrElse("0").toLong)
    }

    var transaction: MysqlTransaction = null
    try {
      transaction = storage.createStorageTransaction
      debug("Getting timeline from timestamp {}", timestampCursor)
      var mutations = transaction.getTimeline(table, timestampCursor, BATCH_SIZE)

      // filter out already processed records
      if (this.lastElement.isDefined) {
        val (ts, keys) = lastElement.get
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
        this.context.data += ("from_timestamp" -> (timestampCursor.value + 1).toString)
        this.lastElement = None
      }

      mutations map (mr => Map(
        "keys" -> mr.accessPath.keys,
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
  }

  override def next(): Option[Map[String, Any]] = {
    val optElem = super.next()
    for (elem <- optElem) {
      this.lastElement = Some((elem("new_timestamp").asInstanceOf[Timestamp], elem("keys").asInstanceOf[Seq[String]]))
    }

    optElem
  }

  def kill() {

  }

}
