package com.wajam.mry.storage.mysql

import com.wajam.nrv.service.{TokenRangeSeq, TokenRange}
import com.wajam.spnl.feeder.CachedDataFeeder
import com.wajam.nrv.Logging
import com.wajam.spnl.TaskContext

/**
 * Feeder trait which iterate over table records sequentially per range of tokens. When started it resume from the last
 * acknowledged record saved in the task context. When it finish, it loops over and starts again with the first
 * range of data.
 */
trait TableContinuousFeeder extends CachedDataFeeder with ResumableRecordDataFeeder with Logging {

  private[mysql] lazy val completedCount = metrics.counter("completed-count", name)

  private var currentContext: TaskContext = null
  private var lastRecord: Option[DataRecord] = None
  private var currentRange: Option[TokenRange] = None

  def tokenRanges: TokenRangeSeq

  def context: TaskContext = currentContext

  def init(context: TaskContext) {
    currentContext = context
    lastRecord = toRecord(context.data)
  }

  def loadMore() = {
    try {
      val (loadRange, startRecord) = getLoadPosition

      // Filter out the "from" records in case it is returned by the load method
      val records = filterStartRecord(loadRecords(loadRange, startRecord), startRecord)

      currentRange = Some(loadRange)
      lastRecord = records.lastOption

      records.map(fromRecord).toList
    } catch {
      case e: Exception => {
        error("An exception occured while loading more elements for {}", name, e)
        Nil
      }
    }
  }

  private def getLoadPosition: (TokenRange, Option[DataRecord]) = {
    val loadRange = lastRecord match {
      case Some(record) => tokenRanges.find(token(record))
      case None => {
        currentRange match {
          case Some(range) => tokenRanges.next(range)
          case None => None
        }
      }
    }

    loadRange match {
      case Some(range) => (range, lastRecord)
      case None => {
        completedCount += 1
        (tokenRanges.head, None)
      }
    }
  }

  def kill() {}
}
