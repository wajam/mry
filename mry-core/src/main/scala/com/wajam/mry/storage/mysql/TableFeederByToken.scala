package com.wajam.mry.storage.mysql

import com.wajam.commons.Logging
import com.wajam.nrv.service.{TokenRange, TokenRangeSeq}
import com.wajam.spnl.feeder.CachedDataFeeder
import com.wajam.spnl.TaskContext

/**
 * Feeder trait which ease creation of feeders who iterate over table records sequentially per range of tokens.
 * When started it resume from the last acknowledged record saved in the task context.
 */
private[mysql] trait TableFeederByToken extends CachedDataFeeder with ResumableRecordDataFeeder with Logging {

  private var currentContext: TaskContext = null
  private[mysql] var lastRecord: Option[DataRecord] = None
  private[mysql] var currentRange: Option[TokenRange] = None

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

  private[mysql] def getLoadPosition: (TokenRange, Option[DataRecord])

  def kill() {}
}
