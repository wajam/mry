package com.wajam.mry.storage.mysql

import com.wajam.nrv.service.TokenRange
import com.wajam.spnl.feeder.CachedDataFeeder
import com.wajam.nrv.Logging
import com.wajam.spnl.TaskContext

trait TableContinuousFeeder extends CachedDataFeeder with ResumableRecordDataFeeder with Logging {

  private var currentContext: TaskContext = null
  private var lastRecord: Option[DataRecord] = None
  private var currentRange: Option[TokenRange] = None

  def tokenRanges: Seq[TokenRange]

  def context: TaskContext = currentContext

  def init(context: TaskContext) {
    currentContext = context
    lastRecord = toRecord(context.data)
  }

  def loadMore() = {
    try {
      val loadRange = lastRecord match {
        case Some(record) => tokenRanges.find(_.contains(token(record))).getOrElse(tokenRanges.head)
        case None => {
          currentRange match {
            case Some(range) => range.nextRange(tokenRanges).getOrElse(tokenRanges.head)
            case None => tokenRanges.head
          }
        }
      }
      currentRange = Some(loadRange)

      // The loadRecords method may returns the "from" record, we remove it first.
      val records = lastRecord match {
        case Some(record) => loadRecords(loadRange, lastRecord).filterNot(_ == record)
        case None => loadRecords(loadRange, lastRecord)
      }
      lastRecord = records.lastOption

      records.map(toData).toList
    } catch {
      case e: Exception => {
        error("An exception occured while loading more elements for {}", name, e)
        Nil
      }
    }
  }

  def kill() {}
}
