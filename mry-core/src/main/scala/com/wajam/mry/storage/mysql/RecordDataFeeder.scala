package com.wajam.mry.storage.mysql

import com.wajam.nrv.service.TokenRange
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.TaskContext
import com.wajam.spnl.feeder.Feeder.FeederData
import com.wajam.spnl.TaskContext.ContextData

trait RecordDataFeeder extends Feeder {

  type DataRecord

  def toRecord(data: FeederData): Option[DataRecord]

  def fromRecord(record: DataRecord): FeederData
}

trait ResumableRecordDataFeeder extends RecordDataFeeder {

  def context: TaskContext

  def token(record: DataRecord): Long

  def loadRecords(range: TokenRange, startAfterRecord: Option[DataRecord]): Iterable[DataRecord]

  def toContextData(data: FeederData): ContextData = data

  def ack(data: FeederData) {
    // Update context with the latest acknowledged record data
    context.data = toContextData(data).map(entry => entry match {
      case (k, v: String) => (k, v)
      case (k, v: Seq[Any]) => (k, v.map(_.toString))
      case (k, v) => (k, v.toString)
    })
  }

  /**
   * Filter out the specified start record from the records collection
   */
  protected def filterStartRecord(records: Iterable[DataRecord], startAfterRecord: Option[DataRecord]): Iterable[DataRecord] = {
    startAfterRecord match {
      case Some(startRecord) => records.filter(_ != startRecord)
      case None => records
    }
  }
}
