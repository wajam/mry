package com.wajam.mry.storage.mysql

import com.wajam.nrv.service.TokenRange
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.{TaskData, TaskContext}
import com.wajam.spnl.TaskContext.ContextData

trait RecordDataFeeder extends Feeder {

  type DataRecord

  def toRecord(data: TaskData): Option[DataRecord]

  def fromRecord(record: DataRecord): TaskData
}

trait ResumableRecordDataFeeder extends RecordDataFeeder {

  def context: TaskContext

  def token(record: DataRecord): Long

  def loadRecords(range: TokenRange, startAfterRecord: Option[DataRecord]): Iterable[DataRecord]

  def toContextData(data: TaskData): ContextData = data.values + ("token" -> data.token)

  def fromContextData(data: ContextData): TaskData = TaskData(data("token").toString.toLong, values = data - "token")

  def ack(data: TaskData) {
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
