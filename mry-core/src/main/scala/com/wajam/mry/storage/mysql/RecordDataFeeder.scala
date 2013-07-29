package com.wajam.mry.storage.mysql

import com.wajam.nrv.service.TokenRange
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.TaskContext

trait RecordDataFeeder extends Feeder {

  type DataRecord

  def toRecord(data: Map[String, Any]): Option[DataRecord]

  def fromRecord(record: DataRecord): Map[String, Any]
}

trait ResumableRecordDataFeeder extends RecordDataFeeder {

  def context: TaskContext

  def token(record: DataRecord): Long

  def loadRecords(range: TokenRange, fromRecord: Option[DataRecord]): Iterable[DataRecord]

  def toContextData(data: Map[String, Any]): Map[String, Any] = data

  def ack(data: Map[String, Any]) {
    // Update context with the latest acknowledged record data
    context.data = toContextData(data).map(entry => entry match {
      case (k, v: String) => (k, v)
      case (k, v: Seq[Any]) => (k, v.map(_.toString))
      case (k, v) => (k, v.toString)
    })
  }

}
