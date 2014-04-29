package com.wajam.mry.storage.mysql

import com.github.nscala_time.time.Imports._
import org.joda.time.format.ISODateTimeFormat
import com.wajam.nrv.service.TokenRange
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.TaskContext

/**
 * Feeder trait which iterate over table records sequentially per range of tokens. When started it resume from the last
 * acknowledged record saved in the task context. When it finish, it stops.
 */
trait TableOnceFeeder extends TableFeederByToken with Job {
  import TableOnceFeeder._

  private def parseOptDate(date: Option[Any]) = date.map(str => dateFormatter.parseDateTime(str.asInstanceOf[String]))
  private def getStartDate = parseOptDate(context.data.get(StartDate))
  private def getFinishedDate = parseOptDate(context.data.get(FinishedDate))
  private[this] def getLatestStartDate = parseOptDate(contextDataToUpdate.get(StartDate))
  private[this] def getLatestFinishedDate = parseOptDate(contextDataToUpdate.get(FinishedDate))
  private def getJobId = context.data.get(FinishedDate).map(_.asInstanceOf[String])

  private[this] object Lock
  private[this] var contextDataToUpdate = Map.empty[String, String]

  def dateFormatter = ISODateTimeFormat.dateTime()

  private val dummyRange = TokenRange(1, 0) // start is after end
  private def emptyResult = (dummyRange, lastRecord)

  private def updateContext(values: Map[String, String]): Unit = Lock.synchronized {
    val updated = contextDataToUpdate ++ values
    contextDataToUpdate = updated
    updated
  }

  def start(newJobId: String): Boolean = Lock.synchronized {
    if (!isProcessing) {
      updateContext(Map(StartDate -> dateFormatter.print(DateTime.now), JobId -> newJobId))
      true
    } else false
  }

  def stop(): Boolean = Lock.synchronized {
    if (isProcessing) {
      updateContext(Map(FinishedDate -> dateFormatter.print(DateTime.now)))
      lastRecord = None
      currentRange = None
      true
    } else false
  }

  def currentJobId: String = Lock.synchronized(name + getJobId)

  def isProcessing: Boolean = Lock.synchronized {
    // Finished date, if defined, is older than start date
    (getLatestStartDate orElse getStartDate) match {
      case Some(date) => (getLatestFinishedDate orElse getFinishedDate).forall(_ < date)
      case None => false
    }
  }

  abstract override def toContextData(data: Feeder.FeederData): TaskContext.ContextData = super.toContextData(data) ++
    Map(StartDate -> getStartDate, FinishedDate -> getFinishedDate, JobId -> getJobId)

  abstract override def fromRecord(record: DataRecord): Feeder.FeederData = {
    super.fromRecord(record) ++ Map("job_id" -> getJobId)
  }

  private[mysql] def getLoadPosition: (TokenRange, Option[DataRecord]) = Lock.synchronized {
    context.data ++= contextDataToUpdate
    contextDataToUpdate = Map.empty
    if (isProcessing) {
      val loadRange: Option[TokenRange] = lastRecord match {
        case Some(record) => tokenRanges.find(token(record))
        case None => currentRange match {
          case Some(range) if range == dummyRange => Some(tokenRanges.head)
          case Some(range) => tokenRanges.next(range) orElse {
            stop()
            context.data ++= contextDataToUpdate
            None
          }
          case None => None
        }
      }

      contextDataToUpdate = Map.empty

      loadRange match {
        case Some(range) => (range, lastRecord)
        case None if isProcessing => (tokenRanges.head, None)
        case None => emptyResult
      }
    } else emptyResult
  }
}

object TableOnceFeeder {
  private val StartDate = "start-date"
  private val FinishedDate = "finished-date"
  private val JobId = "job-id"
}
