package com.wajam.mry

import com.wajam.nrv.consistency.{Consistency, ConsistentStore}
import com.wajam.nrv.data.{InMessage, Message}
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.service.TokenRange
import execution.{ExecutionContext, Transaction}
import storage.ConsistentStorage

/**
 * Consistent MRY database
 */
class ConsistentDatabase[T <: ConsistentStorage](serviceName: String = "database")
  extends Database[T](serviceName) with ConsistentStore {

  def requiresConsistency(message: Message): Boolean = {
    findAction(message.path, message.method) match {
      case Some(action) => {
        action == remoteWriteExecuteToken || action == remoteReadExecuteToken
      }
      case _ => false
    }
  }

  /**
   * Returns the latest record timestamp for the specified token ranges
   */
  def getLastTimestamp(ranges: Seq[TokenRange]): Option[Timestamp] = {
    storages.values.map(_.getLastTimestamp(ranges)).max
  }


  /**
   * Returns the mutation messages from the given timestamp inclusively for the specified token ranges.
   */
  def readTransactions(from: Timestamp, to: Timestamp, ranges: Seq[TokenRange]): Iterator[Message] = {
    // TODO: somehow support more than one storage
    val (_, storage) = storages.head

    new Iterator[Message] {
      val itr = storage.readTransactions(from, to, ranges)

      def hasNext = itr.hasNext

      def next() = {
        val value = itr.next()
        val transaction = new Transaction()
        value.applyTo(transaction)
        val message = new InMessage(Map(Database.TOKEN_KEY -> value.token), data = transaction)
        Consistency.setMessageTimestamp(message, value.timestamp)
        message
      }
    }
  }


  /**
   * Apply the specified mutation message to this consistent database
   */
  def writeTransaction(message: Message) {
    val context = new ExecutionContext(storages, Consistency.getMessageTimestamp(message))
    context.cluster = cluster

    try {
      val transaction = message.getData[Transaction]
      transaction.execute(context)
      context.commit()
      transaction.reset()
    } catch {
      case e: Exception => {
        debug("Got an exception executing transaction", e)
        context.rollback()
        throw e
      }
    }
  }

  /**
   * Truncate all records at the given timestamp for the specified token.
   */
  def truncateAt(timestamp: Timestamp, token: Long) {
    for (storage <- storages.values) {
      storage.truncateAt(timestamp, token)
    }
  }

  /**
   * Truncate all records from the given timestamp inclusively for the specified token ranges.
   */
  def truncateFrom(timestamp: Timestamp, tokens: Seq[TokenRange]) {
    throw new Exception("Not implemented!")
  }

}
