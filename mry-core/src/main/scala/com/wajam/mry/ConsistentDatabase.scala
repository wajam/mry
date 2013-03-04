package com.wajam.mry

import com.wajam.nrv.consistency.ConsistentStore
import com.wajam.nrv.data.Message
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.service.TokenRange
import storage.{Storage, ConsistentStorage}

/**
 * Consistent MRY database
 */
class ConsistentDatabase[T <: ConsistentStorage](serviceName: String = "database")
  extends Database[T](serviceName) with ConsistentStore {

  def requiresConsistency(message: Message) : Boolean = {
    findAction(message.path, message.method) match {
      case Some(action) => {
        action == remoteWriteExecuteToken || action == remoteReadExecuteToken
      }
      case _ => false
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
