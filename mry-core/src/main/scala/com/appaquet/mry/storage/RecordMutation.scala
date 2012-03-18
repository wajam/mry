package com.appaquet.mry.storage

/**
 * Represents a mutation that has been executed on a record of a storage
 */
class RecordMutation(var newRecord: Record, var oldRecord: Option[Record] = None) {

}
