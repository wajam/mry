package com.wajam.mry.storage

import com.wajam.mry.model.Table
import com.wajam.mry.execution.Timestamp

/**
 * Storage operations transactions with ACID properties
 */
abstract class StorageTransaction {
  /*
  Set(table *Table, keys []string, data []byte) error
    Get(table *Table, keys []string) (*Row, error)
  GetQuery(query Query) (RowIterator, error)
  GetTimeline(table *Table, from time.Time, count int) ([]RowMutation, error)
  Rollback() error
    Commit() error
  */

  def set(table: Table, keys: Seq[String], record: Record)

  def get(table: Table, keys: Seq[String]): Option[Record]

  def query(query: Query): Result[Record]

  def timeline(table: Table, from: Timestamp): Result[RecordMutation]

  def rollback()

  def commit()
}
