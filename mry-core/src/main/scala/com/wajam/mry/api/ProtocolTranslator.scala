package com.wajam.mry.api

import com.wajam.mry.execution.{Value, Transaction}


/**
 * MysqlTransaction translators, used to translate protocol specific transactions to
 * mry objects
 */
trait ProtocolTranslator {
  def encodeValue(value: Value): Array[Byte]

  def decodeValue(data: Array[Byte]): Value

  def encodeTransaction(transaction: Transaction): Array[Byte]

  def decodeTransaction(data: Array[Byte]): Transaction

  def encodeAll(transport: Transport): Array[Byte]

  def decodeAll(data: Array[Byte]): Transport
}
