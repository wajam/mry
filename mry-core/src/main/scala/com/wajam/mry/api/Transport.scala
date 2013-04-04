package com.wajam.mry.api

import com.wajam.mry.execution.{Transaction, Value}

case class Transport(transaction: Option[Transaction], values: Option[Seq[Value]])
