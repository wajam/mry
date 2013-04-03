package com.wajam.mry.api

import com.wajam.mry.execution.{Transaction, Value}

case class Transport(empty: Boolean, request: Option[Transaction], response: Option[Seq[Value]])
