package com.wajam.mry.api

import com.wajam.mry.execution.{Transaction, Value}

case class Transport(request: Option[Transaction], response: Seq[Value])
