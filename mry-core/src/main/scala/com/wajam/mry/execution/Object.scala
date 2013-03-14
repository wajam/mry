package com.wajam.mry.execution

import com.wajam.nrv.utils.ContentEquals

/**
 * Object of a transaction (variable or value)
 */
trait Object extends ContentEquals {
  def value: Value
}
