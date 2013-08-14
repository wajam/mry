package com.wajam.mry.entity

trait NumericId extends Entity {
  val id = Field[Long](this, "id")

  def key = id.get.toString
}




