package com.wajam.mry.entities

trait NumericId extends Entity {
  val id = Field[Long](this, "id")

  def key = id.get.toString
}




