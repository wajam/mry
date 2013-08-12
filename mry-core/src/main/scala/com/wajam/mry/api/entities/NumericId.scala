package com.wajam.mry.api.entities

trait NumericId extends MryEntity {
  val id = Field[Long](this, "id")

  def key = id.get.toString
}




