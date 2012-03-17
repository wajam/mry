package com.appaquet.mry.execution

/**
 * DESCRIPTION HERE
 *
 * Author: Andre-Philippe Paquet < app@quet.ca >
 */

object Implicits {
  implicit def string2value(value:String) = new ValueString(value)
}
