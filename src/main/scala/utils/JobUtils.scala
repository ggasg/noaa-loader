package com.gaston.pocs
package utils

object JobUtils {
  def generateIntegerKey(fromCol: String): String = {
    if (fromCol != null)
      scala.util.hashing.MurmurHash3.stringHash(fromCol).abs.toString
    else
      null
  }

}
