package com.gaston.pocs

import com.gaston.pocs.utils.JobUtils
import munit.FunSuite

class JobUtilsTests extends FunSuite {
  test("generateIntegerKey for Station USW00093736") {
    val expectedIntegerKey = scala.util.hashing.MurmurHash3.stringHash("USW00093736").abs.toString
    val actualIntegerKey = JobUtils.generateIntegerKey("USW00093736")
    assertEquals(actualIntegerKey, expectedIntegerKey)
  }

  test("generateIntegerKey with null") {
    val actualIntegerKey = JobUtils.generateIntegerKey(null)
    assert(actualIntegerKey == null)
  }
}
