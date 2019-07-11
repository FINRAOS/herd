package org.apache.spark.sql.herd

import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FileIndexInternalTestsSuite extends FunSuite {

  test("Regex should match _committed_XXX filename") {
    val fileName = "_committed_6680527761926262645"
    val committedRegex = "(^_committed_.*$)"

    val foundMatch = fileName.matches(committedRegex)
    assertTrue(foundMatch)
  }
}
