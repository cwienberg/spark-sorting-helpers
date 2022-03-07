package cwienberg.spark.sorting

import org.scalatest.funsuite.AnyFunSuite

class SecondarySortKeyTest extends AnyFunSuite {
  test("test apply from tuple method") {
    val ssk = SecondarySortKey(("key", "value"))
    assert(ssk.key == "key")
    assert(ssk.value == "value")
  }
}
