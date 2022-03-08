package net.gonzberg.spark.sorting

import org.scalatest.funsuite.AnyFunSuite

import scala.math.Ordered.orderingToOrdered

class SecondarySortKeyTest extends AnyFunSuite {
  test("test apply from tuple method") {
    val ssk = SecondarySortKey(("key", "value"))
    assert(ssk.key == "key")
    assert(ssk.value == "value")
  }

  test("test toTuple") {
    val ssk = new SecondarySortKey("key", "value")
    assert(ssk.toTuple == ("key", "value"))
  }

  test("test ordering") {
    val first = new SecondarySortKey("key1", "value1")
    val second = new SecondarySortKey("key1", "value2")
    val third = new SecondarySortKey("key2", "value1")
    assert(first < second)
    assert(second < third)
    assert(first < third)
  }
}
