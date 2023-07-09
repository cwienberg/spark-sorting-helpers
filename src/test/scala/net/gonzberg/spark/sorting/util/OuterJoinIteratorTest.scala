package net.gonzberg.spark.sorting.util

import org.scalatest.funsuite.AnyFunSuite

class OuterJoinIteratorTest extends AnyFunSuite {

  test("all empty input iterators returns an empty iterator") {
    val iter = OuterJoinIterator[String, Any, Any, Any, Any](Iterator.empty, Iterator.empty, Iterator.empty, Iterator.empty)
    assert(!iter.hasNext)
  }

  test("handles empty values for keys") {
    val iter = OuterJoinIterator(Iterator(1->5), Iterator(2->6), Iterator(3->7), Iterator(4->8))
    assert(
      iter.toVector == Vector(
        1 -> (Some(5), None, None, None),
        2 -> (None, Some(6), None, None),
        3 -> (None, None, Some(7), None),
        4 -> (None, None, None, Some(8))
      )
    )
  }

  test("fully joins values with the same key") {
    val iter = OuterJoinIterator(Iterator(1->5), Iterator(1->6), Iterator(1->7), Iterator(1->8))
    assert(
      iter.toVector == Vector(
        1 -> (Some(5), Some(6), Some(7), Some(8))
      )
    )
  }

  test("joins keys with partial values") {
    val iter = OuterJoinIterator(Iterator(1->5), Iterator(2->6), Iterator(1->7, 2->8), Iterator(2->9))
    assert(
      iter.toVector == Vector(
        1 -> (Some(5), None, Some(7), None),
        2 -> (None, Some(6), Some(8), Some(9))
      )
    )
  }

  test("joins keys with repeat in same iterator") {
    val iter = OuterJoinIterator(Iterator(1->5, 1->6), Iterator(1->7), Iterator(1->8), Iterator(1->9))
    assert(
      iter.toVector == Vector(
        1 -> (Some(5), Some(7), Some(8), Some(9)),
        1 -> (Some(6), Some(7), Some(8), Some(9))
      )
    )
  }

  test("next on empty iterator throws") {
    val emptyIter = OuterJoinIterator[Int, Int, Int](Iterator.empty, Iterator.empty)
    assert(emptyIter.isEmpty)
    assertThrows[RuntimeException] {
      emptyIter.next()
    }
  }

  test("repeated or out-or-order keys throws") {
    assertThrows[IllegalStateException] {
      val repeatedKeyJoin = new OuterJoinIterator(
        Iterator("b" -> Iterator(1), "b" -> Iterator(2)),
        Iterator("b" -> Iterator(3)),
        Iterator("c" -> Iterator(4)),
        Iterator("d" -> Iterator(5))
      )
      repeatedKeyJoin.foreach(_ => ())
    }

    assertThrows[IllegalStateException] {
      val outOfOrderJoin = new OuterJoinIterator(
        Iterator("b" -> Iterator(1), "a" -> Iterator(2)),
        Iterator("b" -> Iterator(3)),
        Iterator("c" -> Iterator(4)),
        Iterator("d" -> Iterator(5))
      )
      outOfOrderJoin.foreach(_ => ())
    }
  }

}
