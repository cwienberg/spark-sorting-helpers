package net.gonzberg.spark.sorting

import org.scalatest.funsuite.AnyFunSuite

class GroupByKeyIteratorTest extends AnyFunSuite {

  test("groups elements as expected") {
    val inputIter = Iterator(
      ("key1", 1),
      ("key1", 2),
      ("key2", 3),
      ("key2", 4),
      ("key2", 5),
      ("key3", 6)
    )
    val groupedIter = new GroupByKeyIterator(inputIter)
    val actual = groupedIter.map { case (k, vs) => k -> vs.toSeq }.toSeq
    val expected =
      Seq("key1" -> Seq(1, 2), "key2" -> Seq(3, 4, 5), "key3" -> Seq(6))
    assert(expected == actual)
  }

  test(
    "calling next and hasNext when you haven't iterated the previous iterator fails"
  ) {
    val inputIter = Iterator(("key1", 1), ("key1", 2), ("key2", 3), ("key2", 4))
    val groupedIter = new GroupByKeyIterator(inputIter)
    groupedIter.next()
    assertThrows[IllegalStateException](groupedIter.hasNext)
    assertThrows[IllegalStateException](groupedIter.next())
  }

  test("can iterate back to a previous group") {
    val inputIter =
      Iterator(("key1", 1), ("key1", 2), ("key2", 3), ("key2", 4), ("key1", 5))
    val groupedIter = new GroupByKeyIterator(inputIter)
    val actual = groupedIter.map { case (k, vs) => k -> vs.toSeq }.toSeq
    val expected =
      Seq("key1" -> Seq(1, 2), "key2" -> Seq(3, 4), "key1" -> Seq(5))
    assert(expected == actual)
  }

  test("a previous iterator cannot resume if its grouping reappears") {
    val inputIter = Iterator(
      ("key1", 1),
      ("key1", 2),
      ("key2", 3),
      ("key2", 4),
      ("key1", 5)
    ).buffered
    val groupedIter = new GroupByKeyIterator(inputIter)
    val (k, key1iter) = groupedIter.next()
    assert("key1" == k)
    while (key1iter.hasNext) key1iter.next()
    val (_, key2iter) = groupedIter.next()
    while (key2iter.hasNext) key2iter.next()
    assert(inputIter.head._1 == "key1", "test improperly set up")
    assert(groupedIter.hasNext, "test improperly set up")
    assert(
      !key1iter.hasNext,
      "key1 iter was exhausted and should not have next"
    )
    assert(groupedIter.next()._2.next() == 5, "test was improperly set up")
  }

  test("next on an empty group iterator throws") {
    val inputIter = Iterator(("key1", 1), ("key1", 2), ("key2", 3), ("key2", 4))
    val groupedIter = new GroupByKeyIterator(inputIter)
    val (_, key1iter) = groupedIter.next()
    while (key1iter.hasNext) key1iter.next()
    assert(!key1iter.hasNext, "test improperly setup")
    assertThrows[NoSuchElementException](key1iter.next())
  }

  test("next on an empty iterator of groups throws") {
    val inputIter = Iterator(("key1", 1), ("key1", 2))
    val groupedIter = new GroupByKeyIterator(inputIter)
    val (_, key1iter) = groupedIter.next()
    while (key1iter.hasNext) key1iter.next()
    assert(!groupedIter.hasNext, "test improperly setup")
    assertThrows[NoSuchElementException](groupedIter.next())
  }

  test("empty returns an empty iterator") {
    assert(GroupByKeyIterator.empty.isEmpty)
  }

}
