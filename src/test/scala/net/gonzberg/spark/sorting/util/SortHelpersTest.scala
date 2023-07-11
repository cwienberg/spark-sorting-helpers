package net.gonzberg.spark.sorting.util

import net.gonzberg.spark.sorting.SparkTestingMixin
import org.apache.spark.HashPartitioner
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.compat._
import scala.math.Ordered.orderingToOrdered

class SortHelpersTest extends AnyFunSuite with SparkTestingMixin {
  test("repartitionAndSort sorts keys within partitions") {
    val rdd = sc.parallelize(Seq(
      1 -> 10, 2 -> 9, 3 -> 8, 4 -> 7, 5 -> 6,
      1 -> 5, 2 -> 4, 3 -> 3, 4 -> 2, 5 -> 1
    ), 1)
    val repartitionedRDD = SortHelpers.repartitionAndSort(rdd, new HashPartitioner(1))
    repartitionedRDD.foreachPartition { partition =>
      val keys = partition.map(_._1).toVector
      assert(keys.lazyZip(keys.tail).forall(_ <= _))
    }
  }

  test("repartitionAndSort sorts values within keysets") {
    val rdd = sc.parallelize(Seq(
      1 -> 10, 2 -> 9, 3 -> 8, 4 -> 7, 5 -> 6,
      1 -> 5, 2 -> 4, 3 -> 3, 4 -> 2, 5 -> 1
    ), 1)
    val repartitionedRDD = SortHelpers.repartitionAndSort(rdd, new HashPartitioner(1))
    repartitionedRDD.foreachPartition { partition =>
      val partitionVector = partition.toVector
      assert(partitionVector.lazyZip(partitionVector.tail).forall(_ <= _))
    }
  }

  test("repartitionAndSort sorts values within keysets by function") {
    val rdd = sc.parallelize(Seq(
      1 -> (10, "z"), 2 -> (9, "y"), 3 -> (8, "x"), 4 -> (7, "w"), 5 -> (6, "v"),
      1 -> (5, "u"), 2 -> (4, "t"), 3 -> (3, "s"), 4 -> (2, "r"), 5 -> (1, "q")
    ), 1)
    val repartitionedRDD = SortHelpers.repartitionAndSort(rdd, (p: (Int, String)) => p._2, new HashPartitioner(1))
    repartitionedRDD.foreachPartition { partition =>
      val partitionVectorWithOnlySortedByValues = partition.map {case (key, (_, sortedValue)) => (key, sortedValue)}.toVector
      assert(partitionVectorWithOnlySortedByValues.lazyZip(partitionVectorWithOnlySortedByValues.tail).forall(_ <= _))
    }
  }

  test("modifyResourcePreparationAndOp combines two functions into a curried function") {
    val prepareResource = (resource: Map[String, Int]) => resource.transform((_, v) => -v)
    val op = (resource: Map[String, Int], value: String) => resource(value)
    val preparedOp = SortHelpers.modifyResourcePreparationAndOp(prepareResource, op)
    assert(preparedOp(Map("test" -> 4))("test") == -4)
  }

  test("joinAndApply joins properly and applies op") {
    val operation = (resource: Map[String, Int]) => (value: String) => resource(value)
    val resources = Iterator("key1" -> Map("lookup1" -> 1, "lookup2" -> 2), "key2" -> Map("lookup3" -> 3, "lookup4" -> 4))
    val values = Iterator("key1" -> Iterator("lookup1", "lookup2"), "key2" -> Iterator("lookup3", "lookup4"))
    val actual = SortHelpers.joinAndApply(operation)(resources, values).toVector
    val expected = Vector(
      "key1" -> 1,
      "key1" -> 2,
      "key2" -> 3,
      "key2" -> 4
    )
    assert(actual == expected)
  }

  test("joinAndApply fails when there are more than one start values for a key") {
    val operation = (resource: Map[String, Int]) => (value: String) => resource(value)
    val resources = Iterator("key1" -> Map("lookup1" -> 1, "lookup2" -> 2), "key1" -> Map.empty[String, Int], "key2" -> Map("lookup3" -> 3, "lookup4" -> 4))
    val values = Iterator("key1" -> Iterator("lookup1", "lookup2"), "key2" -> Iterator("lookup3", "lookup4"))
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndApply(operation)(resources, values).foreach(identity)
    }
  }

  test("joinAndApply fails when a resource is missing for a key") {
    val operation = (resource: Map[String, Int]) => (value: String) => resource(value)
    val values = Vector("key1" -> Iterator("lookup1", "lookup2"), "key2" -> Iterator("lookup3", "lookup4"))
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndApply(operation)(Iterator("key1" -> Map("lookup1" -> 1, "lookup2" -> 2)), values.iterator).foreach(identity)
    }
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndApply(operation)(Iterator("key2" -> Map("lookup3" -> 3, "lookup4" -> 4)), values.iterator).foreach(identity)
    }
  }

  test("joinAndApply fails when a value is missing for a key") {
    val operation = (resource: Map[String, Int]) => (value: String) => resource(value)
    val resources = Vector("key1" -> Map("lookup1" -> 1, "lookup2" -> 2), "key2" -> Map("lookup3" -> 3, "lookup4" -> 4))
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndApply(operation)(resources.iterator, Iterator("key1" -> Iterator("lookup1", "lookup2"))).foreach(identity)
    }
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndApply(operation)(resources.iterator, Iterator("key2" -> Iterator("lookup3", "lookup4"))).foreach(identity)
    }
  }

  test("joinAndFold joins properly and folds with op") {
    val operation = (start: Double, next: Int) => start + next
    val startValues = Iterator("key1" -> 1.0, "key2" -> 2.0)
    val values = Iterator("key1" -> Iterator(4, 2, -10), "key2" -> Iterator(-6))
    val actual = SortHelpers.joinAndFold(operation)(startValues, values).toVector
    val expected = Vector(
      "key1" -> -3.0,
      "key2" -> -4.0
    )
    assert(actual == expected)
  }

  test("joinAndFold fails when there are more than one start values for a key") {
    val operation = (start: Double, next: Int) => start + next
    val startValues = Iterator("key1" -> 1.0, "key1" -> 5.0, "key2" -> 2.0)
    val values = Iterator("key1" -> Iterator(4, 2, -10), "key2" -> Iterator(-6))
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndFold(operation)(startValues, values).foreach(identity)
    }
  }

  test("joinAndFold fails when a start value is missing for a key") {
    val operation = (start: Double, next: Int) => start + next
    val values = Vector("key1" -> Iterator(4, 2, -10), "key2" -> Iterator(-6))
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndFold(operation)(Iterator("key1" -> 1.0), values.iterator).foreach(identity)
    }
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndFold(operation)(Iterator("key2" -> 2.0), values.iterator).foreach(identity)
    }
  }

  test("joinAndFold succeeds when a value is missing for a key") {
    val operation = (start: Double, next: Int) => start + next
    val startValues = Vector("key1" -> 1.0, "key2" -> 2.0)
    assert(
      SortHelpers.joinAndFold(operation)(startValues.iterator, Iterator("key1" -> Iterator(4, 2, -10))).toVector ==
        Seq("key1" -> -3.0, "key2" -> 2.0)
    )
    assert(
      SortHelpers.joinAndFold(operation)(startValues.iterator, Iterator("key2" -> Iterator(-6))).toVector ==
        Seq("key1" -> 1.0, "key2" -> -4.0)
    )
  }

  test("joinAndFold fails when value groupings are duplicated") {
    val operation = (start: Double, next: Int) => start + next
    val startValues = Iterator("key1" -> 1.0, "key2" -> 2.0)
    val values = Iterator("key1" -> Iterator(4, 2, -10), "key1" -> Iterator(4, 2, -10), "key2" -> Iterator(-6))
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndFold(operation)(startValues, values).foreach(_ => ())
    }
  }

  test("joinAndFold fails when values or start values are out of order") {
    val operation = (start: Double, next: Int) => start + next
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndFold(operation)(
        Iterator("key2" -> 2.0, "key1" -> 1.0),
        Iterator("key1" -> Iterator(4, 2, -10), "key2" -> Iterator(-6))).foreach(_ => ()
      )
    }
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndFold(operation)(
        Iterator("key1" -> 1.0, "key2" -> 2.0),
        Iterator("key2" -> Iterator(-6), "key1" -> Iterator(4, 2, -10))).foreach(_ => ()
      )
    }
  }
}
