package net.gonzberg.spark.sorting

import org.apache.spark.HashPartitioner
import org.scalatest.funsuite.AnyFunSuite

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
      assert((keys, keys.tail).zipped.forall(_ <= _))
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
      assert((partitionVector, partitionVector.tail).zipped.forall(_ <= _))
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
      assert((partitionVectorWithOnlySortedByValues, partitionVectorWithOnlySortedByValues.tail).zipped.forall(_ <= _))
    }
  }

  test("modifyResourcePreparationAndOp combines two functions into a curried function") {
    val prepareResource = (resource: Map[String, Int]) => resource.mapValues(v => -v)
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

  test("joinAndApply fails when a resource is missing for a key") {
    val operation = (resource: Map[String, Int]) => (value: String) => resource(value)
    val values = Vector("key1" -> Iterator("lookup1", "lookup2"), "key2" -> Iterator("lookup3", "lookup4"))
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndApply(operation)(Iterator("key1" -> Map("lookup1" -> 1, "lookup2" -> 2)), values.toIterator).foreach(identity)
    }
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndApply(operation)(Iterator("key2" -> Map("lookup3" -> 3, "lookup4" -> 4)), values.toIterator).foreach(identity)
    }
  }

  test("joinAndApply failes when a value is missing for a key") {
    val operation = (resource: Map[String, Int]) => (value: String) => resource(value)
    val resources = Vector("key1" -> Map("lookup1" -> 1, "lookup2" -> 2), "key2" -> Map("lookup3" -> 3, "lookup4" -> 4))
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndApply(operation)(resources.toIterator, Iterator("key1" -> Iterator("lookup1", "lookup2"))).foreach(identity)
    }
    assertThrows[IllegalArgumentException] {
      SortHelpers.joinAndApply(operation)(resources.toIterator, Iterator("key2" -> Iterator("lookup3", "lookup4"))).foreach(identity)
    }
  }
}