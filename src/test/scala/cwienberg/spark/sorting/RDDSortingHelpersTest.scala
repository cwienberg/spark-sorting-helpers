package cwienberg.spark.sorting

import RDDSortingHelpers._
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkException}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.Queue
import scala.util.Random

case class GroupByTestObject(group: String, value: Int)

class RDDSortingHelpersTest
    extends AnyFunSuite
    with Matchers
    with SparkTestingMixin {

  def rand: Random = new Random(47)

  test("creates expected, sorted groups") {
    val input = for {
      key <- Seq(1, 10, 100)
      value <- 0.until(100)
    } yield (key, value * key)
    val rdd = sc.parallelize(rand.shuffle(input), 5)
    val actual = rdd.sortedGroupByKey.collectAsMap()
    assert(actual.size == 3)
    assert(actual.keys.toSet == Set(1, 10, 100))
    actual.values.foreach(v => assert(v.size == 100))
    actual.foreach { case (k, values) =>
      values.zip(0.until(100)).foreach { case (a, b) => a == b * k }
    }
  }

  test("creates expected, sorted groups with a number of partitions") {
    val input = for {
      key <- Seq(1, 10, 100)
      value <- 0.until(100)
    } yield (key, value * key)
    val rdd = sc.parallelize(rand.shuffle(input), 5)
    val actualRDD = rdd.sortedGroupByKey(7).cache()
    val actual = actualRDD.collectAsMap()
    assert(actual.size == 3)
    assert(actual.keys.toSet == Set(1, 10, 100))
    actual.values.foreach(v => assert(v.size == 100))
    actual.foreach { case (k, values) =>
      values.zip(0.until(100)).foreach { case (a, b) => a == b * k }
    }
    assert(actualRDD.getNumPartitions == 7)
    actualRDD.unpersist(false)
  }

  test("creates expected, sorted groups with a given partitioner") {
    val input = for {
      key <- Seq(1, 10, 100)
      value <- 0.until(100)
    } yield (key, value * key)
    val rdd = sc.parallelize(rand.shuffle(input), 5)
    val partitioner = new RangePartitioner[Int, Int](3, rdd, true)
    val actualRDD = rdd.sortedGroupByKey(partitioner).cache()
    val actual = actualRDD.collectAsMap()
    assert(actual.size == 3)
    assert(actual.keys.toSet == Set(1, 10, 100))
    actual.values.foreach(v => assert(v.size == 100))
    actual.foreach { case (k, values) =>
      values.zip(0.until(100)).foreach { case (a, b) => a == b * k }
    }
    assert(actualRDD.getNumPartitions == 3)
    actualRDD.unpersist(false)
  }

  test("creates expected, sorted groups when they're already partitioned") {
    val input = for {
      key <- Seq(1, 10, 100, 1000, 10000)
      value <- 0.until(100)
    } yield (key, value * key)
    val partitioner = new HashPartitioner(3)
    val rdd = sc.parallelize(rand.shuffle(input), 5).partitionBy(partitioner)
    val actual = rdd.sortedGroupByKey(partitioner).collectAsMap()
    assert(actual.size == 5)
    assert(actual.keys.toSet == Set(1, 10, 100, 1000, 10000))
    actual.values.foreach(v => assert(v.size == 100))
    actual.foreach { case (k, values) =>
      values.zip(0.until(100)).foreach { case (a, b) => a == b * k }
    }
  }

  test("sortedFoldLeftByKey applies fold as expected") {
    val input =
      Seq(("key1", 1), ("key1", 2), ("key1", 3), ("key2", 4), ("key2", 5))
    val rdd = sc.parallelize(rand.shuffle(input))
    val actual = rdd
      .sortedFoldLeftByKey(
        Queue.empty[Int],
        (q: Queue[Int], v: Int) => q.enqueue(v)
      )
      .collectAsMap()
    val expected = Map("key1" -> Queue(1, 2, 3), "key2" -> Queue(4, 5))
    assert(expected == actual)
  }

  test(
    "sortedFoldLeftByKey with a number of partitions applies fold as expected"
  ) {
    val input =
      Seq(("key1", 1), ("key1", 2), ("key1", 3), ("key2", 4), ("key2", 5))
    val rdd = sc.parallelize(rand.shuffle(input))
    val actualRDD = rdd
      .sortedFoldLeftByKey(
        Queue.empty[Int],
        (q: Queue[Int], v: Int) => q.enqueue(v),
        7
      )
      .cache()
    val actual = actualRDD.collectAsMap()
    val expected = Map("key1" -> Queue(1, 2, 3), "key2" -> Queue(4, 5))
    assert(expected == actual)
    assert(actualRDD.getNumPartitions == 7)
    actualRDD.unpersist(false)
  }

  test("sortedFoldLeftByKey with a partitioner applies fold as expected") {
    val input =
      Seq(("key1", 1), ("key1", 2), ("key1", 3), ("key2", 4), ("key2", 5))
    val rdd = sc.parallelize(rand.shuffle(input))
    val partitioner = new RangePartitioner[String, Int](3, rdd, true)
    val actualRDD = rdd
      .sortedFoldLeftByKey(
        Queue.empty[Int],
        (q: Queue[Int], v: Int) => q.enqueue(v),
        partitioner
      )
      .cache()
    val actual = actualRDD.collectAsMap()
    val expected = Map("key1" -> Queue(1, 2, 3), "key2" -> Queue(4, 5))
    assert(expected == actual)
    assert(actualRDD.getNumPartitions == 3)
    actualRDD.unpersist(false)
  }

  test("mapValuesWithKeyedResource fails when a key has two resources") {
    val resources =
      Seq("key1" -> Map.empty[String, Int], "key1" -> Map.empty[String, Int])
    val resourcesRDD = sc.parallelize(resources)
    val dataRDD = sc.emptyRDD[(String, Unit)]
    assertThrows[SparkException] {
      dataRDD
        .mapValuesWithKeyedResource(
          resourcesRDD,
          (r: Map[String, Int]) => (_: Unit) => r
        )
        .collect()
    }
  }

  test("mapValuesWithKeyedResource fails when a key has no resources") {
    val resources = Seq("key1" -> Map.empty[String, Int])
    val resourcesRDD = sc.parallelize(resources)
    val data = Seq(("key1", ()), ("key2", ()))
    val dataRDD = sc.parallelize(data)
    assertThrows[SparkException] {
      dataRDD
        .mapValuesWithKeyedResource(
          resourcesRDD,
          (r: Map[String, Int]) => (_: Unit) => r
        )
        .collect()
    }
  }

  test(
    "mapValuesWithKeyedResource pairs the right resource to the right values and applies the operation"
  ) {
    val resources =
      Seq("key1" -> Map(1 -> 10, 2 -> 20), "key2" -> Map(3 -> -30, 4 -> -40))
    val resourcesRDD = sc.parallelize(resources)
    val data = Seq("key1" -> 1, "key1" -> 2, "key2" -> 3, "key2" -> 4)
    val dataRDD = sc.parallelize(data)
    val actual = dataRDD
      .mapValuesWithKeyedResource(
        resourcesRDD,
        (r: Map[Int, Int]) => (v: Int) => r(v)
      )
      .collect()
    val expected =
      Array("key1" -> 10, "key1" -> 20, "key2" -> -30, "key2" -> -40)
    expected contains theSameElementsInOrderAs(actual)
  }

  test(
    "mapValuesWithKeyedResource with numPartitions pairs the right resource to the right values and applies the operation"
  ) {
    val resources =
      Seq("key1" -> Map(1 -> 10, 2 -> 20), "key2" -> Map(3 -> -30, 4 -> -40))
    val resourcesRDD = sc.parallelize(resources)
    val data = Seq("key1" -> 1, "key1" -> 2, "key2" -> 3, "key2" -> 4)
    val dataRDD = sc.parallelize(data)
    val actualRDD = dataRDD
      .mapValuesWithKeyedResource(
        resourcesRDD,
        (r: Map[Int, Int]) => (v: Int) => r(v),
        7
      )
      .cache()
    val actual = actualRDD.collect()
    val expected =
      Array("key1" -> 10, "key1" -> 20, "key2" -> -30, "key2" -> -40)
    expected contains theSameElementsInOrderAs(actual)
    assert(actualRDD.getNumPartitions == 7)
    actualRDD.unpersist(false)
  }

  test(
    "mapValuesWithKeyedResource with partitioner pairs the right resource to the right values and applies the operation"
  ) {
    val resources =
      Seq("key1" -> Map(1 -> 10, 2 -> 20), "key2" -> Map(3 -> -30, 4 -> -40))
    val resourcesRDD = sc.parallelize(resources)
    val data = Seq("key1" -> 1, "key1" -> 2, "key2" -> 3, "key2" -> 4)
    val dataRDD = sc.parallelize(data)
    val partitioner = new RangePartitioner(3, dataRDD, true)
    val actualRDD = dataRDD
      .mapValuesWithKeyedResource(
        resourcesRDD,
        (r: Map[Int, Int]) => (v: Int) => r(v),
        partitioner
      )
      .cache()
    val actual = actualRDD.collect()
    val expected =
      Array("key1" -> 10, "key1" -> 20, "key2" -> -30, "key2" -> -40)
    expected contains theSameElementsInOrderAs(actual)
    assert(actualRDD.getNumPartitions == 3)
    actualRDD.unpersist(false)
  }

  test("fullOuterJoinWithSortedValues joins 4 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val rdd4 = sc.parallelize(Seq(
      "a" -> "rdd4-a", "b" -> "rdd4-b", "c" -> "rdd4-c", "d" -> "rdd4-d"
    ))
    val actual = rdd1.fullOuterJoinWithSortedValues(rdd2, rdd3, rdd4, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actual == Seq(
        "a" -> (Some("rdd1-a"), None, Some("rdd3-a"), Some("rdd4-a")),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), Some("rdd3-b"), Some("rdd4-b")),
        "c" -> (None, Some("rdd2-c"), None, Some("rdd4-c")),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d")),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d"))
      )
    )
  }

  test("fullOuterJoinWithSortedValues joins 3 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val actual = rdd1.fullOuterJoinWithSortedValues(rdd2, rdd3, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actual == Seq(
        "a" -> (Some("rdd1-a"), None, Some("rdd3-a")),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), Some("rdd3-b")),
        "c" -> (None, Some("rdd2-c"), None),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), Some("rdd3-d")),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), Some("rdd3-d"))
      )
    )
  }

  test("fullOuterJoinWithSortedValues joins 2 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val actual = rdd1.fullOuterJoinWithSortedValues(rdd2, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actual == Seq(
        "a" -> (Some("rdd1-a"), None),
        "b" -> (Some("rdd1-b"), Some("rdd2-b")),
        "c" -> (None, Some("rdd2-c")),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d")),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"))
      )
    )
  }

  test("innerJoinWithSortedValues joins 4 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val rdd4 = sc.parallelize(Seq(
      "a" -> "rdd4-a", "b" -> "rdd4-b", "c" -> "rdd4-c", "d" -> "rdd4-d"
    ))
    val actual = rdd1.innerJoinWithSortedValues(rdd2, rdd3, rdd4, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actual == Seq(
        "b" -> ("rdd1-b", "rdd2-b", "rdd3-b", "rdd4-b"),
        "d" -> ("rdd1-d1", "rdd2-d", "rdd3-d", "rdd4-d"),
        "d" -> ("rdd1-d2", "rdd2-d", "rdd3-d", "rdd4-d")
      )
    )
  }

  test("innerJoinWithSortedValues joins 3 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val actual = rdd1.innerJoinWithSortedValues(rdd2, rdd3, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actual == Seq(
        "b" -> ("rdd1-b", "rdd2-b", "rdd3-b"),
        "d" -> ("rdd1-d1", "rdd2-d", "rdd3-d"),
        "d" -> ("rdd1-d2", "rdd2-d", "rdd3-d")
      )
    )
  }

  test("innerJoinWithSortedValues joins 2 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val actual = rdd1.innerJoinWithSortedValues(rdd2, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actual == Seq(
        "b" -> ("rdd1-b", "rdd2-b"),
        "d" -> ("rdd1-d1", "rdd2-d"),
        "d" -> ("rdd1-d2", "rdd2-d")
      )
    )
  }

  test("leftJoinWithSortedValues joins 4 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val rdd4 = sc.parallelize(Seq(
      "a" -> "rdd4-a", "b" -> "rdd4-b", "c" -> "rdd4-c", "d" -> "rdd4-d"
    ))
    val actual = rdd1.leftJoinWithSortedValues(rdd2, rdd3, rdd4, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actual == Seq(
        "a" -> ("rdd1-a", None, Some("rdd3-a"), Some("rdd4-a")),
        "b" -> ("rdd1-b", Some("rdd2-b"), Some("rdd3-b"), Some("rdd4-b")),
        "d" -> ("rdd1-d1", Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d")),
        "d" -> ("rdd1-d2", Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d"))
      )
    )
  }

  test("leftJoinWithSortedValues joins 3 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val actual = rdd1.leftJoinWithSortedValues(rdd2, rdd3, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actual == Seq(
        "a" -> ("rdd1-a", None, Some("rdd3-a")),
        "b" -> ("rdd1-b", Some("rdd2-b"), Some("rdd3-b")),
        "d" -> ("rdd1-d1", Some("rdd2-d"), Some("rdd3-d")),
        "d" -> ("rdd1-d2", Some("rdd2-d"), Some("rdd3-d"))
      )
    )
  }

  test("leftJoinWithSortedValues joins 2 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val actual = rdd1.leftJoinWithSortedValues(rdd2, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actual == Seq(
        "a" -> ("rdd1-a", None),
        "b" -> ("rdd1-b", Some("rdd2-b")),
        "d" -> ("rdd1-d1", Some("rdd2-d")),
        "d" -> ("rdd1-d2", Some("rdd2-d"))
      )
    )
  }

  test("rightJoinWithSortedValues joins 4 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val rdd4 = sc.parallelize(Seq(
      "a" -> "rdd4-a", "b" -> "rdd4-b", "d" -> "rdd4-d"  // "c" -> "rdd4-c" was removed, compared to the other similar tests tests, to ensure all behaviors were covered
    ))
    val actual = rdd1.rightJoinWithSortedValues(rdd2, rdd3, rdd4, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actual == Seq(
        "a" -> (Some("rdd1-a"), None, Some("rdd3-a"), "rdd4-a"),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), Some("rdd3-b"), "rdd4-b"),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), Some("rdd3-d"), "rdd4-d"),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), Some("rdd3-d"), "rdd4-d")
      )
    )
  }

  test("rightJoinWithSortedValues joins 3 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val actual = rdd1.rightJoinWithSortedValues(rdd2, rdd3, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actual == Seq(
        "a" -> (Some("rdd1-a"), None, "rdd3-a"),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), "rdd3-b"),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), "rdd3-d"),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), "rdd3-d")
      )
    )
  }

  test("rightJoinWithSortedValues joins 2 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val actual = rdd1.rightJoinWithSortedValues(rdd2, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actual == Seq(
        "b" -> (Some("rdd1-b"), "rdd2-b"),
        "c" -> (None, "rdd2-c"),
        "d" -> (Some("rdd1-d1"), "rdd2-d"),
        "d" -> (Some("rdd1-d2"), "rdd2-d")
      )
    )
  }
}
