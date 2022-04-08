package net.gonzberg.spark.sorting

import GroupAndSortByFunctions.rddToGroupByAndSortFunctions
import org.apache.spark.{HashPartitioner, SparkException}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.Queue
import scala.util.Random

class TestWrapper[T](val value: T) extends Serializable

object TestWrapper {
  def apply[T](value: T): TestWrapper[T] = new TestWrapper(value)
}

class GroupAndSortByFunctionsTest
  extends AnyFunSuite
    with Matchers
    with SparkTestingMixin {

  def rand: Random = new Random(47)

  test("creates expected, sorted groups") {
    val input = for {
      key <- Seq(1, 10, 100)
      value <- 0.until(100)
    } yield (key, TestWrapper(value * key))
    val rdd = sc.parallelize(rand.shuffle(input), 5)
    val actual = rdd.sortedGroupByKey((v: TestWrapper[Int]) => v.value).collectAsMap()
    assert(actual.size == 3)
    assert(actual.keys.toSet == Set(1, 10, 100))
    actual.values.foreach(v => assert(v.size == 100))
    actual.foreach { case (k, values) =>
      values.zip(0.until(100)).foreach { case (a, b) => a.value == b * k }
    }
  }

  test("creates expected, sorted groups with a number of partitions") {
    val input = for {
      key <- Seq(1, 10, 100)
      value <- 0.until(100)
    } yield (key, TestWrapper(value * key))
    val rdd = sc.parallelize(rand.shuffle(input), 5)
    val actualRDD = rdd.sortedGroupByKey((v: TestWrapper[Int]) => v.value, 7).cache()
    val actual = actualRDD.collectAsMap()
    assert(actual.size == 3)
    assert(actual.keys.toSet == Set(1, 10, 100))
    actual.values.foreach(v => assert(v.size == 100))
    actual.foreach { case (k, values) =>
      values.zip(0.until(100)).foreach { case (a, b) => a.value == b * k }
    }
    assert(actualRDD.getNumPartitions == 7)
    actualRDD.unpersist(false)
  }

  test("creates expected, sorted groups with a given partitioner") {
    val input = for {
      key <- Seq(1, 10, 100)
      value <- 0.until(100)
    } yield (key, TestWrapper(value * key))
    val rdd = sc.parallelize(rand.shuffle(input), 5)
    val partitioner = new HashPartitioner(3)
    val actualRDD = rdd.sortedGroupByKey((v: TestWrapper[Int]) => v.value, partitioner).cache()
    val actual = actualRDD.collectAsMap()
    assert(actual.size == 3)
    assert(actual.keys.toSet == Set(1, 10, 100))
    actual.values.foreach(v => assert(v.size == 100))
    actual.foreach { case (k, values) =>
      values.zip(0.until(100)).foreach { case (a, b) => a.value == b * k }
    }
    assert(actualRDD.getNumPartitions == 3)
    actualRDD.unpersist(false)
  }

  test("creates expected, sorted groups when they're already partitioned") {
    val input = for {
      key <- Seq(1, 10, 100, 1000, 10000)
      value <- 0.until(100)
    } yield (key, TestWrapper(value * key))
    val partitioner = new HashPartitioner(3)
    val rdd = sc.parallelize(rand.shuffle(input), 5).partitionBy(partitioner)
    val actual = rdd.sortedGroupByKey((v: TestWrapper[Int]) => v.value, partitioner).collectAsMap()
    assert(actual.size == 5)
    assert(actual.keys.toSet == Set(1, 10, 100, 1000, 10000))
    actual.values.foreach(v => assert(v.size == 100))
    actual.foreach { case (k, values) =>
      values.zip(0.until(100)).foreach { case (a, b) => a.value == b * k }
    }
  }

  test("sortedFoldLeftByKey applies fold as expected") {
    val input =
      Seq(("key1", TestWrapper(1)), ("key1", TestWrapper(2)), ("key1", TestWrapper(3)), ("key2", TestWrapper(4)), ("key2", TestWrapper(5)))
    val rdd = sc.parallelize(rand.shuffle(input))
    val actual = rdd
      .sortedFoldLeftByKey(
        Queue.empty[TestWrapper[Int]],
        (q: Queue[TestWrapper[Int]], v: TestWrapper[Int]) => q.enqueue(v),
        _.value
      )
      .mapValues(_.map(_.value))
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
        identity(_: Int),
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
    val partitioner = new HashPartitioner(3)
    val actualRDD = rdd
      .sortedFoldLeftByKey(
        Queue.empty[Int],
        (q: Queue[Int], v: Int) => q.enqueue(v),
        identity(_: Int),
        partitioner
      )
      .cache()
    val actual = actualRDD.collectAsMap()
    val expected = Map("key1" -> Queue(1, 2, 3), "key2" -> Queue(4, 5))
    assert(expected == actual)
    assert(actualRDD.getNumPartitions == 3)
    actualRDD.unpersist(false)
  }

  test("mapValuesWithKeyedPreparedResource fails when a key has two resources") {
    val resources =
      Seq("key1" -> Map.empty[String, Int], "key1" -> Map.empty[String, Int])
    val resourcesRDD = sc.parallelize(resources)
    val dataRDD = sc.emptyRDD[(String, Unit)]
    assertThrows[SparkException] {
      dataRDD
        .mapValuesWithKeyedPreparedResource(
          resourcesRDD,
          (r: Map[String, Int]) => (_: Unit) => r,
          identity(_: Unit)
        )
        .collect()
    }
  }

  test("mapValuesWithKeyedPreparedResource fails when a key has no resources") {
    val dataRDD = sc.parallelize(Seq(("key1", ()), ("key2", ())))
    val resources1 = sc.parallelize(Seq("key1" -> Map.empty[String, Int]))
    assertThrows[SparkException] {
      dataRDD
        .mapValuesWithKeyedPreparedResource(
          resources1,
          (r: Map[String, Int]) => (_: Unit) => r,
          identity(_: Unit),
          1
        )
        .collect()
    }

    val resources2 = sc.parallelize(Seq("key2" -> Map.empty[String, Int]))
    assertThrows[SparkException] {
      dataRDD
        .mapValuesWithKeyedPreparedResource(
          resources2,
          (r: Map[String, Int]) => (_: Unit) => r,
          identity(_: Unit),
          1
        )
        .collect()
    }
  }

  test("mapValuesWithKeyedPreparedResource fails when a key has no values") {
    val resourcesRDD = sc.parallelize(Seq("key1" -> Map.empty[String, Int], "key2" -> Map.empty[String, Int]))
    val data1 = sc.parallelize(Seq(("key1", ())))
    assertThrows[SparkException] {
      data1
        .mapValuesWithKeyedPreparedResource(
          resourcesRDD,
          (r: Map[String, Int]) => (_: Unit) => r,
          identity(_: Unit),
          1
        )
        .collect()
    }

    val data2 = sc.parallelize(Seq(("key2", ())))
    assertThrows[SparkException] {
      data2
        .mapValuesWithKeyedPreparedResource(
          resourcesRDD,
          (r: Map[String, Int]) => (_: Unit) => r,
          identity(_: Unit),
          1
        )
        .collect()
    }
  }

  test(
    "mapValuesWithKeyedPreparedResource pairs the right resource to the right values and applies the operation"
  ) {
    val resources =
      Seq("key1" -> Map(1 -> 10, 2 -> 20), "key2" -> Map(3 -> -30, 4 -> -40))
    val resourcesRDD = sc.parallelize(resources)
    val data = Seq("key1" -> 1, "key1" -> 2, "key2" -> 3, "key2" -> 4)
    val dataRDD = sc.parallelize(data)
    val actual = dataRDD
      .mapValuesWithKeyedPreparedResource(
        resourcesRDD,
        (r: Map[Int, Int]) => (v: Int) => r(v),
        identity(_: Int)
      )
      .collect()
    val expected =
      Array("key1" -> 10, "key1" -> 20, "key2" -> -30, "key2" -> -40)
    expected contains theSameElementsInOrderAs(actual)
  }

  test(
    "mapValuesWithKeyedPreparedResource with numPartitions pairs the right resource to the right values and applies the operation"
  ) {
    val resources =
      Seq("key1" -> Map(1 -> 10, 2 -> 20), "key2" -> Map(3 -> -30, 4 -> -40))
    val resourcesRDD = sc.parallelize(resources)
    val data = Seq("key1" -> 1, "key1" -> 2, "key2" -> 3, "key2" -> 4)
    val dataRDD = sc.parallelize(data)
    val actualRDD = dataRDD
      .mapValuesWithKeyedPreparedResource(
        resourcesRDD,
        (r: Map[Int, Int]) => (v: Int) => r(v),
        identity(_: Int),
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
    "mapValuesWithKeyedPreparedResource with partitioner pairs the right resource to the right values and applies the operation"
  ) {
    val resources =
      Seq("key1" -> Map(1 -> 10, 2 -> 20), "key2" -> Map(3 -> -30, 4 -> -40))
    val resourcesRDD = sc.parallelize(resources)
    val data = Seq("key1" -> 1, "key1" -> 2, "key2" -> 3, "key2" -> 4)
    val dataRDD = sc.parallelize(data)
    val partitioner = new HashPartitioner(3)
    val actualRDD = dataRDD
      .mapValuesWithKeyedPreparedResource(
        resourcesRDD,
        (r: Map[Int, Int]) => (v: Int) => r(v),
        identity(_: Int),
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

  test(
    "mapValuesWithKeyedPreparedResource with separated preparation and map ops pairs the right resource to the right values and applies the operation"
  ) {
    val resources =
      Seq("key1" -> Map(1 -> 10, 2 -> 20), "key2" -> Map(3 -> -30, 4 -> -40))
    val resourcesRDD = sc.parallelize(resources)
    val data = Seq("key1" -> 1, "key1" -> 2, "key2" -> 3, "key2" -> 4)
    val dataRDD = sc.parallelize(data)

    val expected =
      Array("key1" -> -10, "key1" -> -20, "key2" -> 30, "key2" -> 40)

    val actualWithPartitioner = dataRDD
      .mapValuesWithKeyedPreparedResource(
        resourcesRDD,
        (r: Map[Int, Int]) => r.mapValues(v => -v),
        (r: Map[Int, Int], v: Int) => r(v),
        identity(_: Int),
        new HashPartitioner(3)
      )
      .collect()
    expected contains theSameElementsInOrderAs(actualWithPartitioner)

    val actualWithNumPartitions = dataRDD
      .mapValuesWithKeyedPreparedResource(
        resourcesRDD,
        (r: Map[Int, Int]) => r.mapValues(v => -v),
        (r: Map[Int, Int], v: Int) => r(v),
        identity(_: Int),
        3
      )
      .collect()
    expected contains theSameElementsInOrderAs(actualWithNumPartitions)

    val actualWithDefaultPartitioner = dataRDD
      .mapValuesWithKeyedPreparedResource(
        resourcesRDD,
        (r: Map[Int, Int]) => r.mapValues(v => -v),
        (r: Map[Int, Int], v: Int) => r(v),
        identity(_: Int)
      )
      .collect()
    expected contains theSameElementsInOrderAs(actualWithDefaultPartitioner)
  }

  test(
    "mapValuesWithKeyedResource pairs the right resource to the right values and applies the operation"
  ) {
    val resources =
      Seq("key1" -> Map(1 -> 10, 2 -> 20), "key2" -> Map(3 -> -30, 4 -> -40))
    val resourcesRDD = sc.parallelize(resources)
    val data = Seq("key1" -> 1, "key1" -> 2, "key2" -> 3, "key2" -> 4)
    val dataRDD = sc.parallelize(data)

    val expected =
      Array("key1" -> 10, "key1" -> 20, "key2" -> -30, "key2" -> -40)

    val actualWithPartitioner = dataRDD
      .mapValuesWithKeyedResource(
        resourcesRDD,
        (r: Map[Int, Int], v: Int) => r(v),
        identity(_: Int),
        new HashPartitioner(3)
      )
      .collect()
    expected contains theSameElementsInOrderAs(actualWithPartitioner)

    val actualWithNumPartitions = dataRDD
      .mapValuesWithKeyedResource(
        resourcesRDD,
        (r: Map[Int, Int], v: Int) => r(v),
        identity(_: Int),
        3
      )
      .collect()
    expected contains theSameElementsInOrderAs(actualWithNumPartitions)

    val actualWithDefaultPartitioner = dataRDD
      .mapValuesWithKeyedResource(
        resourcesRDD,
        (r: Map[Int, Int], v: Int) => r(v),
        identity(_: Int)
      )
      .collect()
    expected contains theSameElementsInOrderAs(actualWithDefaultPartitioner)
  }
}
