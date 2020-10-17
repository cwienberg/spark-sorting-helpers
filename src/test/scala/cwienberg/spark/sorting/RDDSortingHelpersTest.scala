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

  test("groupByAndSort groups like-data together and sorts it") {
    val input = Seq(
      GroupByTestObject("group1", 1),
      GroupByTestObject("group1", 2),
      GroupByTestObject("group1", 3),
      GroupByTestObject("group2", 4),
      GroupByTestObject("group2", 5)
    )
    implicit val order = Ordering.by(GroupByTestObject.unapply)
    val rdd = sc.parallelize(rand.shuffle(input), 5)
    val actual = rdd.groupByAndSort(_.group).collect()
    val expected = Array(
      Vector(
        GroupByTestObject("group1", 1),
        GroupByTestObject("group1", 2),
        GroupByTestObject("group1", 3)
      ),
      Vector(GroupByTestObject("group2", 4), GroupByTestObject("group2", 5))
    )
    expected contains theSameElementsAs(actual)
  }

  test(
    "groupByAndSort with a number of partitions groups like-data together and sorts it"
  ) {
    val input = Seq(
      GroupByTestObject("group1", 1),
      GroupByTestObject("group1", 2),
      GroupByTestObject("group1", 3),
      GroupByTestObject("group2", 4),
      GroupByTestObject("group2", 5)
    )
    implicit val order = Ordering.by(GroupByTestObject.unapply)
    val rdd = sc.parallelize(rand.shuffle(input), 5)
    val actualRDD = rdd.groupByAndSort(_.group, 7).cache()
    val actual = actualRDD.collect()
    val expected = Array(
      Vector(
        GroupByTestObject("group1", 1),
        GroupByTestObject("group1", 2),
        GroupByTestObject("group1", 3)
      ),
      Vector(GroupByTestObject("group2", 4), GroupByTestObject("group2", 5))
    )
    expected contains theSameElementsAs(actual)
    assert(actualRDD.getNumPartitions == 7)
    actualRDD.unpersist(false)
  }

  test(
    "groupByAndSort with a partitioner groups like-data together and sorts it"
  ) {
    val input = Seq(
      GroupByTestObject("group1", 1),
      GroupByTestObject("group1", 2),
      GroupByTestObject("group1", 3),
      GroupByTestObject("group2", 4),
      GroupByTestObject("group2", 5)
    )
    implicit val order = Ordering.by(GroupByTestObject.unapply)
    val rdd = sc.parallelize(rand.shuffle(input), 5)
    val partitioner = new HashPartitioner(3)
    val actualRDD = rdd.groupByAndSort(_.group, partitioner).cache()
    val actual = actualRDD.collect()
    val expected = Array(
      Vector(
        GroupByTestObject("group1", 1),
        GroupByTestObject("group1", 2),
        GroupByTestObject("group1", 3)
      ),
      Vector(GroupByTestObject("group2", 4), GroupByTestObject("group2", 5))
    )
    expected contains theSameElementsAs(actual)
    assert(actualRDD.getNumPartitions == 3)
    actualRDD.unpersist(false)
  }

  test("sortedFoldLeftBy applies to like-data and folds over sorted values") {
    val input = Seq(
      GroupByTestObject("group1", 1),
      GroupByTestObject("group1", 2),
      GroupByTestObject("group1", 3),
      GroupByTestObject("group2", 4),
      GroupByTestObject("group2", 5)
    )
    implicit val order = Ordering.by(GroupByTestObject.unapply)
    val rdd = sc.parallelize(rand.shuffle(input), 5)
    val actual = rdd
      .sortedFoldLeftBy(
        _.group,
        Queue.empty[GroupByTestObject],
        (q: Queue[GroupByTestObject], v: GroupByTestObject) => q.enqueue(v)
      )
      .collect()
    val expected = Array(
      Queue(
        GroupByTestObject("group1", 1),
        GroupByTestObject("group1", 2),
        GroupByTestObject("group1", 3)
      ),
      Queue(GroupByTestObject("group2", 4), GroupByTestObject("group2", 5))
    )
    expected contains theSameElementsAs(actual)
  }

  test(
    "sortedFoldLeftBy with numPartitions applies to like-data and folds over sorted values"
  ) {
    val input = Seq(
      GroupByTestObject("group1", 1),
      GroupByTestObject("group1", 2),
      GroupByTestObject("group1", 3),
      GroupByTestObject("group2", 4),
      GroupByTestObject("group2", 5)
    )
    implicit val order = Ordering.by(GroupByTestObject.unapply)
    val rdd = sc.parallelize(rand.shuffle(input), 5)
    val actualRDD = rdd
      .sortedFoldLeftBy(
        _.group,
        Queue.empty[GroupByTestObject],
        (q: Queue[GroupByTestObject], v: GroupByTestObject) => q.enqueue(v),
        7
      )
      .cache()
    val actual = actualRDD.collect()
    val expected = Array(
      Queue(
        GroupByTestObject("group1", 1),
        GroupByTestObject("group1", 2),
        GroupByTestObject("group1", 3)
      ),
      Queue(GroupByTestObject("group2", 4), GroupByTestObject("group2", 5))
    )
    expected contains theSameElementsAs(actual)
    assert(actualRDD.getNumPartitions == 7)
    actualRDD.unpersist(false)
  }

  test(
    "sortedFoldLeftBy with partitioner applies to like-data and folds over sorted values"
  ) {
    val input = Seq(
      GroupByTestObject("group1", 1),
      GroupByTestObject("group1", 2),
      GroupByTestObject("group1", 3),
      GroupByTestObject("group2", 4),
      GroupByTestObject("group2", 5)
    )
    implicit val order = Ordering.by(GroupByTestObject.unapply)
    val rdd = sc.parallelize(rand.shuffle(input), 5)
    val partitioner = new HashPartitioner(3)
    val actualRDD = rdd
      .sortedFoldLeftBy(
        _.group,
        Queue.empty[GroupByTestObject],
        (q: Queue[GroupByTestObject], v: GroupByTestObject) => q.enqueue(v),
        partitioner
      )
      .cache()
    val actual = actualRDD.collect()
    val expected = Array(
      Queue(
        GroupByTestObject("group1", 1),
        GroupByTestObject("group1", 2),
        GroupByTestObject("group1", 3)
      ),
      Queue(GroupByTestObject("group2", 4), GroupByTestObject("group2", 5))
    )
    expected contains theSameElementsAs(actual)
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

}
