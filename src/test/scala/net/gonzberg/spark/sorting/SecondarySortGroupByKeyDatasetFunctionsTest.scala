package net.gonzberg.spark.sorting

import SecondarySortGroupByKeyDatasetFunctions.datasetToSecondarySortGroupByKeyDatasetFunctions
import org.apache.spark.SparkException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.Queue
import scala.util.Random

case class DatasetTestWrapper[T](value: T)

class SecondarySortGroupByKeyDatasetFunctionsTest extends AnyFunSuite with Matchers with SparkTestingMixin {

  import spark.implicits._

  def rand: Random = new Random(47)

  test("creates expected, sorted groups") {
    val input = for {
      key <- Seq(1, 10, 100)
      value <- 0.until(100)
    } yield (key, DatasetTestWrapper(value * key))
    val dataset = rand.shuffle(input).toDS()
    val actual = dataset.sortedGroupByKey(dataset.col("_2.value")).collect().toMap
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
    } yield (key, DatasetTestWrapper(value * key))
    val dataset = rand.shuffle(input).toDS()
    val actualDataset = dataset.sortedGroupByKey(7, dataset.col("_2.value")).cache()
    val actual = actualDataset.collect().toMap
    assert(actual.size == 3)
    assert(actual.keys.toSet == Set(1, 10, 100))
    actual.values.foreach(v => assert(v.size == 100))
    actual.foreach { case (k, values) =>
      values.zip(0.until(100)).foreach { case (a, b) => a.value == b * k }
    }
    assert(actualDataset.rdd.getNumPartitions == 7)
    actualDataset.unpersist()
  }

  test("sortedFoldLeftByKey applies fold as expected") {
    val input =
      Seq(("key1", DatasetTestWrapper(1)), ("key1", DatasetTestWrapper(2)), ("key1", DatasetTestWrapper(3)), ("key2", DatasetTestWrapper(4)), ("key2", DatasetTestWrapper(5)))
    val dataset = rand.shuffle(input).toDS()
    val actual = dataset
      .sortedFoldLeftByKey(
        Queue.empty[DatasetTestWrapper[Int]],
        (q: Queue[DatasetTestWrapper[Int]], v: DatasetTestWrapper[Int]) => q.enqueue(v),
        dataset.col("_2.value")
      )
      .map { case (key, queue) => (key, queue.map(_.value)) }
      .collect()
      .toMap
    val expected = Map("key1" -> Queue(1, 2, 3), "key2" -> Queue(4, 5))
    assert(expected == actual)
  }

  test(
    "sortedFoldLeftByKey with a number of partitions applies fold as expected"
  ) {
    val input =
      Seq(("key1", 1), ("key1", 2), ("key1", 3), ("key2", 4), ("key2", 5))
    val dataset = rand.shuffle(input).toDS()
    val actualDataset = dataset
      .sortedFoldLeftByKey(
        Queue.empty[Int],
        (q: Queue[Int], v: Int) => q.enqueue(v),
        7,
        dataset.col("_2")
      )
      .cache()
    val actual = actualDataset.collect().toMap
    val expected = Map("key1" -> Queue(1, 2, 3), "key2" -> Queue(4, 5))
    assert(expected == actual)
    assert(actualDataset.rdd.getNumPartitions == 7)
    actualDataset.unpersist(false)
  }

  test("sortedFoldLeftByKey with startingValues DS with startingValues RDD applies join and fold as expected") {
    val input =
      Seq(("key1", 1), ("key1", 2), ("key1", 3), ("key2", 4), ("key2", 5))
    val startingValues = Seq(("key1", Queue(-1)), ("key2", Queue(-2)))
    val dataset = rand.shuffle(input).toDS()
    val startingValuesDS = startingValues.toDS()
    val actual = dataset
      .sortedFoldLeftByKey(
        startingValuesDS,
        (q: Queue[Int], v: Int) => q.enqueue(v),
        dataset.col("_2")
      )
      .collect()
      .toMap
    val expected = Map("key1" -> Queue(-1, 1, 2, 3), "key2" -> Queue(-2, 4, 5))
    assert(expected == actual)
  }

  test("sortedFoldLeftByKey with startingValues DS with a number of partitions applies join and fold as expected") {
    val input =
      Seq(("key1", 1), ("key1", 2), ("key1", 3), ("key2", 4), ("key2", 5))
    val startingValues = Seq(("key1", Queue(-1)), ("key2", Queue(-2)))
    val dataset = rand.shuffle(input).toDS()
    val startingValuesDS = startingValues.toDS()
    val actualDS = dataset
      .sortedFoldLeftByKey(
        startingValuesDS,
        (q: Queue[Int], v: Int) => q.enqueue(v),
        numPartitions = 7,
        dataset.col("_2")
      )
      .cache()
    val actual = actualDS.collect().toMap
    val expected = Map("key1" -> Queue(-1, 1, 2, 3), "key2" -> Queue(-2, 4, 5))
    assert(expected == actual)
    assert(actualDS.rdd.getNumPartitions == 7)
    actualDS.unpersist(false)
  }

  test("sortedFoldLeftByKey with startingValues DS fails when key has two start values") {
    val input =
      Seq(("key1", 1), ("key1", 2), ("key1", 3), ("key2", 4), ("key2", 5))
    val startingValues = Seq(("key1", Queue(-1)), ("key1", Queue(-100)), ("key2", Queue(-2)))
    val dataset = rand.shuffle(input).toDS()
    val startingValuesDS = startingValues.toDS()
    assertThrows[SparkException] {
      dataset
        .sortedFoldLeftByKey(
          startingValuesDS,
          (q: Queue[Int], v: Int) => q.enqueue(v),
          dataset.col("_2")
        )
        .collect()
    }
  }

  test("sortedFoldLeftByKey with startingValues DS fails when key has no start values") {
    val input =
      Seq(("key1", 1), ("key1", 2), ("key1", 3), ("key2", 4), ("key2", 5))
    val startingValues = Seq(("key1", Queue(-1)))
    val dataset = rand.shuffle(input).toDS()
    val startingValuesDS = startingValues.toDS()
    assertThrows[SparkException] {
      dataset
        .sortedFoldLeftByKey(
          startingValuesDS,
          (q: Queue[Int], v: Int) => q.enqueue(v),
          dataset.col("_2")
        )
        .collect()
    }
  }

  test("sortedFoldLeftByKey with startingValues DS fails when key has no values") {
    val input =
      Seq(("key1", 1), ("key1", 2), ("key1", 3))
    val startingValues = Seq(("key1", Queue(-1)), ("key2", Queue(-2)))
    val dataset = rand.shuffle(input).toDS()
    val startingValuesDS = startingValues.toDS()
    assertThrows[SparkException] {
      dataset
        .sortedFoldLeftByKey(
          startingValuesDS,
          (q: Queue[Int], v: Int) => q.enqueue(v),
          dataset.col("_2")
        )
        .collect()
    }
  }

}
