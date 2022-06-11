package net.gonzberg.spark.sorting

import SecondarySortGroupByKeyDatasetFunctions.datasetToSecondarySortGroupByKeyDatasetFunctions
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

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

}
