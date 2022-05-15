package net.gonzberg.spark.sorting

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class implicitsTest extends AnyFunSuite with Matchers with SparkTestingMixin {
  import implicits._

  test("SecondarySortGroupingPairRDDFunctions implicits available") {
    val rdd = sc.parallelize(
      Seq("key1" ->"value1", "key1" -> "value2", "key2" -> "value1"), numSlices = 1
    )
    val expected = Array("key1" -> Vector("value1", "value2"), "key2" -> Vector("value1"))
    val actual = rdd.sortedGroupByKey(numPartitions = 1).collect()
    expected should contain theSameElementsInOrderAs(actual)
  }

  test("SecondarySortJoiningPairRDDFunctions implicits available") {
    val rdd1 = sc.parallelize(
      Seq("key1" -> "value1", "key2" -> "value1"),
      numSlices = 1
    )
    val rdd2 = sc.parallelize(Seq("key2" -> "value2"), numSlices = 1)
    val expected = Array("key2" -> ("value1", "value2"))
    val actual = rdd1.innerJoinWithSortedValues(rdd2, numPartitions = 1).collect()
    expected should contain theSameElementsInOrderAs(actual)
  }

  test("SecondarySortGroupAndSortByPairRDDFunctions implicits available") {
    val rdd = sc.parallelize(
      Seq("key1" ->"value1", "key1" -> "value2", "key2" -> "value1"), numSlices = 1
    )
    val expected = Array("key1" -> Vector("value1", "value2"), "key2" -> Vector("value1"))
    val actual = rdd.groupByKeyAndSortBy((s: String) => s, numPartitions = 1).collect()
    expected should contain theSameElementsInOrderAs(actual)
  }
}
