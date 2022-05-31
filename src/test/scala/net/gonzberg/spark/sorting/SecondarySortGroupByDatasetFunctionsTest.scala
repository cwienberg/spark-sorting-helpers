package net.gonzberg.spark.sorting

import SecondarySortGroupByDatasetFunctions.datasetToSecondarySortedDatasetFunctions
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Random

case class DSTestWrapper(integerValue: Int, stringValue: String)
case class AggregatedTestWrapper(integerValues: Seq[Int] = Seq.empty, stringValue: String = null)

class SecondarySortGroupByDatasetFunctionsTest extends AnyFunSuite with Matchers with SparkTestingMixin {
  def rand: Random = new Random(47)

  test("foldLeft applies fold as expected") {
    import spark.implicits._
    val input =
      Seq(DSTestWrapper(1, "key1"), DSTestWrapper(2, "key1"), DSTestWrapper(3, "key1"), DSTestWrapper(4, "key2"), DSTestWrapper(5, "key2"))
    val ds = rand.shuffle(input).toDS()
    val actual = ds
      .partitionBy("stringValue")
      .secondarySortBy("integerValue")
      .foldLeft(
        AggregatedTestWrapper(),
        (agg: AggregatedTestWrapper, v: DSTestWrapper) => AggregatedTestWrapper(agg.integerValues :+ v.integerValue, v.stringValue)
      )
      .collect()
    val expected = Seq(AggregatedTestWrapper(Seq(1, 2, 3), "key1"), AggregatedTestWrapper(Seq(4, 5), "key2"))
    actual should contain theSameElementsAs expected
  }
}
