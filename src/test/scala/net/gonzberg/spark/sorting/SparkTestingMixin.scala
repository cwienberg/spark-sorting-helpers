package net.gonzberg.spark.sorting

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

private[sorting] trait SparkTestingMixin {
  val spark: SparkSession = SparkTestingMixin.spark
  def sc: SparkContext = spark.sparkContext
}

private[sorting] object SparkTestingMixin {
  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("SparkSortingTestApplication")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .config("spark.worker.cleanup.enabled", "true")
    .config("spark.default.parallelism", "5")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()
}
