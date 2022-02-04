package cwienberg.spark.sorting

import org.apache.spark.{SparkConf, SparkContext}

private[sorting] trait SparkTestingMixin {
  def sc: SparkContext = SparkTestingMixin.sc
}

private[sorting] object SparkTestingMixin {
  lazy val sc: SparkContext = {
    val sparkConf = new SparkConf()
      .setAppName("SparkSortingTestApplication")
      .setMaster("local[*]")
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.ui.enabled", "false")
      .set("spark.worker.cleanup.enabled", "true")
      .set("spark.default.parallelism", "5")
      .set("spark.sql.shuffle.partitions", "5")

    SparkContext.getOrCreate(sparkConf)
  }
}
