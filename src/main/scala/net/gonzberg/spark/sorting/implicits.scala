package net.gonzberg.spark.sorting

import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

object implicits {
  implicit def rddToSecondarySortGroupingPairRDDFunctions[
    K: Ordering: ClassTag,
    V: Ordering: ClassTag
  ](rdd: RDD[(K, V)]): SecondarySortGroupingPairRDDFunctions[K, V] = {
    SecondarySortGroupingPairRDDFunctions
      .rddToSecondarySortGroupingPairRDDFunctions(rdd)
  }

  implicit def rddToSecondarySortJoiningPairRDDFunctions[
    K: Ordering: ClassTag,
    V: Ordering: ClassTag
  ](rdd: RDD[(K, V)]): SecondarySortJoiningPairRDDFunctions[K, V] = {
    SecondarySortJoiningPairRDDFunctions
      .rddToSecondarySortJoiningPairRDDFunctions(rdd)
  }

  implicit def rddToSecondarySortGroupAndSortByPairRDDFunctions[
    K: Ordering: ClassTag,
    V: ClassTag
  ](rdd: RDD[(K, V)]): SecondarySortGroupAndSortByPairRDDFunctions[K, V] = {
    SecondarySortGroupAndSortByPairRDDFunctions.rddToGroupByAndSortFunctions(
      rdd
    )
  }
}
