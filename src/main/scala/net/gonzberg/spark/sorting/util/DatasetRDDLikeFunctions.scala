package net.gonzberg.spark.sorting.util

import org.apache.spark.sql.{Dataset, Encoder}

import scala.language.implicitConversions

private[sorting] final class DatasetRDDLikeFunctions[K, V](dataset: Dataset[(K, V)]) {
  def mapValues[U](fn: V => U)(implicit encoder: Encoder[(K, U)]): Dataset[(K, U)] = {
    dataset.map { case (k, v) => (k, fn(v))}
  }
}

private[sorting] object DatasetRDDLikeFunctions {
  implicit def toDatasetRDDLikeFunctions[K, V](dataset: Dataset[(K, V)]): DatasetRDDLikeFunctions[K, V] = {
    new DatasetRDDLikeFunctions(dataset)
  }
}
