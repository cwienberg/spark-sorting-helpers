package net.gonzberg.spark.sorting.util

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[sorting] object SortHelpers {
  def repartitionAndSort[K: Ordering, V: Ordering](
    rdd: RDD[(K, V)],
    partitioner: Partitioner
  ): RDD[(K, V)] = {
    rdd
      .mapPartitions(
        partition => {
          partition.map { case (key, value) =>
            (SecondarySortKey(key, value), ())
          }
        },
        preservesPartitioning = true
      )
      .repartitionAndSortWithinPartitions(
        new SecondarySortPartitioner(partitioner)
      )
      .mapPartitions(_.map(_._1.toTuple), preservesPartitioning = true)
  }

  def repartitionAndSort[K: Ordering, V: ClassTag, A: Ordering](
    rdd: RDD[(K, V)],
    by: V => A,
    partitioner: Partitioner
  ): RDD[(K, V)] = {
    rdd
      .mapPartitions(
        partition => {
          partition.map { case (key, value) =>
            (SecondarySortKey(key, by(value)), value)
          }
        },
        preservesPartitioning = true
      )
      .repartitionAndSortWithinPartitions(
        new SecondarySortPartitioner(partitioner)
      )
      .mapPartitions(
        _.map { case (SecondarySortKey(k, _), v) => (k, v) },
        preservesPartitioning = true
      )
  }

  def modifyResourcePreparationAndOp[R, R1, V, A](
    prepareResource: R => R1,
    op: (R1, V) => A
  ): R => V => A = {
    def newOp(resource: R): V => A = {
      val preparedResource = prepareResource(resource)
      op(preparedResource, _)
    }

    newOp
  }

  def joinAndApply[K, R, V, A](
    op: R => V => A
  )(resourcesIter: Iterator[(K, R)], valuesIter: Iterator[(K, Iterator[V])])(
    implicit keyOrdering: Ordering[K]
  ): Iterator[(K, A)] = {
    import keyOrdering.mkOrderingOps

    val resourceOptionIter = resourcesIter.map(Some(_))
    val valueOptionIter = valuesIter.map(Some(_))
    val zippedValuesAndResources =
      resourceOptionIter.zipAll(valueOptionIter, None, None)
    for {
      (maybeResource, maybeValue) <- zippedValuesAndResources
      (resourceKey, resource) = maybeResource.getOrElse(
        throw new IllegalArgumentException(
          "Must provide a resource for every key"
        )
      )
      (valueKey, values) = maybeValue.getOrElse(
        throw new IllegalArgumentException("Must provide a value for every key")
      )
      _ = require(resourceKey >= valueKey, "Must provide a value for every key")
      _ = require(
        resourceKey <= valueKey,
        "Must provide a resource for every key"
      )
      valueFunction = op(resource)
      value <- values
    } yield {
      valueKey -> valueFunction(value)
    }
  }

  def joinAndFold[K, V, A](
    op: (A, V) => A
  )(startValuesIter: Iterator[(K, A)], valuesIter: Iterator[(K, Iterator[V])])(
    implicit keyOrdering: Ordering[K]
  ): Iterator[(K, A)] = {
    import keyOrdering.mkOrderingOps

    val startValuesOptionIter = startValuesIter.map(Some(_))
    val valueOptionIter = valuesIter.map(Some(_))
    val zippedValuesAndResources =
      startValuesOptionIter.zipAll(valueOptionIter, None, None)
    for {
      (maybeStartValue, maybeValue) <- zippedValuesAndResources
      (startValueKey, startValue) = maybeStartValue.getOrElse(
        throw new IllegalArgumentException(
          "Must provide a starting value for every key"
        )
      )
      (valueKey, values) = maybeValue.getOrElse(
        throw new IllegalArgumentException("Must provide a value for every key")
      )
      _ = require(
        startValueKey >= valueKey,
        "Must provide a value for every key"
      )
      _ = require(
        startValueKey <= valueKey,
        "Must provide a starting value for every key"
      )
    } yield {
      valueKey -> values.foldLeft(startValue)(op)
    }
  }
}