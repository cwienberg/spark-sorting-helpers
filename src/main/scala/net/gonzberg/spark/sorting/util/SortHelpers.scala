package net.gonzberg.spark.sorting.util

import BufferedIteratorHelper.iterHeadOption
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

  private def zipByKeyForFold[K, V, A](
    startValuesIter: Iterator[(K, A)],
    valuesIter: Iterator[(K, Iterator[V])]
  )(implicit
    keyOrdering: Ordering[K]
  ): Iterator[(K, (Option[A], Option[Iterator[V]]))] = {
    import keyOrdering.mkOrderingOps
    var prevKey: Option[K] = None
    val bufferedStartValues = startValuesIter.buffered
    val bufferedValues = valuesIter.buffered

    def advanceStart(
      k: K,
      a: A
    ): Option[(K, (Option[A], Option[Iterator[V]]))] = {
      bufferedStartValues.next()
      prevKey = Some(k)
      Some((k, (Some(a), None)))
    }

    def advanceValues(
      k: K,
      vs: Iterator[V]
    ): Option[(K, (Option[A], Option[Iterator[V]]))] = {
      bufferedValues.next()
      prevKey = Some(k)
      Some((k, (None, Some(vs))))
    }

    Iterator
      .continually {
        (
          iterHeadOption(bufferedStartValues),
          iterHeadOption(bufferedValues)
        ) match {
          // Nothing left
          case (None, None) => None

          // An iterator had a repeat or went backwards
          case (Some((k, _)), _) if prevKey.isDefined && k <= prevKey.get =>
            throw new IllegalArgumentException(
              "Start values cannot have duplicated or out-or-order keys"
            )
          case (_, Some((k, _))) if prevKey.isDefined && k <= prevKey.get =>
            throw new IllegalArgumentException(
              "Values cannot have duplicated or out-of-order groupins"
            )

          // Only starting values left
          case (Some((k, a)), None) => advanceStart(k, a)

          // Only values left
          case (None, Some((k, vs))) => advanceValues(k, vs)

          // Start value key comes first
          case (Some((ka, a)), Some((kv, _))) if ka < kv => advanceStart(ka, a)

          // Value key comes first
          case (Some((ka, _)), Some((kv, vs))) if ka > kv =>
            advanceValues(ka, vs)

          // Start and value match on key
          case (Some((ka, a)), Some((kv, vs))) =>
            bufferedStartValues.next()
            bufferedValues.next()
            prevKey = Some(ka)
            Some((ka, (Some(a), Some(vs))))
        }
      }
      .takeWhile(_.isDefined)
      .map(_.get)
  }

  def joinAndFold[K, V, A](
    op: (A, V) => A
  )(startValuesIter: Iterator[(K, A)], valuesIter: Iterator[(K, Iterator[V])])(
    implicit keyOrdering: Ordering[K]
  ): Iterator[(K, A)] = {
    for {
      (key, (maybeStartValue, maybeValues)) <- zipByKeyForFold(
        startValuesIter,
        valuesIter
      )
      startValue = maybeStartValue.getOrElse(
        throw new IllegalArgumentException(
          "Must provide a starting value for every key"
        )
      )
    } yield {
      key -> maybeValues.iterator.flatten.foldLeft(startValue)(op)
    }
  }
}
