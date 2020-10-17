package cwienberg.spark.sorting

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RDDSortingHelpers {

  implicit class SecondarySortGroupingPairRDDFunctions[
    K: Ordering: ClassTag,
    V: Ordering: ClassTag
  ](rdd: RDD[(K, V)])
      extends Serializable {

    private def defaultPartitioner: Partitioner =
      Partitioner.defaultPartitioner(rdd)

    private def groupByKeyAndSortValues(
      partitioner: Partitioner
    ): RDD[(K, Iterator[V])] = {
      val secondarySortPartitioner =
        new SecondarySortPartitioner[K, V](partitioner)

      rdd
        .map((_, ()))
        .repartitionAndSortWithinPartitions(secondarySortPartitioner)
        .mapPartitions(_.map(_._1), preservesPartitioning = true)
        .mapPartitions(new GroupByKeyIterator(_))
    }

    /**
      * Groups by key and sorts the values by some implicit ordering
      * @param partitioner the partitioner for shuffling
      * @return PairRDD of keys and sorted values
      */
    def sortedGroupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = {
      groupByKeyAndSortValues(partitioner).mapValues(_.toVector)
    }

    /**
      * Groups by key and sorts the values by some implicit ordering
      * @param numPartitions the number of partitions for shuffling
      * @return PairRDD of keys and sorted values
      */
    def sortedGroupByKey(numPartitions: Int): RDD[(K, Iterable[V])] = {
      val partitioner = new HashPartitioner(numPartitions)
      sortedGroupByKey(partitioner)
    }

    /**
      * Groups by key and sorts the values by some implicit ordering
      * @return a PairRDD of keys and sorted values
      */
    def sortedGroupByKey: RDD[(K, Iterable[V])] = {
      sortedGroupByKey(defaultPartitioner)
    }

    /**
      * Groups by key and applies a binary operation using foldLeft
      * over the values sorted by some implicit ordering
      * @param startValue the start value for the fold
      * @param op the binary operation for folding
      * @param partitioner the partitioner for shuffling
      * @tparam A the result type of the folding operation
      * @return PairRDD with keys and values, where values are the result
      *         of applying foldLeft across the sorted values
      */
    def sortedFoldLeftByKey[A: ClassTag](
      startValue: A,
      op: (A, V) => A,
      partitioner: Partitioner
    ): RDD[(K, A)] = {
      groupByKeyAndSortValues(partitioner)
        .mapValues(_.foldLeft(startValue)(op))
    }

    /**
      * Groups by key and applies a binary operation using foldLeft
      * over the values sorted by some implicit ordering
      * @param startValue the start value for the fold
      * @param op the binary operation for folding
      * @param numPartitions the number of partitions for shuffling
      * @tparam A the result type of the folding operation
      * @return PairRDD with keys and values, where values are the result
      *         of applying foldLeft across the sorted values
      */
    def sortedFoldLeftByKey[A: ClassTag](
      startValue: A,
      op: (A, V) => A,
      numPartitions: Int
    ): RDD[(K, A)] = {
      val partitioner = new HashPartitioner(numPartitions)
      sortedFoldLeftByKey(startValue, op, partitioner)
    }

    /**
      * Groups by key and applies a binary operation using foldLeft
      * over the values sorted by some implicit ordering
      * @param startValue the start value for the fold
      * @param op the binary operation for folding
      * @tparam A the result type of the folding operation
      * @return PairRDD with keys and values, where values are the result
      *         of applying foldLeft across the sorted values
      */
    def sortedFoldLeftByKey[A: ClassTag](
      startValue: A,
      op: (A, V) => A
    ): RDD[(K, A)] = {
      sortedFoldLeftByKey(startValue, op, defaultPartitioner)
    }

    private implicit def resourceValueOrdering[R]: Ordering[Either[R, V]] =
      new Ordering[Either[R, V]] {
        override def compare(x: Either[R, V], y: Either[R, V]): Int = {
          (x, y) match {
            case (Left(_), Left(_)) =>
              throw new IllegalArgumentException(
                "Cannot compare two resources. Do not provide two resources for the same key."
              )
            case (Left(_), Right(_))    => -1
            case (Right(_), Left(_))    => 1
            case (Right(xv), Right(yv)) => Ordering[V].compare(xv, yv)
          }
        }
      }

    /**
      * Applies op to every value with some resource, where values and resources
      * share the same key. This allows you to send data to executors based on key,
      * so that:
      * (1) the entire set of resources are not held in memory on all executors; and,
      * (2) a specific resource is not duplicated; it is reused for all corresponding
      *     data values
      * One example usage might be when conducting a geospatial operation. If the keys
      * indicate a geographic area, and the value contains geospatial resources in that
      * geographic area, one can apply a method using geospatially local resources
      * to all values while reducing data duplication and shuffling.
      * @param resources a PairRDD of keys and resources, where keys are used to
      *                  determine what data the resource is associated with for
      *                  the operation. There must be exactly one resource for
      *                  each key in the RDD this method is applied to
      * @param op The operation to apply to each value. The operation takes a resource
      *           and returns a function, which will then be applied to each value.
      * @param partitioner the partitioner for shuffling
      * @tparam R the type of resources being used
      * @tparam A the type returned by applying the operation with the resource to each value
      * @return PairRDD of values transformed by applying the operation with the appropriate
      *         resource
      */
    def mapValuesWithKeyedResource[R: ClassTag, A: ClassTag](
      resources: RDD[(K, R)],
      op: R => V => A,
      partitioner: Partitioner
    ): RDD[(K, A)] = {
      val preppedResources: RDD[(K, Either[R, V])] =
        resources.mapValues(r => Left(r))
      val values: RDD[(K, Either[R, V])] = rdd.mapValues(v => Right(v))
      val combined: RDD[(K, Either[R, V])] = preppedResources.union(values)
      val combinedAndSorted: RDD[(K, Iterator[Either[R, V]])] =
        new SecondarySortGroupingPairRDDFunctions(combined)
          .groupByKeyAndSortValues(partitioner)

      combinedAndSorted.flatMapValues { resourceThenValues =>
        val resource = resourceThenValues
          .next()
          .left
          .getOrElse(
            throw new IllegalArgumentException(
              "Must provide a resource for every key"
            )
          )
        val valueFunction = op(resource)
        resourceThenValues.map(_.right.get).map(valueFunction)
      }
    }

    /**
      * Applies op to every value with some resource, where values and resources
      * share the same key. This allows you to send data to executors based on key,
      * so that:
      * (1) the entire set of resources are not held in memory on all executors; and,
      * (2) a specific resource is not duplicated; it is reused for all corresponding
      *     data values
      * One example usage might be when conducting a geospatial operation. If the keys
      * indicate a geographic area, and the value contains geospatial resources in that
      * geographic area, one can apply a method using geospatially local resources
      * to all values while reducing data duplication and shuffling.
      * @param resources a PairRDD of keys and resources, where keys are used to
      *                  determine what data the resource is associated with for
      *                  the operation. There must be exactly one resource for
      *                  each key in the RDD this method is applied to
      * @param op The operation to apply to each value. The operation takes a resource
      *           and returns a function, which will then be applied to each value.
      * @param numPartitions the number of partitions for shuffling
      * @tparam R the type of resources being used
      * @tparam A the type returned by applying the operation with the resource to each value
      * @return PairRDD of values transformed by applying the operation with the appropriate
      *         resource
      */
    def mapValuesWithKeyedResource[R: ClassTag, A: ClassTag](
      resources: RDD[(K, R)],
      op: R => V => A,
      numPartitions: Int
    ): RDD[(K, A)] = {
      val partitioner = new HashPartitioner(numPartitions)
      mapValuesWithKeyedResource(resources, op, partitioner)
    }

    /**
      * Applies op to every value with some resource, where values and resources
      * share the same key. This allows you to send data to executors based on key,
      * so that:
      * (1) the entire set of resources are not held in memory on all executors; and,
      * (2) a specific resource is not duplicated; it is reused for all corresponding
      *     data values
      * One example usage might be when conducting a geospatial operation. If the keys
      * indicate a geographic area, and the value contains geospatial resources in that
      * geographic area, one can apply a method using geospatially local resources
      * to all values while reducing data duplication and shuffling.
      * @param resources a PairRDD of keys and resources, where keys are used to
      *                  determine what data the resource is associated with for
      *                  the operation. There must be exactly one resource for
      *                  each key in the RDD this method is applied to
      * @param op The operation to apply to each value. The operation takes a resource
      *           and returns a function, which will then be applied to each value.
      * @tparam R the type of resources being used
      * @tparam A the type returned by applying the operation with the resource to each value
      * @return PairRDD of values transformed by applying the operation with the appropriate
      *         resource
      */
    def mapValuesWithKeyedResource[R: ClassTag, A: ClassTag](
      resources: RDD[(K, R)],
      op: R => V => A
    ): RDD[(K, A)] = {
      val partitioner = Partitioner.defaultPartitioner(rdd, resources)
      mapValuesWithKeyedResource(resources, op, partitioner)
    }

  }

  implicit class SecondarySortGroupingValueRDDFunctions[V: Ordering: ClassTag](
    rdd: RDD[V]
  ) extends Serializable {
    private def defaultPartitioner: Partitioner =
      Partitioner.defaultPartitioner(rdd)

    /**
      * Groups an RDD using some method which assigns group identifiers and sorts
      * the values within that grouping
      * @param groupFn a method for assigning group identifiers to values
      * @param partitioner the partitioner for shuffling
      * @tparam K the type of the identifier used for grouping
      * @return RDD of Iterables of values from the original RDD, where
      *         each Iterable is composed of values with the same group identifier
      */
    def groupByAndSort[K: Ordering: ClassTag](
      groupFn: V => K,
      partitioner: Partitioner
    ): RDD[Iterable[V]] = {
      rdd.map(v => groupFn(v) -> v).sortedGroupByKey(partitioner).values
    }

    /**
      * Groups an RDD using some method which assigns group identifiers and sorts
      * the values within that grouping
      * @param groupFn a method for assigning group identifiers to values
      * @param numPartitions the number of partitions for shuffling
      * @tparam K the type of the identifier used for grouping
      * @return RDD of Iterables of values from the original RDD, where
      *         each Iterable is composed of values with the same group identifier
      */
    def groupByAndSort[K: Ordering: ClassTag](
      groupFn: V => K,
      numPartitions: Int
    ): RDD[Iterable[V]] = {
      val partitioner = new HashPartitioner(numPartitions)
      groupByAndSort(groupFn, partitioner)
    }

    /**
      * Groups an RDD using some method which assigns group identifiers and sorts
      * the values within that grouping
      * @param groupFn a method for assigning group identifiers to values
      * @tparam K the type of the identifier used for grouping
      * @return RDD of Iterables of values from the original RDD, where
      *         each Iterable is composed of values with the same group identifier
      */
    def groupByAndSort[K: Ordering: ClassTag](
      groupFn: V => K
    ): RDD[Iterable[V]] = {
      groupByAndSort(groupFn, defaultPartitioner)
    }

    /**
      * Groups an RDD using some method which assigns group identifiers and applies
      * foldLeft to each group of values in sorted order by some implicit Ordering
      * @param groupFn a method for assigning group identifiers to values
      * @param startValue the start value for the fold
      * @param op the binary operation for folding
      * @param partitioner the partitioner for shuffling
      * @tparam K the type of the identifier used for grouping
      * @tparam A the result type of the folding operation
      * @return RDD of values resulting from folding over each grouping
      */
    def sortedFoldLeftBy[K: Ordering: ClassTag, A: ClassTag](
      groupFn: V => K,
      startValue: A,
      op: (A, V) => A,
      partitioner: Partitioner
    ): RDD[A] = {
      rdd
        .map(v => groupFn(v) -> v)
        .sortedFoldLeftByKey(startValue, op, partitioner)
        .values
    }

    /**
      * Groups an RDD using some method which assigns group identifiers and applies
      * foldLeft to each group of values in sorted order by some implicit Ordering
      * @param groupFn a method for assigning group identifiers to values
      * @param startValue the start value for the fold
      * @param op the binary operation for folding
      * @param numPartitions the number of partitions for shuffling
      * @tparam K the type of the identifier used for grouping
      * @tparam A the result type of the folding operation
      * @return RDD of values resulting from folding over each grouping
      */
    def sortedFoldLeftBy[K: Ordering: ClassTag, A: ClassTag](
      groupFn: V => K,
      startValue: A,
      op: (A, V) => A,
      numPartitions: Int
    ): RDD[A] = {
      val partitioner = new HashPartitioner(numPartitions)
      sortedFoldLeftBy(groupFn, startValue, op, partitioner)
    }

    /**
      * Groups an RDD using some method which assigns group identifiers and applies
      * foldLeft to each group of values in sorted order by some implicit Ordering
      * @param groupFn a method for assigning group identifiers to values
      * @param startValue the start value for the fold
      * @param op the binary operation for folding
      * @tparam K the type of the identifier used for grouping
      * @tparam A the result type of the folding operation
      * @return RDD of values resulting from folding over each grouping
      */
    def sortedFoldLeftBy[K: Ordering: ClassTag, A: ClassTag](
      groupFn: V => K,
      startValue: A,
      op: (A, V) => A
    ): RDD[A] = {
      sortedFoldLeftBy(groupFn, startValue, op, defaultPartitioner)
    }
  }
}
