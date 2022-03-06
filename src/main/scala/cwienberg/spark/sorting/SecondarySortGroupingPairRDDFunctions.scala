package cwienberg.spark.sorting

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.language.implicitConversions
import scala.reflect.ClassTag

final class SecondarySortGroupingPairRDDFunctions[
  K: ClassTag,
  V: Ordering: ClassTag
](rdd: RDD[(K, V)])(implicit keyOrdering: Ordering[K])
    extends Serializable {

  import keyOrdering.mkOrderingOps

  import SecondarySortGroupingPairRDDFunctions._

  private def defaultPartitioner: Partitioner =
    Partitioner.defaultPartitioner(rdd)

  private def groupByKeyAndSortValues(
    partitioner: Partitioner
  ): RDD[(K, Iterator[V])] = {
    repartitionAndSort(rdd, partitioner)
      .mapPartitions(new GroupByKeyIterator(_), preservesPartitioning = true)
  }

  /** Groups by key and sorts the values by some implicit ordering
    * @param partitioner the partitioner for shuffling
    * @return PairRDD of keys and sorted values
    */
  def sortedGroupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = {
    groupByKeyAndSortValues(partitioner).mapValues(_.toVector)
  }

  /** Groups by key and sorts the values by some implicit ordering
    * @param numPartitions the number of partitions for shuffling
    * @return PairRDD of keys and sorted values
    */
  def sortedGroupByKey(numPartitions: Int): RDD[(K, Iterable[V])] = {
    val partitioner = new HashPartitioner(numPartitions)
    sortedGroupByKey(partitioner)
  }

  /** Groups by key and sorts the values by some implicit ordering
    * @return a PairRDD of keys and sorted values
    */
  def sortedGroupByKey: RDD[(K, Iterable[V])] = {
    sortedGroupByKey(defaultPartitioner)
  }

  /** Groups by key and applies a binary operation using foldLeft
    * over the values sorted by some implicit ordering
    * @param startValue the start value for the fold
    * @param op the binary operation for folding
    * @param partitioner the partitioner for shuffling
    * @tparam A the result type of the folding operation
    * @return PairRDD with keys and values, where values are the result
    *         of applying foldLeft across the sorted values
    */
  def sortedFoldLeftByKey[A](
    startValue: A,
    op: (A, V) => A,
    partitioner: Partitioner
  ): RDD[(K, A)] = {
    groupByKeyAndSortValues(partitioner)
      .mapValues(_.foldLeft(startValue)(op))
  }

  /** Groups by key and applies a binary operation using foldLeft
    * over the values sorted by some implicit ordering
    * @param startValue the start value for the fold
    * @param op the binary operation for folding
    * @param numPartitions the number of partitions for shuffling
    * @tparam A the result type of the folding operation
    * @return PairRDD with keys and values, where values are the result
    *         of applying foldLeft across the sorted values
    */
  def sortedFoldLeftByKey[A](
    startValue: A,
    op: (A, V) => A,
    numPartitions: Int
  ): RDD[(K, A)] = {
    val partitioner = new HashPartitioner(numPartitions)
    sortedFoldLeftByKey(startValue, op, partitioner)
  }

  /** Groups by key and applies a binary operation using foldLeft
    * over the values sorted by some implicit ordering
    * @param startValue the start value for the fold
    * @param op the binary operation for folding
    * @tparam A the result type of the folding operation
    * @return PairRDD with keys and values, where values are the result
    *         of applying foldLeft across the sorted values
    */
  def sortedFoldLeftByKey[A](startValue: A, op: (A, V) => A): RDD[(K, A)] = {
    sortedFoldLeftByKey(startValue, op, defaultPartitioner)
  }

  /** Applies op to every value with some resource, where values and resources
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
  def mapValuesWithKeyedPreparedResource[R: ClassTag, A](
    resources: RDD[(K, R)],
    op: R => V => A,
    partitioner: Partitioner
  ): RDD[(K, A)] = {
    val repartitionedResources: RDD[(K, R)] =
      resources.repartitionAndSortWithinPartitions(partitioner)
    val repartitionedValues: RDD[(K, Iterator[V])] = groupByKeyAndSortValues(
      partitioner
    )
    repartitionedResources.zipPartitions(repartitionedValues, true) {
      (resourcesIter, valuesIter) =>
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
            throw new IllegalArgumentException(
              "Must provide a value for every key"
            )
          )
          _ = require(
            resourceKey >= valueKey,
            "Must provide a value for every key"
          )
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
  }

  /** Applies op to every value with some resource, where values and resources
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
  def mapValuesWithKeyedPreparedResource[R: ClassTag, A](
    resources: RDD[(K, R)],
    op: R => V => A,
    numPartitions: Int
  ): RDD[(K, A)] = {
    val partitioner = new HashPartitioner(numPartitions)
    mapValuesWithKeyedPreparedResource(resources, op, partitioner)
  }

  /** Applies op to every value with some resource, where values and resources
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
  def mapValuesWithKeyedPreparedResource[R: ClassTag, A](
    resources: RDD[(K, R)],
    op: R => V => A
  ): RDD[(K, A)] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, resources)
    mapValuesWithKeyedPreparedResource(resources, op, partitioner)
  }

  /** Applies op to every value with some resource, where values and resources
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
    * @param prepareResource a function to transform the resource into what will be
    *                        used in op
    * @param op the operation to apply to each value. The operation takes a resource
    *           and value and returns the transformed value
    * @param partitioner the partitioner for shuffling
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedPreparedResource[R: ClassTag, R1, A](
    resources: RDD[(K, R)],
    prepareResource: R => R1,
    op: (R1, V) => A,
    partitioner: Partitioner
  ): RDD[(K, A)] = {
    val newOp = modifyResourcePreparationAndOp(prepareResource, op)
    mapValuesWithKeyedPreparedResource(resources, newOp, partitioner)
  }

  /** Applies op to every value with some resource, where values and resources
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
    * @param prepareResource a function to transform the resource into what will be
    *                        used in op
    * @param op the operation to apply to each value. The operation takes a resource
    *           and value and returns the transformed value
    * @param numPartitions the number of partitions for shuffling
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedPreparedResource[R: ClassTag, R1, A](
    resources: RDD[(K, R)],
    prepareResource: R => R1,
    op: (R1, V) => A,
    numPartitions: Int
  ): RDD[(K, A)] = {
    val partitioner = new HashPartitioner(numPartitions)
    mapValuesWithKeyedPreparedResource(
      resources,
      prepareResource,
      op,
      partitioner
    )
  }

  /** Applies op to every value with some resource, where values and resources
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
    * @param prepareResource a function to transform the resource into what will be
    *                        used in op
    * @param op the operation to apply to each value. The operation takes a resource
    *           and value and returns the transformed value
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedPreparedResource[R: ClassTag, R1, A](
    resources: RDD[(K, R)],
    prepareResource: R => R1,
    op: (R1, V) => A
  ): RDD[(K, A)] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, resources)
    mapValuesWithKeyedPreparedResource(
      resources,
      prepareResource,
      op,
      partitioner
    )
  }

  /** Applies op to every value with some resource, where values and resources
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
    * @param op the operation to apply to each value. Takes a resource and value and
    *           returns the transformed value
    * @param partitioner the partitioner for shuffling
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedResource[R: ClassTag, A](
    resources: RDD[(K, R)],
    op: (R, V) => A,
    partitioner: Partitioner
  ): RDD[(K, A)] = {
    val newOp = modifyResourcePreparationAndOp(identity[R], op)
    mapValuesWithKeyedPreparedResource(resources, newOp, partitioner)
  }

  /** Applies op to every value with some resource, where values and resources
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
    * @param op the operation to apply to each value. Takes a resource and value and
    *           returns the transformed value
    * @param numPartitions the number of partitions for shuffling
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedResource[R: ClassTag, A](
    resources: RDD[(K, R)],
    op: (R, V) => A,
    numPartitions: Int
  ): RDD[(K, A)] = {
    val partitioner = new HashPartitioner(numPartitions)
    mapValuesWithKeyedResource(resources, op, partitioner)
  }

  /** Applies op to every value with some resource, where values and resources
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
    * @param op the operation to apply to each value. Takes a resource and value and
    *           returns the transformed value
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedResource[R: ClassTag, A](
    resources: RDD[(K, R)],
    op: (R, V) => A
  ): RDD[(K, A)] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, resources)
    mapValuesWithKeyedResource(resources, op, partitioner)
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    partitioner: Partitioner
  ): RDD[(K, (Option[V], Option[B], Option[C], Option[D]))] = {
    val thisPartitioned = repartitionAndSort(rdd, partitioner)
    val bPartitioned = repartitionAndSort(rddB, partitioner)
    val cPartitioned = repartitionAndSort(rddC, partitioner)
    val dPartitioned = repartitionAndSort(rddD, partitioner)
    thisPartitioned.zipPartitions(
      bPartitioned,
      cPartitioned,
      dPartitioned,
      preservesPartitioning = true
    ) { (iterV, iterB, iterC, iterD) =>
      OuterJoinIterator(iterV, iterB, iterC, iterD)
    }
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    numPartitions: Int
  ): RDD[(K, (Option[V], Option[B], Option[C], Option[D]))] = {
    fullOuterJoinWithSortedValues(
      rddB,
      rddC,
      rddD,
      new HashPartitioner(numPartitions)
    )
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)]
  ): RDD[(K, (Option[V], Option[B], Option[C], Option[D]))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC, rddD)
    fullOuterJoinWithSortedValues(rddB, rddC, rddD, partitioner)
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    partitioner: Partitioner
  ): RDD[(K, (Option[V], Option[B], Option[C]))] = {
    val thisPartitioned = repartitionAndSort(rdd, partitioner)
    val bPartitioned = repartitionAndSort(rddB, partitioner)
    val cPartitioned = repartitionAndSort(rddC, partitioner)
    thisPartitioned.zipPartitions(
      bPartitioned,
      cPartitioned,
      preservesPartitioning = true
    ) { (iterV, iterB, iterC) =>
      OuterJoinIterator(iterV, iterB, iterC)
    }
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    numPartitions: Int
  ): RDD[(K, (Option[V], Option[B], Option[C]))] = {
    fullOuterJoinWithSortedValues(
      rddB,
      rddC,
      new HashPartitioner(numPartitions)
    )
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)]
  ): RDD[(K, (Option[V], Option[B], Option[C]))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC)
    fullOuterJoinWithSortedValues(rddB, rddC, partitioner)
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    partitioner: Partitioner
  ): RDD[(K, (Option[V], Option[B]))] = {
    val thisPartitioned = repartitionAndSort(rdd, partitioner)
    val bPartitioned = repartitionAndSort(rddB, partitioner)
    thisPartitioned.zipPartitions(bPartitioned, preservesPartitioning = true) {
      (iterV, iterB) =>
        OuterJoinIterator(iterV, iterB)
    }
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    numPartitions: Int
  ): RDD[(K, (Option[V], Option[B]))] = {
    fullOuterJoinWithSortedValues(rddB, new HashPartitioner(numPartitions))
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)]
  ): RDD[(K, (Option[V], Option[B]))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB)
    fullOuterJoinWithSortedValues(rddB, partitioner)
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    partitioner: Partitioner
  ): RDD[(K, (V, B, C, D))] = {
    fullOuterJoinWithSortedValues(rddB, rddC, rddD, partitioner)
      .flatMapValues { case (maybeV, maybeB, maybeC, maybeD) =>
        for {
          v <- maybeV
          b <- maybeB
          c <- maybeC
          d <- maybeD
        } yield (v, b, c, d)
      }
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    numPartitions: Int
  ): RDD[(K, (V, B, C, D))] = {
    innerJoinWithSortedValues(
      rddB,
      rddC,
      rddD,
      new HashPartitioner(numPartitions)
    )
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)]
  ): RDD[(K, (V, B, C, D))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC, rddD)
    innerJoinWithSortedValues(rddB, rddC, rddD, partitioner)
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    partitioner: Partitioner
  ): RDD[(K, (V, B, C))] = {
    fullOuterJoinWithSortedValues(rddB, rddC, partitioner).flatMapValues {
      case (maybeV, maybeB, maybeC) =>
        for {
          v <- maybeV
          b <- maybeB
          c <- maybeC
        } yield (v, b, c)
    }
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    numPartitions: Int
  ): RDD[(K, (V, B, C))] = {
    innerJoinWithSortedValues(rddB, rddC, new HashPartitioner(numPartitions))
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)]
  ): RDD[(K, (V, B, C))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC)
    innerJoinWithSortedValues(rddB, rddC, partitioner)
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    partitioner: Partitioner
  ): RDD[(K, (V, B))] = {
    fullOuterJoinWithSortedValues(rddB, partitioner).flatMapValues {
      case (maybeV, maybeB) =>
        for {
          v <- maybeV
          b <- maybeB
        } yield (v, b)
    }
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    numPartitions: Int
  ): RDD[(K, (V, B))] = {
    innerJoinWithSortedValues(rddB, new HashPartitioner(numPartitions))
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)]
  ): RDD[(K, (V, B))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB)
    innerJoinWithSortedValues(rddB, partitioner)
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    partitioner: Partitioner
  ): RDD[(K, (V, Option[B], Option[C], Option[D]))] = {
    fullOuterJoinWithSortedValues(rddB, rddC, rddD, partitioner)
      .flatMapValues { case (maybeV, maybeB, maybeC, maybeD) =>
        for {
          v <- maybeV
        } yield (v, maybeB, maybeC, maybeD)
      }
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    numPartitions: Int
  ): RDD[(K, (V, Option[B], Option[C], Option[D]))] = {
    leftJoinWithSortedValues(
      rddB,
      rddC,
      rddD,
      new HashPartitioner(numPartitions)
    )
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)]
  ): RDD[(K, (V, Option[B], Option[C], Option[D]))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC, rddD)
    leftJoinWithSortedValues(rddB, rddC, rddD, partitioner)
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    partitioner: Partitioner
  ): RDD[(K, (V, Option[B], Option[C]))] = {
    fullOuterJoinWithSortedValues(rddB, rddC, partitioner).flatMapValues {
      case (maybeV, maybeB, maybeC) =>
        for {
          v <- maybeV
        } yield (v, maybeB, maybeC)
    }
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    numPartitions: Int
  ): RDD[(K, (V, Option[B], Option[C]))] = {
    leftJoinWithSortedValues(rddB, rddC, new HashPartitioner(numPartitions))
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)]
  ): RDD[(K, (V, Option[B], Option[C]))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC)
    leftJoinWithSortedValues(rddB, rddC, partitioner)
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    partitioner: Partitioner
  ): RDD[(K, (V, Option[B]))] = {
    fullOuterJoinWithSortedValues(rddB, partitioner).flatMapValues {
      case (maybeV, maybeB) =>
        for {
          v <- maybeV
        } yield (v, maybeB)
    }
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    numPartitions: Int
  ): RDD[(K, (V, Option[B]))] = {
    leftJoinWithSortedValues(rddB, new HashPartitioner(numPartitions))
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)]
  ): RDD[(K, (V, Option[B]))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB)
    leftJoinWithSortedValues(rddB, partitioner)
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    partitioner: Partitioner
  ): RDD[(K, (Option[V], Option[B], Option[C], D))] = {
    fullOuterJoinWithSortedValues(rddB, rddC, rddD, partitioner)
      .flatMapValues { case (maybeV, maybeB, maybeC, maybeD) =>
        for {
          d <- maybeD
        } yield (maybeV, maybeB, maybeC, d)
      }
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    numPartitions: Int
  ): RDD[(K, (Option[V], Option[B], Option[C], D))] = {
    rightJoinWithSortedValues(
      rddB,
      rddC,
      rddD,
      new HashPartitioner(numPartitions)
    )
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)]
  ): RDD[(K, (Option[V], Option[B], Option[C], D))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC, rddD)
    rightJoinWithSortedValues(rddB, rddC, rddD, partitioner)
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    partitioner: Partitioner
  ): RDD[(K, (Option[V], Option[B], C))] = {
    fullOuterJoinWithSortedValues(rddB, rddC, partitioner).flatMapValues {
      case (maybeV, maybeB, maybeC) =>
        for {
          c <- maybeC
        } yield (maybeV, maybeB, c)
    }
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    numPartitions: Int
  ): RDD[(K, (Option[V], Option[B], C))] = {
    rightJoinWithSortedValues(rddB, rddC, new HashPartitioner(numPartitions))
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)]
  ): RDD[(K, (Option[V], Option[B], C))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC)
    rightJoinWithSortedValues(rddB, rddC, partitioner)
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    partitioner: Partitioner
  ): RDD[(K, (Option[V], B))] = {
    fullOuterJoinWithSortedValues(rddB, partitioner).flatMapValues {
      case (maybeV, maybeB) =>
        for {
          b <- maybeB
        } yield (maybeV, b)
    }
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    numPartitions: Int
  ): RDD[(K, (Option[V], B))] = {
    rightJoinWithSortedValues(rddB, new HashPartitioner(numPartitions))
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)]
  ): RDD[(K, (Option[V], B))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB)
    rightJoinWithSortedValues(rddB, partitioner)
  }
}

private object SecondarySortGroupingPairRDDFunctions {
  implicit def rddToSecondarySortGroupingPairRDDFunctions[
    K: Ordering: ClassTag,
    V: Ordering: ClassTag
  ](rdd: RDD[(K, V)]): SecondarySortGroupingPairRDDFunctions[K, V] = {
    new SecondarySortGroupingPairRDDFunctions(rdd)
  }

  private def repartitionAndSort[K: Ordering, V: Ordering](
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

  private def modifyResourcePreparationAndOp[R, R1, V, A](
    prepareResource: R => R1,
    op: (R1, V) => A
  ): R => V => A = {
    def newOp(resource: R): V => A = {
      val preparedResource = prepareResource(resource)
      op(preparedResource, _)
    }
    newOp
  }
}
