package net.gonzberg.spark.sorting

import net.gonzberg.spark.sorting.SortHelpers.{
  joinAndApply,
  modifyResourcePreparationAndOp,
  repartitionAndSort
}
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

final class GroupAndSortByFunctions[K: Ordering: ClassTag, V: ClassTag](
  rdd: RDD[(K, V)]
) extends Serializable {

  private def defaultPartitioner: Partitioner =
    Partitioner.defaultPartitioner(rdd)

  private def groupByKeyAndSortValues[A: Ordering](
    sortBy: V => A,
    partitioner: Partitioner
  ): RDD[(K, Iterator[V])] = {
    repartitionAndSort(rdd, sortBy, partitioner)
      .mapPartitions(new GroupByKeyIterator(_), preservesPartitioning = true)
  }

  /** Groups by key and sorts the values by some implicit ordering
    * @param sortBy how to sort values
    * @param partitioner the partitioner for shuffling
    * @return PairRDD of keys and sorted values
    */
  def sortedGroupByKey[A: Ordering](
    sortBy: V => A,
    partitioner: Partitioner
  ): RDD[(K, Iterable[V])] = {
    groupByKeyAndSortValues(sortBy, partitioner).mapValues(_.toVector)
  }

  /** Groups by key and sorts the values by some implicit ordering
    * @param sortBy how to sort values
    * @param numPartitions the number of partitions for shuffling
    * @return PairRDD of keys and sorted values
    */
  def sortedGroupByKey[A: Ordering](
    sortBy: V => A,
    numPartitions: Int
  ): RDD[(K, Iterable[V])] = {
    val partitioner = new HashPartitioner(numPartitions)
    sortedGroupByKey(sortBy, partitioner)
  }

  /** Groups by key and sorts the values by some implicit ordering
    * @param sortBy how to sort values
    * @return a PairRDD of keys and sorted values
    */
  def sortedGroupByKey[A: Ordering](sortBy: V => A): RDD[(K, Iterable[V])] = {
    sortedGroupByKey(sortBy, defaultPartitioner)
  }

  /** Groups by key and applies a binary operation using foldLeft
    * over the values sorted by some implicit ordering
    * @param startValue the start value for the fold
    * @param op the binary operation for folding
    * @param sortBy how to sort values
    * @param partitioner the partitioner for shuffling
    * @tparam A the result type of the folding operation
    * @return PairRDD with keys and values, where values are the result
    *         of applying foldLeft across the sorted values
    */
  def sortedFoldLeftByKey[S: Ordering, A](
    startValue: A,
    op: (A, V) => A,
    sortBy: V => S,
    partitioner: Partitioner
  ): RDD[(K, A)] = {
    groupByKeyAndSortValues(sortBy, partitioner)
      .mapValues(_.foldLeft(startValue)(op))
  }

  /** Groups by key and applies a binary operation using foldLeft
    * over the values sorted by some implicit ordering
    * @param startValue the start value for the fold
    * @param op the binary operation for folding
    * @param sortBy how to sort values
    * @param numPartitions the number of partitions for shuffling
    * @tparam A the result type of the folding operation
    * @return PairRDD with keys and values, where values are the result
    *         of applying foldLeft across the sorted values
    */
  def sortedFoldLeftByKey[S: Ordering, A](
    startValue: A,
    op: (A, V) => A,
    sortBy: V => S,
    numPartitions: Int
  ): RDD[(K, A)] = {
    val partitioner = new HashPartitioner(numPartitions)
    sortedFoldLeftByKey(startValue, op, sortBy, partitioner)
  }

  /** Groups by key and applies a binary operation using foldLeft
    * over the values sorted by some implicit ordering
    * @param startValue the start value for the fold
    * @param op the binary operation for folding
    * @param sortBy how to sort values
    * @tparam A the result type of the folding operation
    * @return PairRDD with keys and values, where values are the result
    *         of applying foldLeft across the sorted values
    */
  def sortedFoldLeftByKey[S: Ordering, A](
    startValue: A,
    op: (A, V) => A,
    sortBy: V => S
  ): RDD[(K, A)] = {
    sortedFoldLeftByKey(startValue, op, sortBy, defaultPartitioner)
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
    * @param sortBy how to sort values
    * @param partitioner the partitioner for shuffling
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedPreparedResource[R: ClassTag, S: Ordering, A](
    resources: RDD[(K, R)],
    op: R => V => A,
    sortBy: V => S,
    partitioner: Partitioner
  ): RDD[(K, A)] = {
    val repartitionedResources: RDD[(K, R)] =
      resources.repartitionAndSortWithinPartitions(partitioner)
    val repartitionedValues: RDD[(K, Iterator[V])] =
      groupByKeyAndSortValues(sortBy, partitioner)
    repartitionedResources.zipPartitions(
      repartitionedValues,
      preservesPartitioning = true
    )(joinAndApply(op))
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
    * @param sortBy how to sort values
    * @param numPartitions the number of partitions for shuffling
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedPreparedResource[R: ClassTag, S: Ordering, A](
    resources: RDD[(K, R)],
    op: R => V => A,
    sortBy: V => S,
    numPartitions: Int
  ): RDD[(K, A)] = {
    val partitioner = new HashPartitioner(numPartitions)
    mapValuesWithKeyedPreparedResource(resources, op, sortBy, partitioner)
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
    * @param sortBy how to sort values
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedPreparedResource[R: ClassTag, S: Ordering, A](
    resources: RDD[(K, R)],
    op: R => V => A,
    sortBy: V => S
  ): RDD[(K, A)] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, resources)
    mapValuesWithKeyedPreparedResource(resources, op, sortBy, partitioner)
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
    * @param sortBy how to sort values
    * @param partitioner the partitioner for shuffling
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedPreparedResource[R: ClassTag, R1, S: Ordering, A](
    resources: RDD[(K, R)],
    prepareResource: R => R1,
    op: (R1, V) => A,
    sortBy: V => S,
    partitioner: Partitioner
  ): RDD[(K, A)] = {
    val newOp = modifyResourcePreparationAndOp(prepareResource, op)
    mapValuesWithKeyedPreparedResource(resources, newOp, sortBy, partitioner)
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
    * @param sortBy how to sort values
    * @param numPartitions the number of partitions for shuffling
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedPreparedResource[R: ClassTag, R1, S: Ordering, A](
    resources: RDD[(K, R)],
    prepareResource: R => R1,
    op: (R1, V) => A,
    sortBy: V => S,
    numPartitions: Int
  ): RDD[(K, A)] = {
    val partitioner = new HashPartitioner(numPartitions)
    mapValuesWithKeyedPreparedResource(
      resources,
      prepareResource,
      op,
      sortBy,
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
    * @param sortBy how to sort values
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedPreparedResource[R: ClassTag, R1, S: Ordering, A](
    resources: RDD[(K, R)],
    prepareResource: R => R1,
    op: (R1, V) => A,
    sortBy: V => S
  ): RDD[(K, A)] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, resources)
    mapValuesWithKeyedPreparedResource(
      resources,
      prepareResource,
      op,
      sortBy,
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
    * @param sortBy how to sort values
    * @param partitioner the partitioner for shuffling
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedResource[R: ClassTag, S: Ordering, A](
    resources: RDD[(K, R)],
    op: (R, V) => A,
    sortBy: V => S,
    partitioner: Partitioner
  ): RDD[(K, A)] = {
    val newOp = modifyResourcePreparationAndOp(identity[R], op)
    mapValuesWithKeyedPreparedResource(resources, newOp, sortBy, partitioner)
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
    * @param sortBy how to sort values
    * @param numPartitions the number of partitions for shuffling
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedResource[R: ClassTag, S: Ordering, A](
    resources: RDD[(K, R)],
    op: (R, V) => A,
    sortBy: V => S,
    numPartitions: Int
  ): RDD[(K, A)] = {
    val partitioner = new HashPartitioner(numPartitions)
    mapValuesWithKeyedResource(resources, op, sortBy, partitioner)
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
    * @param sortBy how to sort values
    * @tparam R the type of resources being used
    * @tparam A the type returned by applying the operation with the resource to each value
    * @return PairRDD of values transformed by applying the operation with the appropriate
    *         resource
    */
  def mapValuesWithKeyedResource[R: ClassTag, S: Ordering, A](
    resources: RDD[(K, R)],
    op: (R, V) => A,
    sortBy: V => S
  ): RDD[(K, A)] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, resources)
    mapValuesWithKeyedResource(resources, op, sortBy, partitioner)
  }
}

private[sorting] object GroupAndSortByFunctions {
  implicit def rddToGroupByAndSortFunctions[K: Ordering: ClassTag, V: ClassTag](
    rdd: RDD[(K, V)]
  ): GroupAndSortByFunctions[K, V] = {
    new GroupAndSortByFunctions(rdd)
  }
}
