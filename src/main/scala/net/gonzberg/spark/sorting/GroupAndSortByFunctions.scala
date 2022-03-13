package net.gonzberg.spark.sorting

import net.gonzberg.spark.sorting.SortHelpers.repartitionAndSort
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

final class GroupAndSortByFunctions[
  K: Ordering: ClassTag,
  V: ClassTag
](rdd: RDD[(K, V)])
    extends Serializable {

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
  def sortedGroupByKey[A: Ordering](sortBy: V => A, partitioner: Partitioner): RDD[(K, Iterable[V])] = {
    groupByKeyAndSortValues(sortBy, partitioner).mapValues(_.toVector)
  }

  /** Groups by key and sorts the values by some implicit ordering
   * @param sortBy how to sort values
   * @param numPartitions the number of partitions for shuffling
   * @return PairRDD of keys and sorted values
   */
  def sortedGroupByKey[A: Ordering](sortBy: V => A, numPartitions: Int): RDD[(K, Iterable[V])] = {
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
  def sortedFoldLeftByKey[S: Ordering, A](startValue: A, op: (A, V) => A,
    sortBy: V => S): RDD[(K, A)] = {
    sortedFoldLeftByKey(startValue, op, sortBy, defaultPartitioner)
  }
}
