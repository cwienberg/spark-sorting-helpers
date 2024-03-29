package net.gonzberg.spark.sorting

import net.gonzberg.spark.sorting.util.GroupByKeyIterator
import net.gonzberg.spark.sorting.util.SortHelpers.joinAndFold
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset, Encoder}

import scala.language.implicitConversions
import scala.reflect.ClassTag

final class SecondarySortGroupByKeyDatasetFunctions[K, V](
  dataset: Dataset[(K, V)]
) extends Serializable {

  private def groupByKeySortValuesAndMapGroups[T: Encoder](
    orderExprs: Seq[Column],
    numPartitions: Option[Int] = None
  )(mapGroupFunction: ((K, Iterator[V])) => T): Dataset[T] = {
    SecondarySortGroupByKeyDatasetFunctions
      .repartitionAndSort(dataset, numPartitions, orderExprs)
      .mapPartitions { partition =>
        val groupByKeyIterator = new GroupByKeyIterator(partition)
        groupByKeyIterator.map(group => mapGroupFunction(group))
      }
  }

  private def sortedGroupByKey(
    numPartitions: Option[Int],
    orderExprs: Seq[Column]
  )(implicit kSeqVEncoder: Encoder[(K, Seq[V])]): Dataset[(K, Seq[V])] = {
    groupByKeySortValuesAndMapGroups(orderExprs, numPartitions) {
      case (key, values) => (key, values.toSeq)
    }
  }

  /** Groups by key and sorts the values
    * @param numPartitions the number of partitions for shuffling
    * @param orderExprs the column(s) to order by within each group
    * @return Dataset of keys and sorted values
    */
  def sortedGroupByKey(numPartitions: Int, orderExprs: Column*)(implicit
    kSeqVEncoder: Encoder[(K, Seq[V])]
  ): Dataset[(K, Seq[V])] = {
    sortedGroupByKey(Some(numPartitions), orderExprs)
  }

  /** Groups by key and sorts the values
    * @param orderExprs the column(s) to order by within each group
    * @return Dataset of keys and sorted values
    */
  def sortedGroupByKey(
    orderExprs: Column*
  )(implicit kSeqVEncoder: Encoder[(K, Seq[V])]): Dataset[(K, Seq[V])] = {
    sortedGroupByKey(None, orderExprs)
  }

  private def sortedFoldLeftByKey[A](
    startValue: A,
    op: (A, V) => A,
    numPartitions: Option[Int],
    orderExprs: Seq[Column]
  )(implicit kSeqVEncoder: Encoder[(K, A)]): Dataset[(K, A)] = {
    groupByKeySortValuesAndMapGroups(orderExprs, numPartitions) {
      case (key, values) => (key, values.foldLeft(startValue)(op))
    }
  }

  /** Groups by key and applies a binary operation using foldLeft
    * over the values sorted by some implicit ordering
    * @param startValue the start value for the fold
    * @param op the binary operation for folding
    * @param numPartitions the number of partitions
    * @param orderExprs the column(s) to order by within each group
    * @tparam A the result type of the folding operation
    * @return Dataset with keys and values, where values are the result
    *         of applying foldLeft across the sorted values
    */
  def sortedFoldLeftByKey[A](
    startValue: A,
    op: (A, V) => A,
    numPartitions: Int,
    orderExprs: Column*
  )(implicit kSeqVEncoder: Encoder[(K, A)]): Dataset[(K, A)] = {
    sortedFoldLeftByKey(startValue, op, Some(numPartitions), orderExprs)
  }

  /** Groups by key and applies a binary operation using foldLeft
    * over the values sorted by some implicit ordering
    * @param startValue the start value for the fold
    * @param op the binary operation for folding
    * @param orderExprs the column(s) to order by within each group
    * @tparam A the result type of the folding operation
    * @return Dataset with keys and values, where values are the result
    *         of applying foldLeft across the sorted values
    */
  def sortedFoldLeftByKey[A](
    startValue: A,
    op: (A, V) => A,
    orderExprs: Column*
  )(implicit kaEncoder: Encoder[(K, A)]): Dataset[(K, A)] = {
    sortedFoldLeftByKey(startValue, op, None, orderExprs)
  }

  private def sortedFoldLeftWithKeyedStartValues[A: ClassTag](
    startValues: Dataset[(K, A)],
    op: (A, V) => A,
    numPartitions: Option[Int],
    orderExprs: Seq[Column]
  )(implicit
    keyOrdering: Ordering[K],
    kaEncoder: Encoder[(K, A)]
  ): Dataset[(K, A)] = {
    val repartitionedStartValues = SecondarySortGroupByKeyDatasetFunctions
      .repartitionAndSort(startValues, numPartitions)
      .rdd
    val repartitionedValues = SecondarySortGroupByKeyDatasetFunctions
      .repartitionAndSort(dataset, numPartitions, orderExprs)
      .rdd
    val kaRDD = repartitionedStartValues
      .zipPartitions(
        repartitionedValues.mapPartitions(
          new GroupByKeyIterator(_),
          preservesPartitioning = true
        ),
        preservesPartitioning = true
      )(joinAndFold(op))
    dataset.sparkSession.createDataset(kaRDD)
  }

  /** Groups by key and applies a binary operation using foldLeft
    * over the values sorted by some implicit ordering
    * @param startValues a Dataset of start values by key
    * @param op the binary operation for folding
    * @param numPartitions the number of partitions
    * @param orderExprs the column(s) to order by within each group
    * @tparam A the result type of the folding operation
    * @return Dataset with keys and values, where values are the result
    *         of applying foldLeft across the sorted values
    */
  def sortedFoldLeftWithKeyedStartValues[A: ClassTag](
    startValues: Dataset[(K, A)],
    op: (A, V) => A,
    numPartitions: Int,
    orderExprs: Column*
  )(implicit
    keyOrdering: Ordering[K],
    kaEncoder: Encoder[(K, A)]
  ): Dataset[(K, A)] = {
    sortedFoldLeftWithKeyedStartValues(
      startValues,
      op,
      Some(numPartitions),
      orderExprs
    )
  }

  /** Groups by key and applies a binary operation using foldLeft
    * over the values sorted by some implicit ordering
    * @param startValues a Dataset of start values by key
    * @param op the binary operation for folding
    * @param orderExprs the column(s) to order by within each group
    * @tparam A the result type of the folding operation
    * @return Dataset with keys and values, where values are the result
    *         of applying foldLeft across the sorted values
    */
  def sortedFoldLeftWithKeyedStartValues[A: ClassTag](
    startValues: Dataset[(K, A)],
    op: (A, V) => A,
    orderExprs: Column*
  )(implicit
    keyOrdering: Ordering[K],
    kaEncoder: Encoder[(K, A)]
  ): Dataset[(K, A)] = {
    sortedFoldLeftWithKeyedStartValues(startValues, op, None, orderExprs)
  }
}

private[sorting] object SecondarySortGroupByKeyDatasetFunctions {
  implicit def datasetToSecondarySortGroupByKeyDatasetFunctions[K, V](
    dataset: Dataset[(K, V)]
  ): SecondarySortGroupByKeyDatasetFunctions[K, V] = {
    new SecondarySortGroupByKeyDatasetFunctions(dataset)
  }

  private def repartitionAndSort[K, T](
    dataset: Dataset[(K, T)],
    numPartitions: Option[Int] = None,
    orderExprs: Seq[Column] = Seq.empty[Column]
  ): Dataset[(K, T)] = {
    val keyColumn = col(dataset.columns.head)
    val repartitioned = numPartitions match {
      case Some(num) => dataset.repartition(num, keyColumn)
      case None      => dataset.repartition(keyColumn)
    }
    val orderColumns = keyColumn +: orderExprs
    repartitioned.sortWithinPartitions(orderColumns: _*)
  }
}
