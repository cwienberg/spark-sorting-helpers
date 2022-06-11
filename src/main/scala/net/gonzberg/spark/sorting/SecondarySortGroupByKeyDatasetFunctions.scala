package net.gonzberg.spark.sorting

import net.gonzberg.spark.sorting.util.GroupByKeyIterator
import net.gonzberg.spark.sorting.util.DatasetRDDLikeFunctions.toDatasetRDDLikeFunctions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset, Encoder}

import scala.language.implicitConversions

final class SecondarySortGroupByKeyDatasetFunctions[K, V](dataset: Dataset[(K, V)]) extends Serializable {

  private def repartitioned(partitionCol: Column, numPartitions: Option[Int]): Dataset[(K, V)] = {
    numPartitions match {
      case Some(num) => dataset.repartition(num, partitionCol)
      case None => dataset.repartition(partitionCol)
    }
  }

  private def groupByKeySortValuesAndMapGroups[T: Encoder](orderExprs: Seq[Column], numPartitions: Option[Int] = None)(mapGroupFunction: ((K, Iterator[V])) => T): Dataset[T] = {
    val keyColumn = col(dataset.columns.head)
    val orderColumns = keyColumn +: orderExprs
    repartitioned(keyColumn, numPartitions)
      .sortWithinPartitions(orderColumns: _*)
      .mapPartitions { partition =>
        val groupByKeyIterator = new GroupByKeyIterator(partition)
        groupByKeyIterator.map(group => mapGroupFunction(group))
      }
  }

  /** Groups by key and sorts the values
   * @param numPartitions the number of partitions for shuffling
   * @param orderExprs the column(s) to order by within each group
   * @return Dataset of keys and sorted values
   */
  def sortedGroupByKey(numPartitions: Int, orderExprs: Column*)(implicit kSeqVEncoder: Encoder[(K, Seq[V])]): Dataset[(K, Seq[V])] = {
    groupByKeySortValuesAndMapGroups(orderExprs, Some(numPartitions)){
      group => (group._1, group._2.toSeq)
    }
  }

  /** Groups by key and sorts the values
   * @param orderExprs the column(s) to order by within each group
   * @return Dataset of keys and sorted values
   */
  def sortedGroupByKey(orderExprs: Column*)(implicit kSeqVEncoder: Encoder[(K, Seq[V])]): Dataset[(K, Seq[V])] = {
    groupByKeySortValuesAndMapGroups(orderExprs){
      group => (group._1, group._2.toSeq)
    }
  }

}

object SecondarySortGroupByKeyDatasetFunctions {
  implicit def datasetToSecondarySortGroupByKeyDatasetFunctions[K, V](dataset: Dataset[(K, V)]): SecondarySortGroupByKeyDatasetFunctions[K, V] = {
    new SecondarySortGroupByKeyDatasetFunctions(dataset)
  }
}
