package net.gonzberg.spark.sorting

import org.apache.spark.sql.expressions.{Aggregator, Window, WindowSpec}
import org.apache.spark.sql.{Column, Dataset, Encoder, functions}

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

final object SecondarySortGroupByDatasetFunctions {

  private[sorting] final class FoldLeftUDAF[T, U](
    startValue: U,
    op: (U, T) => U
  )(implicit resultEncoder: Encoder[U])
      extends Aggregator[T, U, U] {
    override def zero: U = startValue

    override def reduce(u: U, t: T): U = op(u, t)

    override def merge(u1: U, u2: U): U = {
      throw new RuntimeException("Should never reach this")
    }

    override def finish(reduction: U): U = reduction

    override def bufferEncoder: Encoder[U] = resultEncoder

    override def outputEncoder: Encoder[U] = resultEncoder
  }

  final class SecondarySortedDatasetFunctions[
    T: Encoder: TypeTag
  ] private[sorting] (
    dataset: Dataset[T],
    partitionColumns: Option[Seq[Column]] = None,
    orderColumns: Option[Seq[Column]] = None
  ) {

    def assertHasPartitioningAndOrdering(): Unit = assert(
      partitionColumns.nonEmpty && orderColumns.nonEmpty,
      "Must specify partition and ordering expressions"
    )

    def partitionBy(
      partitionCol: String,
      partitionCols: String*
    ): SecondarySortedDatasetFunctions[T] = {
      val partitionExprs = (partitionCol +: partitionCols).map(new Column(_))
      partitionBy(partitionExprs: _*)
    }

    def partitionBy(
      partitionExprs: Column*
    ): SecondarySortedDatasetFunctions[T] = {
      new SecondarySortedDatasetFunctions(
        dataset,
        Some(partitionExprs),
        orderColumns
      )
    }

    def secondarySortBy(
      orderCol: String,
      orderCols: String*
    ): SecondarySortedDatasetFunctions[T] = {
      secondarySortBy((orderCol +: orderCols).map(new Column(_)): _*)
    }

    def secondarySortBy(
      orderExprs: Column*
    ): SecondarySortedDatasetFunctions[T] = {
      new SecondarySortedDatasetFunctions(
        dataset,
        partitionColumns,
        Some(orderExprs)
      )
    }

    def foldLeft[U: Encoder](startValue: U, op: (U, T) => U): Dataset[U] = {
      assertHasPartitioningAndOrdering()

      val udaf = functions.udaf(new FoldLeftUDAF(startValue, op))
      val datasetColumns = dataset.columns.toSeq.map(new Column(_))

      val foldLeftWindow = Window
        .partitionBy(partitionColumns.get: _*)
        .orderBy(orderColumns.get: _*)
      val maxRowWindow = Window.partitionBy(partitionColumns.get: _*)

      dataset
        .select(
          partitionColumns.get :+
            functions.row_number().over(foldLeftWindow).as("row_number") :+
            udaf
              .apply(datasetColumns: _*)
              .over(foldLeftWindow)
              .as("foldLeftResult"): _*
        )
        .select(
          functions
            .max(new Column("row_number"))
            .over(maxRowWindow)
            .as("max_row_number"),
          new Column("row_number"),
          new Column("foldLeftResult")
        )
        .filter(new Column("max_row_number") === new Column("row_number"))
        .select(new Column("foldLeftResult.*"))
        .as[U]
    }
  }

  implicit def datasetToSecondarySortedDatasetFunctions[T: Encoder: TypeTag](
    dataset: Dataset[T]
  ): SecondarySortedDatasetFunctions[T] = {
    new SecondarySortedDatasetFunctions(dataset)
  }

}
