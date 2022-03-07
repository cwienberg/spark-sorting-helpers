package cwienberg.spark.sorting

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

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
}
