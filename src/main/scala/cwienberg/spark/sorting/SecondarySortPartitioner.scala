package cwienberg.spark.sorting

import org.apache.spark.Partitioner

import scala.reflect.ClassTag

private[sorting] class SecondarySortPartitioner[K: ClassTag, V: ClassTag](
  partitioner: Partitioner
) extends Partitioner
    with Serializable {

  override def numPartitions: Int = partitioner.numPartitions

  override def getPartition(row: Any): Int = {
    if (row.isInstanceOf[(_, _)]) {
      val (k, _) = row.asInstanceOf[(K, V)]
      partitioner.getPartition(k)
    } else {
      partitioner.getPartition(row)
    }
  }

  override def equals(obj: Any): Boolean = partitioner.equals(obj)
}
