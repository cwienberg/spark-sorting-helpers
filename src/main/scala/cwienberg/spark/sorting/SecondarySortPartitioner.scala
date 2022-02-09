package cwienberg.spark.sorting

import org.apache.spark.Partitioner

private[sorting] class SecondarySortPartitioner(
  partitioner: Partitioner
) extends Partitioner
    with Serializable {

  override def numPartitions: Int = partitioner.numPartitions

  override def getPartition(key: Any): Int = {
    if (key.isInstanceOf[SecondarySortKey[_, _]]) {
      partitioner.getPartition(key.asInstanceOf[SecondarySortKey[_, _]].key)
    } else {
      partitioner.getPartition(key)
    }
  }

  override def equals(obj: Any): Boolean = partitioner.equals(obj)
}
