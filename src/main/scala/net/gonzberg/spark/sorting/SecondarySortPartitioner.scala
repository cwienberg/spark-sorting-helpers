package net.gonzberg.spark.sorting

import org.apache.spark.Partitioner

private[sorting] class SecondarySortPartitioner(partitioner: Partitioner)
    extends Partitioner
    with Serializable {

  override def numPartitions: Int = partitioner.numPartitions

  override def getPartition(key: Any): Int = {
    key match {
      case value: SecondarySortKey[_, _] =>
        partitioner.getPartition(value.key)
      case _ =>
        partitioner.getPartition(key)
    }
  }

  override def equals(obj: Any): Boolean = partitioner.equals(obj)
}
