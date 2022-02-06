package cwienberg.spark.sorting

import org.apache.spark.Partitioner

import scala.reflect.ClassTag

private[sorting] class SecondarySortPartitioner[K: ClassTag, V: ClassTag](
  partitioner: Partitioner
) extends Partitioner
    with Serializable {

  override def numPartitions: Int = partitioner.numPartitions

  override def getPartition(key: Any): Int = {
    if (key.isInstanceOf[SecondarySortKey[_, _]]) {
      partitioner.getPartition(key.asInstanceOf[SecondarySortKey[K, V]].key)
    } else {
      partitioner.getPartition(key)
    }
  }

  override def equals(obj: Any): Boolean = partitioner.equals(obj)
}
