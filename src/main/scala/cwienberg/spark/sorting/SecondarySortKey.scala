package cwienberg.spark.sorting

import scala.reflect.ClassTag

private[sorting] case class SecondarySortKey[K: Ordering: ClassTag, V: Ordering: ClassTag](key: K, value: V) extends Ordered[SecondarySortKey[K,V]] {
  override def compare(that: SecondarySortKey[K, V]): Int = {
    implicitly[Ordering[(K, V)]].compare((this.key, this.value), (that.key, that.value))
  }

  def toTuple: (K, V) = (key, value)
}

private[sorting] object SecondarySortKey {
  def apply[K: ClassTag: Ordering, V: ClassTag: Ordering](kv: (K, V)): SecondarySortKey[K,V] = {
    new SecondarySortKey(kv._1, kv._2)
  }
}