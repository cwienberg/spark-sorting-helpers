package cwienberg.spark.sorting

private[sorting] case class SecondarySortKey[K: Ordering, V: Ordering](
  key: K,
  value: V
) extends Ordered[SecondarySortKey[K, V]] {
  override def compare(that: SecondarySortKey[K, V]): Int = {
    implicitly[Ordering[(K, V)]]
      .compare((this.key, this.value), (that.key, that.value))
  }

  def toTuple: (K, V) = (key, value)
}

private[sorting] object SecondarySortKey {
  def apply[K: Ordering, V: Ordering](kv: (K, V)): SecondarySortKey[K, V] = {
    new SecondarySortKey(kv._1, kv._2)
  }
}
