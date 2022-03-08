package net.gonzberg.spark.sorting

private[sorting] case class SecondarySortKey[K: Ordering, V: Ordering](
  key: K,
  value: V
) {
  def toTuple: (K, V) = (key, value)
}

private[sorting] object SecondarySortKey {
  implicit def ordering[K: Ordering, V: Ordering]
    : Ordering[SecondarySortKey[K, V]] = Ordering.by(_.toTuple)

  def apply[K: Ordering, V: Ordering](kv: (K, V)): SecondarySortKey[K, V] = {
    new SecondarySortKey(kv._1, kv._2)
  }
}
