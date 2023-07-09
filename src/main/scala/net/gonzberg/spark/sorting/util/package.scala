package net.gonzberg.spark.sorting

package object util {
  type GroupedByKeyIterator[K, V] = Iterator[(K, Iterator[V])]
}
