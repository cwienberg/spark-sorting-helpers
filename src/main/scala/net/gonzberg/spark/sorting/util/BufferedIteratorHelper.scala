package net.gonzberg.spark.sorting.util

import scala.collection.BufferedIterator

private[util] object BufferedIteratorHelper {
  // define headOption because 2.11's BufferedIterator doesn't have it
  def iterHeadOption[T](iter: BufferedIterator[T]): Option[T] = {
    if (iter.hasNext) {
      Some(iter.head)
    } else {
      None
    }
  }
}
