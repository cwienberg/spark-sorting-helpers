package cwienberg.spark.sorting

private[sorting] class GroupByKeyIterator[K, V](iter: Iterator[(K, V)])
    extends Iterator[(K, Iterator[V])] {
  private val bufferedIterator = iter.buffered
  private var prevIterator: Option[Iterator[V]] = None

  private def prevIteratorHasElements: Boolean =
    prevIterator.exists(_.hasNext)

  private def failIfPrevIteratorUnexhasuted(): Unit = {
    if (prevIteratorHasElements)
      throw new IllegalStateException(
        "previously returned iterator is unexhasuted"
      )
  }

  override def hasNext: Boolean = {
    failIfPrevIteratorUnexhasuted()
    bufferedIterator.hasNext
  }

  override def next(): (K, Iterator[V]) = {
    failIfPrevIteratorUnexhasuted()
    val k = bufferedIterator.head._1

    val groupIterator = new Iterator[V] {
      private val key = k
      private var exhaustedByNext: Boolean = false
      override def hasNext: Boolean =
        !exhaustedByNext && bufferedIterator.hasNext && bufferedIterator.head._1 == key

      override def next(): V = {
        if (!hasNext) throw new NoSuchElementException("next on empty iterator")
        val ret = bufferedIterator.next()._2
        exhaustedByNext = !hasNext
        ret
      }
    }
    prevIterator = Some(groupIterator)
    (k, groupIterator)
  }
}

private[sorting] object GroupByKeyIterator {
  def empty[K, V]: GroupByKeyIterator[K, V] = {
    new GroupByKeyIterator(Iterator.empty)
  }
}
