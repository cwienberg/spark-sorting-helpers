package cwienberg.spark.sorting

private[sorting] class OuterJoinIterator[K: Ordering, A, B, C, D](iterA: GroupByKeyIterator[K,A], iterB: GroupByKeyIterator[K,B], iterC: GroupByKeyIterator[K,C], iterD: GroupByKeyIterator[K,D]) extends Iterator[(K, (Option[A], Option[B], Option[C], Option[D]))] {
  val bufferedIterA: BufferedIterator[(K, Iterator[A])] = iterA.buffered
  val bufferedIterB: BufferedIterator[(K, Iterator[B])] = iterB.buffered
  val bufferedIterC: BufferedIterator[(K, Iterator[C])] = iterC.buffered
  val bufferedIterD: BufferedIterator[(K, Iterator[D])] = iterD.buffered

  var nextGroupIterator: Iterator[(K, (Option[A], Option[B], Option[C], Option[D]))] = Iterator.empty

  private def prepareForNextGroupIterator[T](iter: BufferedIterator[(K, Iterator[T])], nextKey: K): Option[Iterator[Option[T]]] = {
    val isNextKey = iter.headOption.filter(_._1 == nextKey).isDefined
    if (isNextKey) {
      Option(iter.next()._2.map(Some(_))).filter(_.nonEmpty)
    } else {
      None
    }
  }

  private def setNextGroupIterator(): Unit = {
    val (nextA, nextB, nextC, nextD) = (bufferedIterA.headOption, bufferedIterB.headOption, bufferedIterC.headOption, bufferedIterD.headOption)
    val minKey = Array(nextA, nextB, nextC, nextD).flatten.map(_._1).min
    val iter = (
      prepareForNextGroupIterator(bufferedIterA, minKey),
      prepareForNextGroupIterator(bufferedIterB, minKey),
      prepareForNextGroupIterator(bufferedIterC, minKey),
      prepareForNextGroupIterator(bufferedIterD, minKey)
    ) match {
      case (None, None, None, Some(ds)) => for {
        d <- ds
      } yield (None, None, None, d)
      case (None, None, Some(cs), maybeDs) => for {
        c <- cs
        d <- maybeDs.getOrElse(Iterator(None)).toStream
      } yield (None, None, c, d)
      case (None, Some(bs), maybeCs, maybeDs) => for {
        b <- bs
        c <- maybeCs.getOrElse(Iterator(None)).toStream
        d <- maybeDs.getOrElse(Iterator(None)).toStream
      } yield (None, b, c, d)
      case (Some(as), maybeBs, maybeCs, maybeDs) => for {
        a <- as
        b <- maybeBs.getOrElse(Iterator(None)).toStream
        c <- maybeCs.getOrElse(Iterator(None)).toStream
        d <- maybeDs.getOrElse(Iterator(None)).toStream
      } yield (a, b, c, d)
      case _ => throw new RuntimeException(s"Issue with key ${minKey}")
    }
    nextGroupIterator = iter.map(value => minKey -> value)
  }

  override def hasNext: Boolean = {
    nextGroupIterator.hasNext || bufferedIterA.hasNext || bufferedIterB.hasNext || bufferedIterC.hasNext || bufferedIterD.hasNext
  }

  override def next(): (K, (Option[A], Option[B], Option[C], Option[D])) = {
    if (!hasNext) throw new RuntimeException("Called next() on an empty iterator")
    if (nextGroupIterator.nonEmpty) return nextGroupIterator.next()

    setNextGroupIterator()
    nextGroupIterator.next()
  }
}

private[sorting] object OuterJoinIterator {
  def apply[K: Ordering, A, B, C, D](iterA: GroupByKeyIterator[K,A], iterB: GroupByKeyIterator[K,B], iterC: GroupByKeyIterator[K,C], iterD: GroupByKeyIterator[K,D]): OuterJoinIterator[K, A, B, C, D] = {
    new OuterJoinIterator(iterA, iterB, iterC, iterD)
  }

  def apply[K: Ordering, A, B, C, D](iterA: Iterator[(K,A)], iterB: Iterator[(K,B)], iterC: Iterator[(K,C)], iterD: Iterator[(K,D)]): Iterator[(K, (Option[A], Option[B], Option[C], Option[D]))] = {
    new OuterJoinIterator(
      new GroupByKeyIterator(iterA),
      new GroupByKeyIterator(iterB),
      new GroupByKeyIterator(iterC),
      new GroupByKeyIterator(iterD)
    )
  }

  def apply[K: Ordering, A, B, C](iterA: Iterator[(K,A)], iterB: Iterator[(K,B)], iterC: Iterator[(K,C)]): Iterator[(K, (Option[A], Option[B], Option[C]))] = {
    new OuterJoinIterator[K, A, B, C, Any](
      new GroupByKeyIterator(iterA),
      new GroupByKeyIterator(iterB),
      new GroupByKeyIterator(iterC),
      new GroupByKeyIterator(Iterator.empty)
    ).map {
      case (k, (a, b, c, _)) => (k, (a, b, c))
    }
  }

  def apply[K: Ordering, A, B](iterA: Iterator[(K,A)], iterB: Iterator[(K,B)]): Iterator[(K, (Option[A], Option[B]))] = {
    new OuterJoinIterator[K, A, B, Any, Any](
      new GroupByKeyIterator(iterA),
      new GroupByKeyIterator(iterB),
      new GroupByKeyIterator(Iterator.empty),
      new GroupByKeyIterator(Iterator.empty)
    ).map {
      case (k, (a, b, _, _)) => (k, (a, b))
    }
  }
}
