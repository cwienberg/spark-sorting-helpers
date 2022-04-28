package net.gonzberg.spark.sorting

import scala.collection.BufferedIterator
import scala.collection.compat.immutable.LazyList
import scala.reflect.ClassTag

private[sorting] class OuterJoinIterator[K: Ordering: ClassTag, A, B, C, D](
  iterA: GroupByKeyIterator[K, A],
  iterB: GroupByKeyIterator[K, B],
  iterC: GroupByKeyIterator[K, C],
  iterD: GroupByKeyIterator[K, D]
) extends Iterator[(K, (Option[A], Option[B], Option[C], Option[D]))] {
  val bufferedIterA: BufferedIterator[(K, Iterator[A])] = iterA.buffered
  val bufferedIterB: BufferedIterator[(K, Iterator[B])] = iterB.buffered
  val bufferedIterC: BufferedIterator[(K, Iterator[C])] = iterC.buffered
  val bufferedIterD: BufferedIterator[(K, Iterator[D])] = iterD.buffered

  var nextGroupIterator
    : Iterator[(K, (Option[A], Option[B], Option[C], Option[D]))] =
    Iterator.empty

  private def prepareForNextGroupIterator[T](
    iter: BufferedIterator[(K, Iterator[T])],
    nextKey: K
  ): Option[Iterator[Option[T]]] = {
    val isNextKey = iter.hasNext && iter.head._1 == nextKey
    if (isNextKey) {
      Option(iter.next()._2.map(Some(_))).filter(_.nonEmpty)
    } else {
      None
    }
  }

  // define headOption because 2.11's BufferedIterator doesn't have it
  private def iterHeadOption[T](iter: BufferedIterator[T]): Option[T] = {
    if (iter.hasNext) {
      Some(iter.head)
    } else {
      None
    }
  }

  private def setNextGroupIterator(): Unit = {
    val (nextA, nextB, nextC, nextD) = (
      iterHeadOption(bufferedIterA),
      iterHeadOption(bufferedIterB),
      iterHeadOption(bufferedIterC),
      iterHeadOption(bufferedIterD)
    )
    val minKey = Array(nextA, nextB, nextC, nextD).flatten.map(_._1).min
    val iter = (
      prepareForNextGroupIterator(bufferedIterA, minKey),
      prepareForNextGroupIterator(bufferedIterB, minKey),
      prepareForNextGroupIterator(bufferedIterC, minKey),
      prepareForNextGroupIterator(bufferedIterD, minKey)
    ) match {
      case (None, None, None, Some(ds)) =>
        for {
          d <- ds
        } yield (None, None, None, d)
      case (None, None, Some(cs), maybeDs) =>
        val ds = maybeDs.getOrElse(Iterator(None)).to(LazyList)
        for {
          c <- cs
          d <- ds
        } yield (None, None, c, d)
      case (None, Some(bs), maybeCs, maybeDs) =>
        val cs = maybeCs.getOrElse(Iterator(None)).to(LazyList)
        val ds = maybeDs.getOrElse(Iterator(None)).to(LazyList)
        for {
          b <- bs
          c <- cs
          d <- ds
        } yield (None, b, c, d)
      case (Some(as), maybeBs, maybeCs, maybeDs) =>
        val bs = maybeBs.getOrElse(Iterator(None)).to(LazyList)
        val cs = maybeCs.getOrElse(Iterator(None)).to(LazyList)
        val ds = maybeDs.getOrElse(Iterator(None)).to(LazyList)
        for {
          a <- as
          b <- bs
          c <- cs
          d <- ds
        } yield (a, b, c, d)
      case _ => throw new RuntimeException(s"Issue with key $minKey")
    }
    nextGroupIterator = iter.map(value => minKey -> value)
  }

  override def hasNext: Boolean = {
    nextGroupIterator.hasNext || bufferedIterA.hasNext || bufferedIterB.hasNext || bufferedIterC.hasNext || bufferedIterD.hasNext
  }

  override def next(): (K, (Option[A], Option[B], Option[C], Option[D])) = {
    if (!hasNext)
      throw new RuntimeException("Called next() on an empty iterator")
    if (nextGroupIterator.nonEmpty) return nextGroupIterator.next()

    setNextGroupIterator()
    nextGroupIterator.next()
  }
}

private[sorting] object OuterJoinIterator {
  def apply[K: Ordering: ClassTag, A, B, C, D](
    iterA: Iterator[(K, A)],
    iterB: Iterator[(K, B)],
    iterC: Iterator[(K, C)],
    iterD: Iterator[(K, D)]
  ): Iterator[(K, (Option[A], Option[B], Option[C], Option[D]))] = {
    new OuterJoinIterator(
      new GroupByKeyIterator(iterA),
      new GroupByKeyIterator(iterB),
      new GroupByKeyIterator(iterC),
      new GroupByKeyIterator(iterD)
    )
  }

  def apply[K: Ordering: ClassTag, A, B, C](
    iterA: Iterator[(K, A)],
    iterB: Iterator[(K, B)],
    iterC: Iterator[(K, C)]
  ): Iterator[(K, (Option[A], Option[B], Option[C]))] = {
    new OuterJoinIterator[K, A, B, C, Any](
      new GroupByKeyIterator(iterA),
      new GroupByKeyIterator(iterB),
      new GroupByKeyIterator(iterC),
      new GroupByKeyIterator(Iterator.empty)
    ).map { case (k, (a, b, c, _)) =>
      (k, (a, b, c))
    }
  }

  def apply[K: Ordering: ClassTag, A, B](
    iterA: Iterator[(K, A)],
    iterB: Iterator[(K, B)]
  ): Iterator[(K, (Option[A], Option[B]))] = {
    new OuterJoinIterator[K, A, B, Any, Any](
      new GroupByKeyIterator(iterA),
      new GroupByKeyIterator(iterB),
      new GroupByKeyIterator(Iterator.empty),
      new GroupByKeyIterator(Iterator.empty)
    ).map { case (k, (a, b, _, _)) =>
      (k, (a, b))
    }
  }
}
