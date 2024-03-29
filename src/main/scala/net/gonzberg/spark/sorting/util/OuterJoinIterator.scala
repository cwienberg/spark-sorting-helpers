package net.gonzberg.spark.sorting.util

import BufferedIteratorHelper.iterHeadOption

import scala.collection.BufferedIterator
import scala.math.Ordering.Implicits.infixOrderingOps

private[sorting] class OuterJoinIterator[K: Ordering, A, B, C, D](
  iterA: GroupedByKeyIterator[K, A],
  iterB: GroupedByKeyIterator[K, B],
  iterC: GroupedByKeyIterator[K, C],
  iterD: GroupedByKeyIterator[K, D]
) extends Iterator[(K, (Option[A], Option[B], Option[C], Option[D]))] {
  val bufferedIterA: BufferedIterator[(K, Iterator[A])] = iterA.buffered
  val bufferedIterB: BufferedIterator[(K, Iterator[B])] = iterB.buffered
  val bufferedIterC: BufferedIterator[(K, Iterator[C])] = iterC.buffered
  val bufferedIterD: BufferedIterator[(K, Iterator[D])] = iterD.buffered

  var prevKey: Option[K] = None
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

  private def setNextGroupIterator(): Unit = {
    val maybeKeys = Seq(
      iterHeadOption(bufferedIterA).map(_._1),
      iterHeadOption(bufferedIterB).map(_._1),
      iterHeadOption(bufferedIterC).map(_._1),
      iterHeadOption(bufferedIterD).map(_._1)
    )
    val minKey = maybeKeys.flatten.min
    requireIncreasingKeys(minKey)
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
        val ds = maybeDs.getOrElse(Iterator(None)).toSeq
        for {
          c <- cs
          d <- ds
        } yield (None, None, c, d)
      case (None, Some(bs), maybeCs, maybeDs) =>
        val cs = maybeCs.getOrElse(Iterator(None)).toSeq
        val ds = maybeDs.getOrElse(Iterator(None)).toSeq
        for {
          b <- bs
          c <- cs
          d <- ds
        } yield (None, b, c, d)
      case (Some(as), maybeBs, maybeCs, maybeDs) =>
        val bs = maybeBs.getOrElse(Iterator(None)).toSeq
        val cs = maybeCs.getOrElse(Iterator(None)).toSeq
        val ds = maybeDs.getOrElse(Iterator(None)).toSeq
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

  private def requireIncreasingKeys(minKey: K): Unit = {
    if (!prevKey.map(minKey > _).getOrElse(true)) {
      throw new IllegalStateException(
        "next group iterator key must be greater than previous"
      )
    }
    prevKey = Some(minKey)
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
  def apply[K: Ordering, A, B, C, D](
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

  def apply[K: Ordering, A, B, C](
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

  def apply[K: Ordering, A, B](
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
