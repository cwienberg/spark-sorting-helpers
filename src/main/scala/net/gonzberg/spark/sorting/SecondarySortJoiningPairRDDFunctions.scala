package net.gonzberg.spark.sorting

import net.gonzberg.spark.sorting.util.OuterJoinIterator
import net.gonzberg.spark.sorting.util.SortHelpers.repartitionAndSort
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

final class SecondarySortJoiningPairRDDFunctions[
  K: Ordering: ClassTag,
  V: Ordering: ClassTag
](rdd: RDD[(K, V)])
    extends Serializable {

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    partitioner: Partitioner
  ): RDD[(K, (Option[V], Option[B], Option[C], Option[D]))] = {
    val thisPartitioned = repartitionAndSort(rdd, partitioner)
    val bPartitioned = repartitionAndSort(rddB, partitioner)
    val cPartitioned = repartitionAndSort(rddC, partitioner)
    val dPartitioned = repartitionAndSort(rddD, partitioner)
    thisPartitioned.zipPartitions(
      bPartitioned,
      cPartitioned,
      dPartitioned,
      preservesPartitioning = true
    ) { (iterV, iterB, iterC, iterD) =>
      OuterJoinIterator(iterV, iterB, iterC, iterD)
    }
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    numPartitions: Int
  ): RDD[(K, (Option[V], Option[B], Option[C], Option[D]))] = {
    fullOuterJoinWithSortedValues(
      rddB,
      rddC,
      rddD,
      new HashPartitioner(numPartitions)
    )
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)]
  ): RDD[(K, (Option[V], Option[B], Option[C], Option[D]))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC, rddD)
    fullOuterJoinWithSortedValues(rddB, rddC, rddD, partitioner)
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    partitioner: Partitioner
  ): RDD[(K, (Option[V], Option[B], Option[C]))] = {
    val thisPartitioned = repartitionAndSort(rdd, partitioner)
    val bPartitioned = repartitionAndSort(rddB, partitioner)
    val cPartitioned = repartitionAndSort(rddC, partitioner)
    thisPartitioned.zipPartitions(
      bPartitioned,
      cPartitioned,
      preservesPartitioning = true
    ) { (iterV, iterB, iterC) =>
      OuterJoinIterator(iterV, iterB, iterC)
    }
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    numPartitions: Int
  ): RDD[(K, (Option[V], Option[B], Option[C]))] = {
    fullOuterJoinWithSortedValues(
      rddB,
      rddC,
      new HashPartitioner(numPartitions)
    )
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)]
  ): RDD[(K, (Option[V], Option[B], Option[C]))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC)
    fullOuterJoinWithSortedValues(rddB, rddC, partitioner)
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    partitioner: Partitioner
  ): RDD[(K, (Option[V], Option[B]))] = {
    val thisPartitioned = repartitionAndSort(rdd, partitioner)
    val bPartitioned = repartitionAndSort(rddB, partitioner)
    thisPartitioned.zipPartitions(bPartitioned, preservesPartitioning = true) {
      (iterV, iterB) =>
        OuterJoinIterator(iterV, iterB)
    }
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    numPartitions: Int
  ): RDD[(K, (Option[V], Option[B]))] = {
    fullOuterJoinWithSortedValues(rddB, new HashPartitioner(numPartitions))
  }

  /** Performs a full outer join by key
    * @param rddB another RDD to join with
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values, where values are Some if there was
    *         a value in the corresponding RDD for that key, and None if there
    *         were no values.
    */
  def fullOuterJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)]
  ): RDD[(K, (Option[V], Option[B]))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB)
    fullOuterJoinWithSortedValues(rddB, partitioner)
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    partitioner: Partitioner
  ): RDD[(K, (V, B, C, D))] = {
    fullOuterJoinWithSortedValues(rddB, rddC, rddD, partitioner)
      .flatMapValues { case (maybeV, maybeB, maybeC, maybeD) =>
        for {
          v <- maybeV
          b <- maybeB
          c <- maybeC
          d <- maybeD
        } yield (v, b, c, d)
      }
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    numPartitions: Int
  ): RDD[(K, (V, B, C, D))] = {
    innerJoinWithSortedValues(
      rddB,
      rddC,
      rddD,
      new HashPartitioner(numPartitions)
    )
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)]
  ): RDD[(K, (V, B, C, D))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC, rddD)
    innerJoinWithSortedValues(rddB, rddC, rddD, partitioner)
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    partitioner: Partitioner
  ): RDD[(K, (V, B, C))] = {
    fullOuterJoinWithSortedValues(rddB, rddC, partitioner).flatMapValues {
      case (maybeV, maybeB, maybeC) =>
        for {
          v <- maybeV
          b <- maybeB
          c <- maybeC
        } yield (v, b, c)
    }
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    numPartitions: Int
  ): RDD[(K, (V, B, C))] = {
    innerJoinWithSortedValues(rddB, rddC, new HashPartitioner(numPartitions))
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)]
  ): RDD[(K, (V, B, C))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC)
    innerJoinWithSortedValues(rddB, rddC, partitioner)
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    partitioner: Partitioner
  ): RDD[(K, (V, B))] = {
    fullOuterJoinWithSortedValues(rddB, partitioner).flatMapValues {
      case (maybeV, maybeB) =>
        for {
          v <- maybeV
          b <- maybeB
        } yield (v, b)
    }
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    numPartitions: Int
  ): RDD[(K, (V, B))] = {
    innerJoinWithSortedValues(rddB, new HashPartitioner(numPartitions))
  }

  /** Performs a inner join by key
    * @param rddB another RDD to join with
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def innerJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)]
  ): RDD[(K, (V, B))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB)
    innerJoinWithSortedValues(rddB, partitioner)
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    partitioner: Partitioner
  ): RDD[(K, (V, Option[B], Option[C], Option[D]))] = {
    fullOuterJoinWithSortedValues(rddB, rddC, rddD, partitioner)
      .flatMapValues { case (maybeV, maybeB, maybeC, maybeD) =>
        for {
          v <- maybeV
        } yield (v, maybeB, maybeC, maybeD)
      }
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    numPartitions: Int
  ): RDD[(K, (V, Option[B], Option[C], Option[D]))] = {
    leftJoinWithSortedValues(
      rddB,
      rddC,
      rddD,
      new HashPartitioner(numPartitions)
    )
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)]
  ): RDD[(K, (V, Option[B], Option[C], Option[D]))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC, rddD)
    leftJoinWithSortedValues(rddB, rddC, rddD, partitioner)
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    partitioner: Partitioner
  ): RDD[(K, (V, Option[B], Option[C]))] = {
    fullOuterJoinWithSortedValues(rddB, rddC, partitioner).flatMapValues {
      case (maybeV, maybeB, maybeC) =>
        for {
          v <- maybeV
        } yield (v, maybeB, maybeC)
    }
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    numPartitions: Int
  ): RDD[(K, (V, Option[B], Option[C]))] = {
    leftJoinWithSortedValues(rddB, rddC, new HashPartitioner(numPartitions))
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)]
  ): RDD[(K, (V, Option[B], Option[C]))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC)
    leftJoinWithSortedValues(rddB, rddC, partitioner)
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    partitioner: Partitioner
  ): RDD[(K, (V, Option[B]))] = {
    fullOuterJoinWithSortedValues(rddB, partitioner).flatMapValues {
      case (maybeV, maybeB) =>
        for {
          v <- maybeV
        } yield (v, maybeB)
    }
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    numPartitions: Int
  ): RDD[(K, (V, Option[B]))] = {
    leftJoinWithSortedValues(rddB, new HashPartitioner(numPartitions))
  }

  /** Performs a left join by key
    * @param rddB another RDD to join with
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def leftJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)]
  ): RDD[(K, (V, Option[B]))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB)
    leftJoinWithSortedValues(rddB, partitioner)
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    partitioner: Partitioner
  ): RDD[(K, (Option[V], Option[B], Option[C], D))] = {
    fullOuterJoinWithSortedValues(rddB, rddC, rddD, partitioner)
      .flatMapValues { case (maybeV, maybeB, maybeC, maybeD) =>
        for {
          d <- maybeD
        } yield (maybeV, maybeB, maybeC, d)
      }
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)],
    numPartitions: Int
  ): RDD[(K, (Option[V], Option[B], Option[C], D))] = {
    rightJoinWithSortedValues(
      rddB,
      rddC,
      rddD,
      new HashPartitioner(numPartitions)
    )
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param rddD another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @tparam D the type of the values in rddD
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering, C: Ordering, D: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    rddD: RDD[(K, D)]
  ): RDD[(K, (Option[V], Option[B], Option[C], D))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC, rddD)
    rightJoinWithSortedValues(rddB, rddC, rddD, partitioner)
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    partitioner: Partitioner
  ): RDD[(K, (Option[V], Option[B], C))] = {
    fullOuterJoinWithSortedValues(rddB, rddC, partitioner).flatMapValues {
      case (maybeV, maybeB, maybeC) =>
        for {
          c <- maybeC
        } yield (maybeV, maybeB, c)
    }
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)],
    numPartitions: Int
  ): RDD[(K, (Option[V], Option[B], C))] = {
    rightJoinWithSortedValues(rddB, rddC, new HashPartitioner(numPartitions))
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param rddC another RDD to join with
    * @tparam B the type of the values in rddB
    * @tparam C the type of the values in rddC
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering, C: Ordering](
    rddB: RDD[(K, B)],
    rddC: RDD[(K, C)]
  ): RDD[(K, (Option[V], Option[B], C))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB, rddC)
    rightJoinWithSortedValues(rddB, rddC, partitioner)
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param partitioner the partitioner for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    partitioner: Partitioner
  ): RDD[(K, (Option[V], B))] = {
    fullOuterJoinWithSortedValues(rddB, partitioner).flatMapValues {
      case (maybeV, maybeB) =>
        for {
          b <- maybeB
        } yield (maybeV, b)
    }
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @param numPartitions the number of partitions for shuffling
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)],
    numPartitions: Int
  ): RDD[(K, (Option[V], B))] = {
    rightJoinWithSortedValues(rddB, new HashPartitioner(numPartitions))
  }

  /** Performs a right join by key
    * @param rddB another RDD to join with
    * @tparam B the type of the values in rddB
    * @return PairRDD with keys and values
    */
  def rightJoinWithSortedValues[B: Ordering](
    rddB: RDD[(K, B)]
  ): RDD[(K, (Option[V], B))] = {
    val partitioner = Partitioner.defaultPartitioner(rdd, rddB)
    rightJoinWithSortedValues(rddB, partitioner)
  }
}

private[sorting] object SecondarySortJoiningPairRDDFunctions {
  implicit def rddToSecondarySortJoiningPairRDDFunctions[
    K: Ordering: ClassTag,
    V: Ordering: ClassTag
  ](rdd: RDD[(K, V)]): SecondarySortJoiningPairRDDFunctions[K, V] = {
    new SecondarySortJoiningPairRDDFunctions(rdd)
  }
}
