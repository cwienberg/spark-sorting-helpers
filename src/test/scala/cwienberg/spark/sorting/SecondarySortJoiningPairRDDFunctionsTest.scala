package cwienberg.spark.sorting

import SecondarySortJoiningPairRDDFunctions.rddToSecondarySortJoiningPairRDDFunctions
import org.apache.spark.HashPartitioner
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SecondarySortJoiningPairRDDFunctionsTest
  extends AnyFunSuite
    with Matchers
    with SparkTestingMixin {

  test("fullOuterJoinWithSortedValues joins 4 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val rdd4 = sc.parallelize(Seq(
      "a" -> "rdd4-a", "b" -> "rdd4-b", "c" -> "rdd4-c", "d" -> "rdd4-d"
    ))
    val actualWithPartitioner = rdd1.fullOuterJoinWithSortedValues(rdd2, rdd3, rdd4, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actualWithPartitioner == Seq(
        "a" -> (Some("rdd1-a"), None, Some("rdd3-a"), Some("rdd4-a")),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), Some("rdd3-b"), Some("rdd4-b")),
        "c" -> (None, Some("rdd2-c"), None, Some("rdd4-c")),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d")),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d"))
      )
    )
    val actualWithNumPartitions = rdd1.fullOuterJoinWithSortedValues(rdd2, rdd3, rdd4, 3).collect().sortBy(_._1).toSeq
    assert(
      actualWithNumPartitions == Seq(
        "a" -> (Some("rdd1-a"), None, Some("rdd3-a"), Some("rdd4-a")),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), Some("rdd3-b"), Some("rdd4-b")),
        "c" -> (None, Some("rdd2-c"), None, Some("rdd4-c")),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d")),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d"))
      )
    )
    val actualWithDefaultPartitioner = rdd1.fullOuterJoinWithSortedValues(rdd2, rdd3, rdd4).collect().sortBy(_._1).toSeq
    assert(
      actualWithDefaultPartitioner == Seq(
        "a" -> (Some("rdd1-a"), None, Some("rdd3-a"), Some("rdd4-a")),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), Some("rdd3-b"), Some("rdd4-b")),
        "c" -> (None, Some("rdd2-c"), None, Some("rdd4-c")),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d")),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d"))
      )
    )
  }

  test("fullOuterJoinWithSortedValues joins 3 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val actualWithPartitioner = rdd1.fullOuterJoinWithSortedValues(rdd2, rdd3, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actualWithPartitioner == Seq(
        "a" -> (Some("rdd1-a"), None, Some("rdd3-a")),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), Some("rdd3-b")),
        "c" -> (None, Some("rdd2-c"), None),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), Some("rdd3-d")),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), Some("rdd3-d"))
      )
    )
    val actualWithNumPartitions = rdd1.fullOuterJoinWithSortedValues(rdd2, rdd3, 3).collect().sortBy(_._1).toSeq
    assert(
      actualWithNumPartitions == Seq(
        "a" -> (Some("rdd1-a"), None, Some("rdd3-a")),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), Some("rdd3-b")),
        "c" -> (None, Some("rdd2-c"), None),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), Some("rdd3-d")),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), Some("rdd3-d"))
      )
    )
    val actualWithDefaultPartitioner = rdd1.fullOuterJoinWithSortedValues(rdd2, rdd3).collect().sortBy(_._1).toSeq
    assert(
      actualWithDefaultPartitioner == Seq(
        "a" -> (Some("rdd1-a"), None, Some("rdd3-a")),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), Some("rdd3-b")),
        "c" -> (None, Some("rdd2-c"), None),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), Some("rdd3-d")),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), Some("rdd3-d"))
      )
    )
  }

  test("fullOuterJoinWithSortedValues joins 2 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val actualWithPartitioner = rdd1.fullOuterJoinWithSortedValues(rdd2, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actualWithPartitioner == Seq(
        "a" -> (Some("rdd1-a"), None),
        "b" -> (Some("rdd1-b"), Some("rdd2-b")),
        "c" -> (None, Some("rdd2-c")),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d")),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"))
      )
    )
    val actualWithNumPartitions = rdd1.fullOuterJoinWithSortedValues(rdd2, 3).collect().sortBy(_._1).toSeq
    assert(
      actualWithNumPartitions == Seq(
        "a" -> (Some("rdd1-a"), None),
        "b" -> (Some("rdd1-b"), Some("rdd2-b")),
        "c" -> (None, Some("rdd2-c")),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d")),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"))
      )
    )
    val actualWithDefaultPartitioner = rdd1.fullOuterJoinWithSortedValues(rdd2).collect().sortBy(_._1).toSeq
    assert(
      actualWithDefaultPartitioner == Seq(
        "a" -> (Some("rdd1-a"), None),
        "b" -> (Some("rdd1-b"), Some("rdd2-b")),
        "c" -> (None, Some("rdd2-c")),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d")),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"))
      )
    )
  }

  test("innerJoinWithSortedValues joins 4 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val rdd4 = sc.parallelize(Seq(
      "a" -> "rdd4-a", "b" -> "rdd4-b", "c" -> "rdd4-c", "d" -> "rdd4-d"
    ))
    val actualWithPartitioner = rdd1.innerJoinWithSortedValues(rdd2, rdd3, rdd4, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actualWithPartitioner == Seq(
        "b" -> ("rdd1-b", "rdd2-b", "rdd3-b", "rdd4-b"),
        "d" -> ("rdd1-d1", "rdd2-d", "rdd3-d", "rdd4-d"),
        "d" -> ("rdd1-d2", "rdd2-d", "rdd3-d", "rdd4-d")
      )
    )
    val actualWithNumPartitions = rdd1.innerJoinWithSortedValues(rdd2, rdd3, rdd4, 3).collect().sortBy(_._1).toSeq
    assert(
      actualWithNumPartitions == Seq(
        "b" -> ("rdd1-b", "rdd2-b", "rdd3-b", "rdd4-b"),
        "d" -> ("rdd1-d1", "rdd2-d", "rdd3-d", "rdd4-d"),
        "d" -> ("rdd1-d2", "rdd2-d", "rdd3-d", "rdd4-d")
      )
    )
    val actualWithDefaultPartitioner = rdd1.innerJoinWithSortedValues(rdd2, rdd3, rdd4).collect().sortBy(_._1).toSeq
    assert(
      actualWithDefaultPartitioner == Seq(
        "b" -> ("rdd1-b", "rdd2-b", "rdd3-b", "rdd4-b"),
        "d" -> ("rdd1-d1", "rdd2-d", "rdd3-d", "rdd4-d"),
        "d" -> ("rdd1-d2", "rdd2-d", "rdd3-d", "rdd4-d")
      )
    )
  }

  test("innerJoinWithSortedValues joins 3 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val actualWithPartitioner = rdd1.innerJoinWithSortedValues(rdd2, rdd3, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actualWithPartitioner == Seq(
        "b" -> ("rdd1-b", "rdd2-b", "rdd3-b"),
        "d" -> ("rdd1-d1", "rdd2-d", "rdd3-d"),
        "d" -> ("rdd1-d2", "rdd2-d", "rdd3-d")
      )
    )
    val actualWithNumPartitions = rdd1.innerJoinWithSortedValues(rdd2, rdd3, 3).collect().sortBy(_._1).toSeq
    assert(
      actualWithNumPartitions == Seq(
        "b" -> ("rdd1-b", "rdd2-b", "rdd3-b"),
        "d" -> ("rdd1-d1", "rdd2-d", "rdd3-d"),
        "d" -> ("rdd1-d2", "rdd2-d", "rdd3-d")
      )
    )
    val actualWithDefaultPartitioner = rdd1.innerJoinWithSortedValues(rdd2, rdd3).collect().sortBy(_._1).toSeq
    assert(
      actualWithDefaultPartitioner == Seq(
        "b" -> ("rdd1-b", "rdd2-b", "rdd3-b"),
        "d" -> ("rdd1-d1", "rdd2-d", "rdd3-d"),
        "d" -> ("rdd1-d2", "rdd2-d", "rdd3-d")
      )
    )
  }

  test("innerJoinWithSortedValues joins 2 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val actualWithPartitioner = rdd1.innerJoinWithSortedValues(rdd2, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actualWithPartitioner == Seq(
        "b" -> ("rdd1-b", "rdd2-b"),
        "d" -> ("rdd1-d1", "rdd2-d"),
        "d" -> ("rdd1-d2", "rdd2-d")
      )
    )
    val actualWithNumPartitions = rdd1.innerJoinWithSortedValues(rdd2, 3).collect().sortBy(_._1).toSeq
    assert(
      actualWithNumPartitions == Seq(
        "b" -> ("rdd1-b", "rdd2-b"),
        "d" -> ("rdd1-d1", "rdd2-d"),
        "d" -> ("rdd1-d2", "rdd2-d")
      )
    )
    val actualWithDefaultPartitioner = rdd1.innerJoinWithSortedValues(rdd2).collect().sortBy(_._1).toSeq
    assert(
      actualWithDefaultPartitioner == Seq(
        "b" -> ("rdd1-b", "rdd2-b"),
        "d" -> ("rdd1-d1", "rdd2-d"),
        "d" -> ("rdd1-d2", "rdd2-d")
      )
    )
  }

  test("leftJoinWithSortedValues joins 4 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val rdd4 = sc.parallelize(Seq(
      "a" -> "rdd4-a", "b" -> "rdd4-b", "c" -> "rdd4-c", "d" -> "rdd4-d"
    ))
    val actualWithPartitioner = rdd1.leftJoinWithSortedValues(rdd2, rdd3, rdd4, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actualWithPartitioner == Seq(
        "a" -> ("rdd1-a", None, Some("rdd3-a"), Some("rdd4-a")),
        "b" -> ("rdd1-b", Some("rdd2-b"), Some("rdd3-b"), Some("rdd4-b")),
        "d" -> ("rdd1-d1", Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d")),
        "d" -> ("rdd1-d2", Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d"))
      )
    )
    val actualWithNumPartitions = rdd1.leftJoinWithSortedValues(rdd2, rdd3, rdd4, 3).collect().sortBy(_._1).toSeq
    assert(
      actualWithNumPartitions == Seq(
        "a" -> ("rdd1-a", None, Some("rdd3-a"), Some("rdd4-a")),
        "b" -> ("rdd1-b", Some("rdd2-b"), Some("rdd3-b"), Some("rdd4-b")),
        "d" -> ("rdd1-d1", Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d")),
        "d" -> ("rdd1-d2", Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d"))
      )
    )
    val actualWithDefaultPartitioner = rdd1.leftJoinWithSortedValues(rdd2, rdd3, rdd4).collect().sortBy(_._1).toSeq
    assert(
      actualWithDefaultPartitioner == Seq(
        "a" -> ("rdd1-a", None, Some("rdd3-a"), Some("rdd4-a")),
        "b" -> ("rdd1-b", Some("rdd2-b"), Some("rdd3-b"), Some("rdd4-b")),
        "d" -> ("rdd1-d1", Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d")),
        "d" -> ("rdd1-d2", Some("rdd2-d"), Some("rdd3-d"), Some("rdd4-d"))
      )
    )
  }

  test("leftJoinWithSortedValues joins 3 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val actualWithPartitioner = rdd1.leftJoinWithSortedValues(rdd2, rdd3, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actualWithPartitioner == Seq(
        "a" -> ("rdd1-a", None, Some("rdd3-a")),
        "b" -> ("rdd1-b", Some("rdd2-b"), Some("rdd3-b")),
        "d" -> ("rdd1-d1", Some("rdd2-d"), Some("rdd3-d")),
        "d" -> ("rdd1-d2", Some("rdd2-d"), Some("rdd3-d"))
      )
    )
    val actualWithNumPartitions = rdd1.leftJoinWithSortedValues(rdd2, rdd3, 3).collect().sortBy(_._1).toSeq
    assert(
      actualWithNumPartitions == Seq(
        "a" -> ("rdd1-a", None, Some("rdd3-a")),
        "b" -> ("rdd1-b", Some("rdd2-b"), Some("rdd3-b")),
        "d" -> ("rdd1-d1", Some("rdd2-d"), Some("rdd3-d")),
        "d" -> ("rdd1-d2", Some("rdd2-d"), Some("rdd3-d"))
      )
    )
    val actualWithDefaultPartitioner = rdd1.leftJoinWithSortedValues(rdd2, rdd3).collect().sortBy(_._1).toSeq
    assert(
      actualWithDefaultPartitioner == Seq(
        "a" -> ("rdd1-a", None, Some("rdd3-a")),
        "b" -> ("rdd1-b", Some("rdd2-b"), Some("rdd3-b")),
        "d" -> ("rdd1-d1", Some("rdd2-d"), Some("rdd3-d")),
        "d" -> ("rdd1-d2", Some("rdd2-d"), Some("rdd3-d"))
      )
    )
  }

  test("leftJoinWithSortedValues joins 2 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val actualWithPartitioner = rdd1.leftJoinWithSortedValues(rdd2, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actualWithPartitioner == Seq(
        "a" -> ("rdd1-a", None),
        "b" -> ("rdd1-b", Some("rdd2-b")),
        "d" -> ("rdd1-d1", Some("rdd2-d")),
        "d" -> ("rdd1-d2", Some("rdd2-d"))
      )
    )
    val actualWithNumPartitions = rdd1.leftJoinWithSortedValues(rdd2, 3).collect().sortBy(_._1).toSeq
    assert(
      actualWithNumPartitions == Seq(
        "a" -> ("rdd1-a", None),
        "b" -> ("rdd1-b", Some("rdd2-b")),
        "d" -> ("rdd1-d1", Some("rdd2-d")),
        "d" -> ("rdd1-d2", Some("rdd2-d"))
      )
    )
    val actualWithDefaultPartitioner = rdd1.leftJoinWithSortedValues(rdd2).collect().sortBy(_._1).toSeq
    assert(
      actualWithDefaultPartitioner == Seq(
        "a" -> ("rdd1-a", None),
        "b" -> ("rdd1-b", Some("rdd2-b")),
        "d" -> ("rdd1-d1", Some("rdd2-d")),
        "d" -> ("rdd1-d2", Some("rdd2-d"))
      )
    )
  }

  test("rightJoinWithSortedValues joins 4 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val rdd4 = sc.parallelize(Seq(
      "a" -> "rdd4-a", "b" -> "rdd4-b", "d" -> "rdd4-d"  // "c" -> "rdd4-c" was removed, compared to the other similar tests tests, to ensure all behaviors were covered
    ))
    val actualWithPartitioner = rdd1.rightJoinWithSortedValues(rdd2, rdd3, rdd4, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actualWithPartitioner == Seq(
        "a" -> (Some("rdd1-a"), None, Some("rdd3-a"), "rdd4-a"),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), Some("rdd3-b"), "rdd4-b"),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), Some("rdd3-d"), "rdd4-d"),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), Some("rdd3-d"), "rdd4-d")
      )
    )
    val actualWithNumPartitions = rdd1.rightJoinWithSortedValues(rdd2, rdd3, rdd4, 3).collect().sortBy(_._1).toSeq
    assert(
      actualWithNumPartitions == Seq(
        "a" -> (Some("rdd1-a"), None, Some("rdd3-a"), "rdd4-a"),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), Some("rdd3-b"), "rdd4-b"),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), Some("rdd3-d"), "rdd4-d"),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), Some("rdd3-d"), "rdd4-d")
      )
    )
    val actualWithDefaultPartitioner = rdd1.rightJoinWithSortedValues(rdd2, rdd3, rdd4).collect().sortBy(_._1).toSeq
    assert(
      actualWithDefaultPartitioner == Seq(
        "a" -> (Some("rdd1-a"), None, Some("rdd3-a"), "rdd4-a"),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), Some("rdd3-b"), "rdd4-b"),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), Some("rdd3-d"), "rdd4-d"),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), Some("rdd3-d"), "rdd4-d")
      )
    )
  }

  test("rightJoinWithSortedValues joins 3 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val rdd3 = sc.parallelize(Seq(
      "a" -> "rdd3-a", "b" -> "rdd3-b", "d" -> "rdd3-d"
    ))
    val actualWithPartitioner = rdd1.rightJoinWithSortedValues(rdd2, rdd3, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actualWithPartitioner == Seq(
        "a" -> (Some("rdd1-a"), None, "rdd3-a"),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), "rdd3-b"),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), "rdd3-d"),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), "rdd3-d")
      )
    )
    val actualWithNumPartitions = rdd1.rightJoinWithSortedValues(rdd2, rdd3, 3).collect().sortBy(_._1).toSeq
    assert(
      actualWithNumPartitions == Seq(
        "a" -> (Some("rdd1-a"), None, "rdd3-a"),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), "rdd3-b"),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), "rdd3-d"),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), "rdd3-d")
      )
    )
    val actualWithDefaultPartitioner = rdd1.rightJoinWithSortedValues(rdd2, rdd3).collect().sortBy(_._1).toSeq
    assert(
      actualWithDefaultPartitioner == Seq(
        "a" -> (Some("rdd1-a"), None, "rdd3-a"),
        "b" -> (Some("rdd1-b"), Some("rdd2-b"), "rdd3-b"),
        "d" -> (Some("rdd1-d1"), Some("rdd2-d"), "rdd3-d"),
        "d" -> (Some("rdd1-d2"), Some("rdd2-d"), "rdd3-d")
      )
    )
  }

  test("rightJoinWithSortedValues joins 2 RDDs as expected") {
    val rdd1 = sc.parallelize(Seq(
      "a" -> "rdd1-a", "b" -> "rdd1-b", "d" -> "rdd1-d1", "d" -> "rdd1-d2"
    ))
    val rdd2 = sc.parallelize(Seq(
      "b" -> "rdd2-b", "c" -> "rdd2-c", "d" -> "rdd2-d"
    ))
    val actualWithPartitioner = rdd1.rightJoinWithSortedValues(rdd2, new HashPartitioner(3)).collect().sortBy(_._1).toSeq
    assert(
      actualWithPartitioner == Seq(
        "b" -> (Some("rdd1-b"), "rdd2-b"),
        "c" -> (None, "rdd2-c"),
        "d" -> (Some("rdd1-d1"), "rdd2-d"),
        "d" -> (Some("rdd1-d2"), "rdd2-d")
      )
    )
    val actualWithNumPartitions = rdd1.rightJoinWithSortedValues(rdd2, 3).collect().sortBy(_._1).toSeq
    assert(
      actualWithNumPartitions == Seq(
        "b" -> (Some("rdd1-b"), "rdd2-b"),
        "c" -> (None, "rdd2-c"),
        "d" -> (Some("rdd1-d1"), "rdd2-d"),
        "d" -> (Some("rdd1-d2"), "rdd2-d")
      )
    )
    val actualWithDefaultPartitioner = rdd1.rightJoinWithSortedValues(rdd2).collect().sortBy(_._1).toSeq
    assert(
      actualWithDefaultPartitioner == Seq(
        "b" -> (Some("rdd1-b"), "rdd2-b"),
        "c" -> (None, "rdd2-c"),
        "d" -> (Some("rdd1-d1"), "rdd2-d"),
        "d" -> (Some("rdd1-d2"), "rdd2-d")
      )
    )
  }
}
