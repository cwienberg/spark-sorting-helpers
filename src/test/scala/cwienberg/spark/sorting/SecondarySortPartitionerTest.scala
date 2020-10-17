package cwienberg.spark.sorting

import org.apache.spark.HashPartitioner
import org.scalatest.funsuite.AnyFunSuite

class SecondarySortPartitionerTest extends AnyFunSuite {

  test("SecondarySortPartitioner works as expected") {
    val innerPartitioner = new HashPartitioner(3)
    val outerPartitioner =
      new SecondarySortPartitioner[String, Int](innerPartitioner)
    assert(outerPartitioner.numPartitions == 3)
    assert(outerPartitioner == innerPartitioner)
    assert(
      outerPartitioner.getPartition("abc") == innerPartitioner
        .getPartition("abc")
    )
    assert(
      outerPartitioner.getPartition(("key", 0)) == innerPartitioner
        .getPartition("key")
    )
    assert(
      outerPartitioner.getPartition((4, 0)) == innerPartitioner
        .getPartition((4))
    )
  }

}
