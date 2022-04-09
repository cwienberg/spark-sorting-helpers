# Spark Sorting Helpers

![example workflow](https://github.com/cwienberg/spark-sorting-helpers/actions/workflows/scala.yml/badge.svg) [![codecov](https://codecov.io/gh/cwienberg/spark-sorting-helpers/branch/main/graph/badge.svg?token=IC5NUTYXHI)](https://codecov.io/gh/cwienberg/spark-sorting-helpers)

The spark sorting helpers is a library of convenience functions for leveraging the secondary sort functionality of RDD partitioning. Secondary sorting allows an RDD to be partitioned by a key while sorting the values, pushing that sort into the underlying shuffle machinery. This provides an efficient way to sort values within a partition if one is already conducting a shuffle operation anyway (e.g. a join or groupBy).

## Usage
This library uses the "pimp my library" pattern to add methods to RDDs of pairs. You can import the implicits with.

```scala
import net.gonzberg.spark.sorting.implicits._
```

You can then call additional functions on certain RDDs, e.g.
```scala
val rdd: RDD[(String, Int)] = ?
val groupedRDD: RDD[(String, Iterable[Int])] = rdd.sortedGroupByKey
groupedRDD.foreach((k, group) => assert group == group.sorted)
```

## Documentation

Scaladocs are avaiable [here](https://cwienberg.github.io/spark-sorting-helpers/).