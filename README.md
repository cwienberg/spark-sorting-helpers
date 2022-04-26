# Spark Sorting Helpers

[![build status](https://github.com/cwienberg/spark-sorting-helpers/actions/workflows/release.yml/badge.svg)](https://github.com/cwienberg/spark-sorting-helpers/actions/workflows/release.yml) [![codecov](https://codecov.io/gh/cwienberg/spark-sorting-helpers/branch/main/graph/badge.svg?token=IC5NUTYXHI)](https://codecov.io/gh/cwienberg/spark-sorting-helpers) [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/r/https/s01.oss.sonatype.org/net.gonzberg/spark-sorting-helpers_2.12.svg)](https://s01.oss.sonatype.org/content/repositories/releases/net/gonzberg/spark-sorting-helpers_2.12/) [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/s01.oss.sonatype.org/net.gonzberg/spark-sorting-helpers_2.12.svg)](https://s01.oss.sonatype.org/content/repositories/snapshots/net/gonzberg/spark-sorting-helpers_2.12/)

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

## Development

This package is built using `sbt`. You can run the tests with `sbt test`.
