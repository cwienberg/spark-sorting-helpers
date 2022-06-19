# Spark Sorting Helpers

[![build status](https://github.com/cwienberg/spark-sorting-helpers/actions/workflows/release.yml/badge.svg)](https://github.com/cwienberg/spark-sorting-helpers/actions/workflows/release.yml) [![codecov](https://codecov.io/gh/cwienberg/spark-sorting-helpers/branch/main/graph/badge.svg?token=IC5NUTYXHI)](https://codecov.io/gh/cwienberg/spark-sorting-helpers) [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/r/https/s01.oss.sonatype.org/net.gonzberg/spark-sorting-helpers_2.12.svg)](https://s01.oss.sonatype.org/content/repositories/releases/net/gonzberg/spark-sorting-helpers_2.12/) [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/https/s01.oss.sonatype.org/net.gonzberg/spark-sorting-helpers_2.12.svg)](https://s01.oss.sonatype.org/content/repositories/snapshots/net/gonzberg/spark-sorting-helpers_2.12/)

The spark sorting helpers is a library of convenience functions for leveraging the secondary sort functionality of Spark partitioning. Secondary sorting allows an RDD or Dataset to be partitioned by a key while sorting the values, pushing that sort into the underlying shuffle machinery. This provides an efficient way to sort values within a partition if one is already conducting a shuffle operation anyway (e.g. a join or groupBy).

## Usage
This library uses the "pimp my library" pattern to add methods to RDDs or Datasets of pairs. You can import the implicits with:
```scala
import net.gonzberg.spark.sorting.implicits._
```

You can then call additional functions on certain RDDs or Datasets, e.g.
```scala
val rdd: RDD[(String, Int)] = ?
val groupedRDD: RDD[(String, Iterable[Int])] = rdd.sortedGroupByKey
groupedRDD.foreach((k, group) => assert group == group.sorted)
```

## Supported Versions
This library attempts to support Scala `2.11`, `2.12`, and `2.13`. Since there is not a single version of Spark which supports all three of those Scala versions, this library is built against different versions of Spark depending on the Scala version.

| Scala | Spark |
| ----- | ----- |
| 2.11  | 2.4.8 |
| 2.12  | 3.3.0 |
| 2.13  | 3.3.0 |

Other combinations of versions may also work, but these are the ones for which the tests run automatically. We will likely drop `2.11` support in a later release, depending on when it becomes too difficult to support.

## Documentation

Scaladocs are avaiable [here](https://cwienberg.github.io/spark-sorting-helpers/).

## Development

This package is built using `sbt`. You can run the tests with `sbt test`. You can lint with `sbt scalafmt`. You can use `+` in front of a directive to cross-build, though you'll need Java 8 (as opposed to Java 11) to cross-build to Scala 2.11. 
