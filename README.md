# Spark Sorting Helpers

![example workflow](https://github.com/cwienberg/spark-sorting-helpers/actions/workflows/scala.yml/badge.svg) [![codecov](https://codecov.io/gh/cwienberg/spark-sorting-helpers/branch/main/graph/badge.svg?token=IC5NUTYXHI)](https://codecov.io/gh/cwienberg/spark-sorting-helpers)

The spark sorting helpers is a library of convenience functions for leveraging the secondary sort functionality of RDD partitioning. Secondary sorting allows an RDD to be partitioned by a key while sorting the values, pushing that sort into the underlying shuffle machinery. This provides an efficient way to sort values within a partition if one is already conducting a shuffle operation anyway (e.g. a join or groupBy).
