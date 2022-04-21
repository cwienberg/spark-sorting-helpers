lazy val scala212 = "2.12.10"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

lazy val sparkVersion = "2.4.8"
lazy val scalatestVersion = "3.2.0"

ThisBuild / scalaVersion := scala212
ThisBuild / organization := "net.gonzberg"
ThisBuild / homepage := Some(url("https://github.com/cwienberg/spark-sorting-helpers"))
ThisBuild / licenses := List("MIT" -> url("http://opensource.org/licenses/MIT"))
ThisBuild / developers := List(
  Developer(
    "cwienberg",
    "Christopher González Wienberg",
    "christopher@gonzberg.net",
    url("https://gonzberg.net")
  )
)
ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
ThisBuild / sonatypeRepository := "https://s01.oss.sonatype.org/service/local"

enablePlugins(GitHubPagesPlugin)

lazy val core = (project in file("."))
  .enablePlugins(GitHubPagesPlugin)
  .settings(
    name := "spark-sorting-helpers",
    crossScalaVersions := supportedScalaVersions,
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-feature",
      "-Xfatal-warnings"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.scalatest" %% "scalatest-funsuite" % scalatestVersion % Test,
      "org.scalatest" %% "scalatest-shouldmatchers" % scalatestVersion % Test
    ),
    gitHubPagesSiteDir := baseDirectory.value / "target" / "scala-2.12" / "api"
  )
