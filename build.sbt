lazy val scala213 = "2.13.8"
lazy val scala212 = "2.12.15"
lazy val supportedScalaVersions = List(scala213, scala212)
lazy val primaryScalaVersion = scala213
lazy val primaryScalaMinorVersion = primaryScalaVersion.split("\\.").slice(0,2).mkString(".")

lazy val sparkVersion = "3.2.1"
lazy val scalatestVersion = "3.2.11"

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
ThisBuild / organization := "net.gonzberg"
ThisBuild / scalaVersion := primaryScalaVersion

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
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.7.0",
      "org.scalatest" %% "scalatest-funsuite" % scalatestVersion % Test,
      "org.scalatest" %% "scalatest-shouldmatchers" % scalatestVersion % Test
    ),
    gitHubPagesSiteDir := baseDirectory.value / "target" / s"scala-${primaryScalaMinorVersion}" / "api"
  )
