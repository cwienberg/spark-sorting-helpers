lazy val scala213 = "2.13.8"
lazy val scala212 = "2.12.15"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala211, scala212, scala213)
lazy val primaryScalaVersion = scala213

def getMinorVersionFromVersion(version: String): String = {
  version.split("\\.").slice(0,2).mkString(".")
}

lazy val primaryScalaMinorVersion = getMinorVersionFromVersion(primaryScalaVersion)

def sparkDependency(scalaVersion: String): String = {
  getMinorVersionFromVersion(scalaVersion) match {
    case "2.13" => "3.2.1"
    case "2.12" => "3.2.1"
    case "2.11" => "2.4.8"
    case v => throw new RuntimeException(s"Have not defined spark version for scala version $v")
  }
}

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
      "org.apache.spark" %% "spark-core" % scalaVersion(sparkDependency(_)).value % Provided,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.7.0",
      "org.scalatest" %% "scalatest-funsuite" % scalatestVersion % Test,
      "org.scalatest" %% "scalatest-shouldmatchers" % scalatestVersion % Test
    ),
    gitHubPagesSiteDir := baseDirectory.value / "target" / s"scala-$primaryScalaMinorVersion" / "api"
  )
