lazy val scala213 = "2.13.11"
lazy val scala212 = "2.12.17"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala213, scala212, scala211)

def getPartialVersion(version: String): String = {
  CrossVersion.partialVersion(version) match {
    case Some((major, minor)) => s"$major.$minor"
    case _ => throw new RuntimeException(s"Have not defined spark version for scala version $version")
  }
}

def sparkDependency(scalaVersion: String): String = {
  getPartialVersion(scalaVersion) match {
    case "2.13" => "3.3.2"
    case "2.12" => "3.3.2"
    case "2.11" => "2.4.8"
    case v => throw new RuntimeException(s"Have not defined spark version for scala version $v")
  }
}

lazy val scalatestVersion = "3.2.16"

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
ThisBuild / scalaVersion := scala213

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
      "org.apache.spark" %% "spark-sql" % scalaVersion(sparkDependency(_)).value % Provided,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0",
      "org.scalatest" %% "scalatest-funsuite" % scalatestVersion % Test,
      "org.scalatest" %% "scalatest-shouldmatchers" % scalatestVersion % Test
    ),
    gitHubPagesSiteDir := baseDirectory.value / "target" / s"scala-${scalaVersion(getPartialVersion(_)).value}" / "api"
  )
