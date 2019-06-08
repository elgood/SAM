ThisBuild / scalaVersion := "2.12.7"
ThisBuild / organization := "gov.sandia"
Compile/mainClass := Some("sal.parsing.sam.Sal")
//Compile/mainClass := Some("Test")

lazy val parser = (project in file("."))
  .settings(
    name := "SAL-Parser",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
  )


