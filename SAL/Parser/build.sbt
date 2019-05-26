ThisBuild / scalaVersion := "2.12.7"
ThisBuild / organization := "gov.sandia"
Compile/mainClass := Some("sal.parsing.sam.Sal")

lazy val parser = (project in file("."))
  .settings(
    name := "SAL-Parser",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  )


