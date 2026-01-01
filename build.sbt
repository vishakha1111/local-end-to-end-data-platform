ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "com.vishakha"

lazy val root = (project in file("."))
  .settings(
    name := "local-end-to-end-data-platform",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql"  % "3.5.1"
    )
  )
