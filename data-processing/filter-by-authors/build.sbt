ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "filter-by-authors",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"      % "3.0.3" % "provided",
      "org.apache.spark" %% "spark-sql"       % "3.0.3" % "provided",
      "org.apache.spark" %% "spark-streaming" % "3.0.3" % "provided",
      "org.apache.hadoop" % "hadoop-aws"      % "3.4.1"
    )
  )
