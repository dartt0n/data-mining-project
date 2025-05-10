ThisBuild / version := "x"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "data-overview",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"      % "3.0.3" % "provided",
      "org.apache.spark" %% "spark-sql"       % "3.0.3" % "provided",
      "org.apache.spark" %% "spark-streaming" % "3.0.3" % "provided",
      "org.apache.hadoop" % "hadoop-aws"      % "3.4.1"
    )
  )
