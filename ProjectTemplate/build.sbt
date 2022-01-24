ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.14"

lazy val root = (project in file("."))
  .settings(
    name := "_PROJECT_TEMPLATE_",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2",
    libraryDependencies += "io.delta" %% "delta-core"  % "1.0.0"
  )
