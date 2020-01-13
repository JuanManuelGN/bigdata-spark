name := "spark-basic"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % "test"
)