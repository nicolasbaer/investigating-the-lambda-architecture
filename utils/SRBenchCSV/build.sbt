name := "spark-sbt-test"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-assembly_2.10" % "1.0.0-SNAPSHOT",
  "org.apache.commons" % "commons-io" % "1.3.2",
  "org.apache.jena" % "jena-core" % "2.11.1"
)