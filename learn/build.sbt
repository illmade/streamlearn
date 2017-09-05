name := "StreamLearn"

version := "1.0"

scalaVersion := "2.11.11"

name := "SparkPlug"

val http4sVersion = "0.17.0-M3"

val sparkVersion = "2.2.0"

// Only necessary for SNAPSHOT releases
resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

libraryDependencies += "org.specs2" %% "specs2-core" % "3.8.8"

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.11.0.0"

libraryDependencies ++= Seq(
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-blaze-server" % http4sVersion,
  "org.http4s" %% "http4s-blaze-client" % http4sVersion
)

libraryDependencies ++= Seq(
  "org.http4s" %% "http4s-circe" % http4sVersion,
  // Optional for auto-derivation of JSON codecs
  "io.circe" %% "circe-generic" % "0.8.0",
  // Optional for string interpolation to JSON model
  "io.circe" %% "circe-literal" % "0.8.0"
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)