name := "Kafka"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "2.6.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.6.0",
  "com.google.code.gson" % "gson" % "2.8.6",
  "io.github.azhur" %% "kafka-serde-circe" % "0.5.0",
  "io.circe" %% "circe-generic" % "0.13.0"
)