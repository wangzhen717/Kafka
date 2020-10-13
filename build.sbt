name := "Kafka"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "2.6.0",
  "org.apache.zookeeper" % "zookeeper" % "3.5.8"
)