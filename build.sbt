name := "SparkMeetupKafka"
organization := "com.spark.meetup.kafka"
version := "0.1"
scalaVersion := "2.12.10"
autoScalaLibrary := false
val sparkVersion = "3.0.0"


val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.avro"  %  "avro"  %  "1.7.7",
  "org.apache.kafka" %% "kafka" % "0.10.2.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

)

libraryDependencies ++= sparkDependencies

