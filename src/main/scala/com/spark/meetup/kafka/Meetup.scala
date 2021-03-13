package com.spark.meetup.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, from_json, lit}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.log4j.Logger


object Meetup extends App {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val TOPIC = "meetup-topic-serial"
  val BOOTSTRAP_SERVERS = "localhost:9092"

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Spark Meetup Kakfa")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  val schemaString = """{
				"type": "record",
				"name": "meetuprecord",
				"fields": [{"name":  "rsvp", "type": "string"}]
				}""""

  //Load from Kafka topic
  val load = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer.class")
    .load()

  //Convert RSVP from ByteArray to String
  import spark.implicits._
  val df = load.select(col("value").as[Array[Byte]])
    .map(d => {
      val rec = new GenericDatumReader[GenericRecord](new Schema.Parser().parse(schemaString))
        .read(null, DecoderFactory.get().binaryDecoder(d, null))
      val rsvp = rec.get("rsvp").asInstanceOf[org.apache.avro.util.Utf8].toString
      rsvp
    })

  //Schema for RSVPs
  val rsvpSchema = new StructType().add("response", StringType)
    .add("event", new StructType().add("event_name", StringType).add("event_id", StringType))


  val rsvps = df.select(from_json(col("value"), rsvpSchema, Map.empty[String, String]) as "json")
    .select(col("json.response"), col("json.event.event_id"), col("json.event.event_name"))

  val rsvp_responses = rsvps.groupBy("event_id", "event_name", "response").count()

  val meetup_key_val = rsvp_responses.withColumn("key", lit("100")).select(col("key"),
    concat(col("event_id"), lit(" | "), col("event_name"), lit(" | "), col("response"), lit(" | "), col("count")).alias("value"))

  //Writing output to Kafka Sink
  val result = meetup_key_val.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", "meetup-sink")
    .option("checkpointLocation", "chk-point-dr")
    .outputMode("update")
    .start()


  logger.info("Listening and Writing to "+TOPIC)
  result.awaitTermination()



}
