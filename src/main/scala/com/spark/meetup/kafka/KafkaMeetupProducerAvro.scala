package com.spark.meetup.kafka

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.{HttpURLConnection, MalformedURLException, URL}
import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.apache.avro.specific.SpecificDatumWriter
import java.io.ByteArrayOutputStream
import org.apache.avro.io._


object KafkaMeetupProducerAvro {

  private val TOPIC = "meetup-topic-serial"
  private val BOOTSTRAP_SERVERS = "localhost:9092"

  private def createProducer = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaMeetupProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    props.put("schema.registry.url", "http://localhost:8081")
    new KafkaProducer[String, Object](props)
  }

  def runProducer() ={

    val producer:Producer[String,Object] = createProducer

    val userSchema = "{\"type\":\"record\"," +
      "\"name\":\"meetuprecord\"," +
      "\"fields\":[{\"name\":\"rsvp\",\"type\":\"string\"}]}"

    val parser = new Schema.Parser
    val schema = parser.parse(userSchema)

    //val recordInjection = GenericAvroCodecs.toBinary(schema)
    val url = new URL("http://stream.meetup.com/2/rsvps")
    var conn = url.openConnection.asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod("GET")
      conn.setRequestProperty("Accept", "application/json")
      val redirect = conn.getHeaderField("Location")
      if (redirect != null) conn = new URL(redirect).openConnection.asInstanceOf[HttpURLConnection]

      if (conn.getResponseCode != 200) throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode)

      val br = new BufferedReader(new InputStreamReader(conn.getInputStream))

      var output: String = null
      var msgcount = 0

      val writer = new SpecificDatumWriter[GenericRecord](schema)
      val out = new ByteArrayOutputStream()
      val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)

      while ({output = br.readLine; output != null}) {

        //Loop through each RSVP and create Avro Record for Value in Key-Value pair
        val avroRecord = new GenericData.Record(schema)
        avroRecord.put("rsvp", output)
        writer.write(avroRecord, encoder)
        msgcount +=1
        val serializedBytes: Array[Byte] = out.toByteArray()
        val record = new ProducerRecord[String, Object](TOPIC, Integer.toString(msgcount), serializedBytes)
        val metadata = producer.send(record)

        println("sent record(key="+record.key+", value="+record.value +")")

      }

    } catch {
      case e: MalformedURLException =>
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
    }
    conn.disconnect()
    producer.flush()
    producer.close()

  }

  def main(args: Array[String]): Unit = {
    runProducer()
  }
}
