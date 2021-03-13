package com.spark.meetup.kafka

import java.util
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{StringDeserializer,ByteArrayDeserializer}
import org.apache.kafka.clients.consumer.ConsumerConfig



object KakfaMeetupConsumer {

  private val TOPIC = "meetup-topic-serial"
  private val BOOTSTRAP_SERVERS = "localhost:9092"

  def main(args: Array[String]): Unit = {
    consumeFromKafka
  }

  def consumeFromKafka() = {
    val props = new Properties

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1")

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    props.put("schema.registry.url", "http://localhost:8081")

    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer: KafkaConsumer[String, Object] = new KafkaConsumer[String, Object](props)
    consumer.subscribe(util.Arrays.asList(TOPIC))
    print("dfd")
    while (true) {
      val record = consumer.poll(1000).asScala
      for ( data <- record) {
          println(data.key())
      }

    }
  }

}
