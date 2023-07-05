package com.kiran.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization._
import java.util.Properties
import scala.reflect.internal.util.Collections
import scala.collection.JavaConverters._
import java.io.FileInputStream

class Consumer(topics:List[String]) {

  private def getConsumer: KafkaConsumer[String, String] = {
    new KafkaConsumer[String, String](KafkaConfig.getConsumerProperties())
  }

  def consume(): Unit = {
    val consumer = getConsumer
    consumer.subscribe(topics.asJavaCollection)

    while (true) {
      println("Polling for records")
      val results = consumer.poll(2000).asScala
      for (record <- results) {
        println(record.value())
      }
    }
  }
}

object Consumer extends App {
//  (new Consumer(List("sales"))).consume()
  (new Consumer(List("meets"))).consume()
}