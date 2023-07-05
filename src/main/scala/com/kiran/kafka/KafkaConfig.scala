package com.kiran.kafka

import java.util.Properties
import java.io.FileInputStream

object KafkaConfig {
  def getConsumerProperties():Properties = {
    val prop = new Properties()
    prop.load(new FileInputStream("src/main/resources/kafkaconsumer.properties"))
    prop
  }
  
  def getProducerProperties():Properties = {
    val prop:java.util.Properties = new Properties()
    prop.load(new FileInputStream("src/main/resources/kafkaproducer.properties"))
    prop
  }
}