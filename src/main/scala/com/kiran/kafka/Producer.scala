package com.kiran.kafka

import java.util.Properties

import org.apache.kafka.clients._
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import com.kiran.common.EventGenerator
import com.kiran.common.SalesEventGenerator
import com.kiran.common.EventGenerator
import java.io.FileInputStream
import com.kiran.common.EventGeneratorFactory
import org.apache.kafka.common.serialization.StringSerializer

class Producer(eventType:String = "sales") {
  /**
   * Producer class produces messages and push to kafka topic 
   */
  var topic = eventType
  
  //Returns Kafka Producer object
  private def getProducer():KafkaProducer[String, String] =  new KafkaProducer(KafkaConfig.getProducerProperties()) 
    
  def produce():Unit = {
    /**
     * Send the messages to the kafka topics
     */
    val producer = getProducer()
    val eventGen = EventGeneratorFactory(eventType)
    while(true){      
      val producerRecord = new ProducerRecord(topic,"test_key",eventGen.getEvent())
      val meta = producer.send(producerRecord).get
      println("Event pushed to topic ::: patition -> " + meta.partition() + " :: topic -> " + 
          meta.topic())
    }    
  }
}

// Companion object to test the producer
object Producer extends App{
  (new Producer("sales")).produce()
}