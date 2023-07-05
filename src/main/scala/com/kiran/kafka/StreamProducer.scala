package com.kiran.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class StreamProducer(url:String, topic:String) {
  val streamUrl:String = url
  
  private def getProducer:KafkaProducer[String, String] = {
    new KafkaProducer(KafkaConfig.getProducerProperties())
  }
  
  def produce(){
   val streamDataSource = scala.io.Source.fromURL(streamUrl)
    val streamData = streamDataSource.getLines()
   val producer = getProducer
   
   for (data <- streamData){
     val record = new ProducerRecord(topic, "test",  data.toString())
     val meta = producer.send(record).get
     println("Event Pushed ::: Partition -> " + meta.partition() + " :: topic -> " + meta.topic())
   }

    streamDataSource.close()
  }
}

object StreamProducer extends App{
  (new StreamProducer("http://stream.meetup.com/2/rsvps","meets")).produce()
}