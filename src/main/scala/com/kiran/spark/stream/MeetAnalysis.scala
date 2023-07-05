package com.kiran.spark.stream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.{from_json, lit, schema_of_json,udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
//import org.apache.spark.sql.avro._
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.spark.sql.Column
import org.apache.spark.sql.avro.from_avro

object MeetAnalysis {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Meet Up Analysis").master("local[3]").getOrCreate()
    import spark.implicits._

    val jsonData =
      """
        |{"venue":{"venue_name":"Land-Grant Brewing Company","lon":-83.011475,"lat":39.957485,"venue_id":23211512},"visibility":"public","response":"no","guests":0,"member":{"member_id":226209277,"photo":"https:\/\/secure.meetupstatic.com\/photos\/member\/2\/4\/7\/0\/thumb_266289328.jpeg","member_name":"Darius"},"rsvp_id":1796398002,"mtime":1563185945842,"event":{"event_name":"Bar Meetup @ Land-Grant Brewing Company","event_id":"nzbbcryzkbzb","time":1563577200000,"event_url":"https:\/\/www.meetup.com\/columbus-meetup-and-hangout\/events\/261845275\/"},"group":{"group_topics":[{"urlkey":"nightlife","topic_name":"Nightlife"},{"urlkey":"20s-social","topic_name":"20's Social"},{"urlkey":"20s-30s-social","topic_name":"20's & 30's Social"},{"urlkey":"make-new-friends-from-all-walks-of-life","topic_name":"Make New Friends, from all Walks of Life"},{"urlkey":"socialnetwork","topic_name":"Social Networking"},{"urlkey":"newintown","topic_name":"New In Town"},{"urlkey":"local-activities","topic_name":"Local Activities"},{"urlkey":"eating-drinking-talking-laughing-etc","topic_name":"Eating, Drinking, Talking, Laughing, Etc"},{"urlkey":"pubs-bars","topic_name":"Pubs and Bars"},{"urlkey":"beer","topic_name":"Beer"},{"urlkey":"diningout","topic_name":"Dining Out"},{"urlkey":"lgbtfriends","topic_name":"LGBT"}],"group_city":"Columbus","group_country":"us","group_id":18540821,"group_name":"Columbus Friends Meetup (previously 20s Meetup and Hangout)","group_lon":-83,"group_urlname":"columbus-meetup-and-hangout","group_state":"OH","group_lat":39.97}}
      """.stripMargin

    val jsonschema = schema_of_json(jsonData)
    println(jsonschema)

    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("src/main/resources/meet.avsc")))
    val schema = new StructType()
      .add("venue", new StructType())
      .add("visibility", StringType)
      .add("response", StringType)
      .add("guests", IntegerType)
      .add("member",new StructType())
      .add("rsvp_id",LongType)
      .add("mtime",LongType)
      .add("event",new StructType())
      .add("group", new StructType())
//    println(jsonFormatSchema)
    val meetStreamDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "meets")
      .option("startingOffsets", "earliest")
      .option("group.id",0)
      .option("value.deserializer","classOf[StringDeserializer]")
      .load()
    //    val schema = schema_of_json(lit(meetStreamDf.selectExpr("CAST(value as String)").first))

    val query = meetStreamDf
      .selectExpr("CAST(key as String)", "CAST(value as String)")
//      .select(from_json($"value", schema) as "meet")
      .select(from_json($"value", jsonschema) as "comp_val")
      .select($"comp_val.venue.lat".as("lattitude"),$"comp_val.venue.lon",$"comp_val.event.time",$"comp_val.response")
//      .selectExpr("meet.response as respose", "meet.mtime as time")
      .writeStream
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.Continuous("5 second"))
      .start()
    //      println(schema)
        query.awaitTermination()

    val toString = udf((payload: Array[Byte]) => new String(payload))

    val staticDf = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "meets")
      .option("startingOffsets", "earliest")
      .option("value.deserializer","classOf[StringDeserializer]")
      .load()
    staticDf.cache()
    staticDf.select($"value".cast(StringType))
      .withColumn("comp_val",from_json($"value",schema))
      .printSchema()
    staticDf.select($"value".cast(StringType))
      .withColumn("comp_val",from_json(toString($"value"),schema)).show(2)

  }
}