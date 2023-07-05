import java.nio.file.{Files, Paths}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, avro}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Research {

  def jsonParseSpark(): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[3]").getOrCreate()
    import spark.implicits._
    val jsonData =
      """
        |{"venue":{"venue_name":"Land-Grant Brewing Company","lon":-83.011475,"lat":39.957485,"venue_id":23211512},"visibility":"public","response":"no","guests":0,"member":{"member_id":226209277,"photo":"https:\/\/secure.meetupstatic.com\/photos\/member\/2\/4\/7\/0\/thumb_266289328.jpeg","member_name":"Darius"},"rsvp_id":1796398002,"mtime":1563185945842,"event":{"event_name":"Bar Meetup @ Land-Grant Brewing Company","event_id":"nzbbcryzkbzb","time":1563577200000,"event_url":"https:\/\/www.meetup.com\/columbus-meetup-and-hangout\/events\/261845275\/"},"group":{"group_topics":[{"urlkey":"nightlife","topic_name":"Nightlife"},{"urlkey":"20s-social","topic_name":"20's Social"},{"urlkey":"20s-30s-social","topic_name":"20's & 30's Social"},{"urlkey":"make-new-friends-from-all-walks-of-life","topic_name":"Make New Friends, from all Walks of Life"},{"urlkey":"socialnetwork","topic_name":"Social Networking"},{"urlkey":"newintown","topic_name":"New In Town"},{"urlkey":"local-activities","topic_name":"Local Activities"},{"urlkey":"eating-drinking-talking-laughing-etc","topic_name":"Eating, Drinking, Talking, Laughing, Etc"},{"urlkey":"pubs-bars","topic_name":"Pubs and Bars"},{"urlkey":"beer","topic_name":"Beer"},{"urlkey":"diningout","topic_name":"Dining Out"},{"urlkey":"lgbtfriends","topic_name":"LGBT"}],"group_city":"Columbus","group_country":"us","group_id":18540821,"group_name":"Columbus Friends Meetup (previously 20s Meetup and Hangout)","group_lon":-83,"group_urlname":"columbus-meetup-and-hangout","group_state":"OH","group_lat":39.97}}
      """.stripMargin

    val rdd = spark.sparkContext.parallelize(Seq(jsonData))
    rdd.collect().foreach(println)
    val df = rdd.toDF("value")
    val jsonschema = schema_of_json(jsonData)
    println(jsonschema)

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
    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("src/main/resources/meet.avsc")))
      spark.read.schema(schema).json(rdd).show(10)
    df.select(from_json($"value",jsonschema)).printSchema()
    println(df.select(from_json($"value",jsonschema)).schema)

    df.select(from_json($"value",jsonschema)).collect()
    df.withColumn("comp_val",from_json($"value",jsonschema))
      .select($"comp_val.venue.lat".as("lattitude"),$"comp_val.venue.lon",$"comp_val.event.time").show()
    println("Schema from value")
//    df.select(from_json($"value",schema_of_json($"value"))).printSchema()
//    df.select(avro.from_avro($"value",jsonFormatSchema)).printSchema()

  }


  def sampleSpark: Unit ={
    val spark = SparkSession.builder().appName("test").master("local[3]").getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
//    val sData = scala.io.Source.fromURL("http://stream.meetup.com/2/rsvps")
//    for (x <- sData.getLines())
//      println("Line Number : " + x)
//
//    sData.close()
    jsonParseSpark()
//    sampleSpark
  }

}