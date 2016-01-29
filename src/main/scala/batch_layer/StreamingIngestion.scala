package batch_layer

import batch_layer.DateUtils._
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object StreamingIngestion extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf = new SparkConf()
    .setAppName("StreamingEventCounter")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "127.0.0.1")

  val ssc = new StreamingContext(conf, Seconds(5))
  val sqlContext = new SQLContext(ssc.sparkContext)

  val topics = Set("events_topic")
  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> "localhost:9092",
    "group.id" -> "batch_layer"
  )

  val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

  stream.map(_._2).foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      val messages = sqlContext.read.json(rdd)
      messages.write.mode(SaveMode.Append).json("pathToFile")
    }
  })
}
