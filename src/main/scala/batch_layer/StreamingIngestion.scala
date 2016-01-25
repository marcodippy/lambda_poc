import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingIngestion extends App {
//  Logger.getRootLogger.setLevel(Level.WARN)
//
//  val conf = new SparkConf().setAppName("StreamingIngestion").setMaster("local[*]")
//  val ssc = new StreamingContext(conf, Seconds(4))
//
//  val topics = Set("events_topic")
//  val kafkaParams = Map[String, String](
//    "metadata.broker.list" -> "localhost:9092",
//    "group.id" -> "batch_layer"
//  )
//
//  val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//
//  val events = stream.map {
//    jsonEvent => Event.fromJson(jsonEvent._2)
//  }
//
//  events.saveAsTextFiles("./new_data/chunk")
//
//  ssc.start()
//  ssc.awaitTermination()
}
