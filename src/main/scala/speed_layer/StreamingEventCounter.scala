package speed_layer

import kafka.serializer.StringDecoder
import model.BucketModel.{Bucket, BucketTypes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.DataFrameUtils._

//TODO ADD TIME MEASURE - METRICS
object StreamingEventCounter {
  val CASSANDRA_HOST = "127.0.0.1"
  val KAFKA_BROKER_LIST = "localhost:9092"

  def main(args: Array[String]) {
    test.PrepareDatabase.prepareRealTimeDatabase(CASSANDRA_HOST)

    val conf = new SparkConf().setAppName("StreamingEventCounter").setMaster("local[*]")
//      .set("spark.eventLog.enabled", "true")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.cassandra.connection.host", CASSANDRA_HOST)

    val ssc = new StreamingContext(conf, Seconds(1))
    val sqlContext = new SQLContext(ssc.sparkContext)

    Logger.getRootLogger.setLevel(Level.WARN)

    val stream = getStream(ssc)

    stream.map { case (key, value) => value }.foreachRDD(rdd => {
      if (!rdd.isEmpty()) processMessagesRdd(rdd, sqlContext)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def processMessagesRdd(rdd: RDD[String], sqlContext: SQLContext) = {
    val df = sqlContext.read.json(rdd)

    println("Aggregating by minute...")
    val eventsPerMinute = df.groupBy(col("event"), bucketStartDateCol(BucketTypes.minute, "timestamp") as "bdate").count().cache()

    println(s"Saving data with bucket [m] : DF size => ${eventsPerMinute.count()}")
    saveToCassandra(eventsPerMinute.withBucketColumn(BucketTypes.minute))

    println("Aggregating by hour...")
    val eventsPerHour = eventsPerMinute.groupBy(col("event"), bucketStartDateCol(BucketTypes.hour, "bdate") as "bdate").agg(sum("count") as "count")

    println(s"Saving data with bucket [H] : DF size => ${eventsPerHour.count()}")
    saveToCassandra(eventsPerHour.withBucketColumn(BucketTypes.hour))

    eventsPerMinute.unpersist()
  }

  def getStream(ssc: StreamingContext): InputDStream[(String, String)] = {
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> KAFKA_BROKER_LIST,
      "group.id" -> "speed_layer"
    )
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("events_topic"))
  }

  //TODO try to use the low level API for batching inserts and try session.executeAsynch
  def saveToCassandra(df: DataFrame) = {
    df.
      write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "events", "keyspace" -> "lambda_poc"))
      .save()
  }
}
