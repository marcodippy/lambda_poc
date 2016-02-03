package speed_layer

import kafka.serializer.StringDecoder
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
    Logger.getRootLogger.setLevel(Level.WARN)

    test.PrepareDatabase.prepareRealTimeDatabase(CASSANDRA_HOST)

    val conf = new SparkConf().setAppName("StreamingEventCounter").setMaster("local[*]")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.cassandra.connection.host", CASSANDRA_HOST)

    val ssc = new StreamingContext(conf, Seconds(1))
    val sqlContext = new SQLContext(ssc.sparkContext)

    val stream = getStream(ssc)

    stream.map { case (key, value) => value }.foreachRDD(rdd => {
      if (!rdd.isEmpty()) processMessagesRdd(rdd, sqlContext)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def processMessagesRdd(rdd: RDD[String], sqlContext: SQLContext) = {
    val df = sqlContext.read.json(rdd).withColumn("count", lit(1))

    println("Aggregating by minute...")
    val eventsPerMinute = df.groupBy(
      col("event"),
      yearCol("timestamp"), monthCol("timestamp"), dayCol("timestamp"), hourCol("timestamp"), minuteCol("timestamp")
    ).count().cache()

    saveToCassandra(eventsPerMinute.withBucketAndBDateColumns("m"), "m")

    println("Aggregating by hour...")
    val eventsPerHour = eventsPerMinute.groupBy(
      col("event"),
      col("year"), col("month"), col("day"), col("hour")
    ).agg(sum("count") as "count")

    saveToCassandra(eventsPerHour.withBucketAndBDateColumns("H"), "H")

    eventsPerMinute.unpersist()
  }

  def getStream(ssc: StreamingContext): InputDStream[(String, String)] = {
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> KAFKA_BROKER_LIST,
      "group.id" -> "speed_layer"
    )
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("events_topic"))
  }

  def saveToCassandra(df: DataFrame, bucket: String) = {
    println(s"Saving data with bucket [$bucket] : DF size => ${df.count()}")

    df.select("event", "bucket", "bdate", "count").
      write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "events", "keyspace" -> "lambda_poc"))
      .save()
  }
}
