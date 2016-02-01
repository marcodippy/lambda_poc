package speed_layer

import batch_layer.DateUtils._
import com.datastax.driver.core.Cluster
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SaveMode}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


//ADD TIME MEASURE - METRICS

object StreamingEventCounter2 {
  val CASSANDRA_HOST = "127.0.0.1"
  val KAFKA_BROKER_LIST = "localhost:9092"

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    prepareDatabase(CASSANDRA_HOST)

    val conf = new SparkConf().setAppName("StreamingEventCounter").setMaster("local[*]")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.cassandra.connection.host", CASSANDRA_HOST)

    val ssc = new StreamingContext(conf, Seconds(1))
    val sqlContext = new SQLContext(ssc.sparkContext)

    val stream = getStream(ssc)

    stream.map { case (key, value) => value }.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val df = sqlContext.read.json(rdd).withColumn("count", lit(1))

        println("Aggregating by minute...")

        val eventsPerMinute = df.groupBy(
          col("event"),
          year(col("timestamp")) as "year",
          month(col("timestamp")) as "month",
          dayofmonth(col("timestamp")) as "day",
          hour(col("timestamp")) as "hour",
          minute(col("timestamp")) as "minute"
        ).count().cache()


        saveToCassandra(eventsPerMinute, "m", bdate_min(col("year"), col("month"), col("day"), col("hour"), col("minute")))

        println("Aggregating by hour...")

        val eventsPerHour = eventsPerMinute.groupBy(
          col("event"),
          col("year") as "year",
          col("month") as "month",
          col("day") as "day",
          col("hour") as "hour"
        ).count()

        saveToCassandra(eventsPerHour, "H", bdate_h(col("year"), col("month"), col("day"), col("hour")))

        eventsPerMinute.unpersist()
        /*
          there shouldn't be the need for saving the D,M,Y buckets into the realtime views because data in RT views expires as
          soon as the batch views absorb them (it should happen "often"")

          saveToCassandra(df, "D", bdate_d(year(col("timestamp")), month(col("timestamp")), dayofmonth(col("timestamp"))))
          saveToCassandra(df, "M", bdate_m(year(col("timestamp")), month(col("timestamp"))))
          saveToCassandra(df, "Y", bdate_y(year(col("timestamp"))))
        */
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def getStream(ssc: StreamingContext): InputDStream[(String, String)] = {
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> KAFKA_BROKER_LIST,
      "group.id" -> "speed_layer"
    )

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("events_topic"))
  }

  def saveToCassandra(df: DataFrame, bucket: String, bdate: Column) = {
    println(s"Saving data with bucket [$bucket] : DF size => ${df.count()}")

    df.withColumn("bucket", lit(bucket)).withColumn("bdate", bdate).
      select("event", "bucket", "bdate", "count").
      write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "events", "keyspace" -> "lambda_poc"))
      .save()
  }

  def prepareDatabase(cassandraHost: String) {
    val cluster = Cluster.builder().addContactPoint(cassandraHost).build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS lambda_poc WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS lambda_poc.events (event text, bucket text, bdate timestamp, count counter, PRIMARY KEY ((event, bucket), bdate) );")
    session.execute("TRUNCATE lambda_poc.events;")
  }
}
