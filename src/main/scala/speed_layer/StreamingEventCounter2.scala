package speed_layer

import batch_layer.DateUtils._
import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.cql.CassandraConnector
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingEventCounter2 extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf = new SparkConf()
    .setAppName("StreamingEventCounter")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "127.0.0.1")

  val ssc = new StreamingContext(conf, Seconds(4))
  val sqlContext = new SQLContext(ssc.sparkContext)

  val cc = new CassandraSQLContext(ssc.sparkContext)
  cc.setKeyspace("lambda_poc")

  val connector = CassandraConnector(conf)

  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect()
  session.execute("CREATE KEYSPACE IF NOT EXISTS lambda_poc WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
  session.execute("CREATE TABLE IF NOT EXISTS lambda_poc.events (event text, bucket text, bdate timestamp, count counter, PRIMARY KEY ((event, bucket), bdate) );")
  session.execute("TRUNCATE lambda_poc.events;")

  val topics = Set("events_topic")
  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> "localhost:9092",
    "group.id" -> "speed_layer"
  )

  val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

  stream.map(_._2).foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      val df = sqlContext.read.json(rdd).withColumnRenamed("type", "event").withColumn("count", lit(1))

      saveToCassandra(df, "m", bdate_min(year(col("timestamp")), month(col("timestamp")), dayofmonth(col("timestamp")), hour(col("timestamp")), minute(col("timestamp"))))
      saveToCassandra(df, "H", bdate_h(year(col("timestamp")), month(col("timestamp")), dayofmonth(col("timestamp")), hour(col("timestamp"))))
      saveToCassandra(df, "D", bdate_d(year(col("timestamp")), month(col("timestamp")), dayofmonth(col("timestamp"))))
      saveToCassandra(df, "M", bdate_m(year(col("timestamp")), month(col("timestamp"))))
      saveToCassandra(df, "Y", bdate_y(year(col("timestamp"))))
    }
  })

  def saveToCassandra(df: DataFrame, bucket: String, bdate: Column) = {
    println(s"Saving data with bucket [$bucket]")

    df.withColumn("bucket", lit(bucket)).withColumn("bdate", bdate).
      select("event", "bucket", "bdate", "count").
      write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "events", "keyspace" -> "lambda_poc"))
      .save()
  }

  ssc.start()
  ssc.awaitTermination()
}
