package batch_layer

import batch_layer.DateUtils._
import com.databricks.spark.avro._
import com.datastax.driver.core.Cluster
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object DataProcessor {

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    val startTime = new DateTime()

    val conf = new SparkConf().setAppName("BatchEventCounter").setMaster("local[*]")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.sql.shuffle.partitions", "1")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    prepareDatabase(sc)

    processData(sqlContext, "hdfs://localhost:9000/tmp/output")

    sc.stop()
    val endTime = new DateTime()
    println(s"Processing finished in ${endTime.getMillis - startTime.getMillis} ms")


  }

  def processData(sqlContext: SQLContext, inputDir: String) = {
    sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
    val df = sqlContext.read.avro(inputDir).cache()

    println("Aggregating by minute...")
    val eventsPerMinute = df.groupBy("event", "year", "month", "day", "hour", "minute").count().cache()

    println("Aggregating by hour...")
    val eventsPerHour = eventsPerMinute.groupBy("event", "year", "month", "day", "hour").agg(sum("count") as "count").cache()

    println("Aggregating by day...")
    val eventsPerDay = eventsPerHour.groupBy("event", "year", "month", "day").agg(sum("count") as "count").cache()

    println("Aggregating by month...")
    val eventsPerMonth = eventsPerDay.groupBy("event", "year", "month").agg(sum("count") as "count").cache()

    println("Aggregating by year...")
    val eventsPerYear = eventsPerMonth.groupBy("event", "year").agg(sum("count") as "count")

    saveToCassandra(eventsPerMinute, "m", bdate_min(col("year"), col("month"), col("day"), col("hour"), col("minute")))
    saveToCassandra(eventsPerHour, "H", bdate_h(col("year"), col("month"), col("day"), col("hour")))
    saveToCassandra(eventsPerDay, "D", bdate_d(col("year"), col("month"), col("day")))
    saveToCassandra(eventsPerMonth, "M", bdate_m(col("year"), col("month")))
    saveToCassandra(eventsPerYear, "Y", bdate_y(col("year")))
  }

  def prepareDatabase(sc: SparkContext) = {
    val cc = new CassandraSQLContext(sc)
    cc.setKeyspace("lambda_poc")

    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS lambda_poc WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS lambda_poc.batch_events (event text, bucket text, bdate timestamp, count bigint, PRIMARY KEY ((event, bucket), bdate) );")
    session.execute("TRUNCATE lambda_poc.batch_events;")
    session.close()
  }

  private def saveToCassandra(df: DataFrame, bucket: String, bdate: Column) = {
    println(s"Saving data with bucket [$bucket]")

    df.withColumn("bucket", lit(bucket)).withColumn("bdate", bdate).
      select("event", "bucket", "bdate", "count").
      write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "batch_events", "keyspace" -> "lambda_poc"))
      .save()
  }
}

// NOTES
//Use partitioning to control the parallelism for writing to your data storage. Your data storage may not support too many concurrent connections.

//Use batch statement!

//Consider a static pool of db connection on each spark worker

//If you are writing to a sharded data storage, partition your RDD to match your sharding strategy.
//  That way each of your Spark workers only connects to one database shard, rather than each Spark worker connecting to every database shard.
