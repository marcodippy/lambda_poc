package batch_layer

import java.util.Date

import _root_.test.PrepareDatabase
import com.databricks.spark.avro._
import com.datastax.driver.core.BatchStatement
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import utils.DataFrameUtils._

object DataProcessor {

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    val startTime = new DateTime()

    val conf = new SparkConf().setAppName("BatchEventCounter").setMaster("local[*]")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.sql.shuffle.partitions", "1")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")

    PrepareDatabase.prepareBatchDatabase("127.0.0.1")

    processData(sqlContext, "hdfs://localhost:9000/events")

    sc.stop()
    val endTime = new DateTime()
    println(s"Processing completed in ${endTime.getMillis - startTime.getMillis} ms")
    System.exit(0)
  }

  def processData(sqlContext: SQLContext, inputDir: String) = {
    sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")

    //maybe it's a good idea to reduce the number of partitions (coalesce(numPartitions)). Investigate this
    val df = sqlContext.read.avro(inputDir).coalesce(4).cache()

    println("Retrieving distinct event types...")
    val EVENT_TYPES = sqlContext.sparkContext.broadcast[Seq[String]](df.select("event").distinct().collect().map(row => row.getAs[String]("event")))

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

    saveToCassandra(eventsPerMinute.withBDateColumn("m"), "m", EVENT_TYPES)
    saveToCassandra(eventsPerHour.withBDateColumn("H"), "H", EVENT_TYPES)
    saveToCassandra(eventsPerDay.withBDateColumn("D"), "D", EVENT_TYPES)
    saveToCassandra(eventsPerMonth.withBDateColumn("M"), "M", EVENT_TYPES)
    saveToCassandra(eventsPerYear.withBDateColumn("Y"), "Y", EVENT_TYPES)
  }

  private def saveToCassandra(df: DataFrame, bucket: String, events: Broadcast[Seq[String]]) = {
    println(s"Saving data with bucket [$bucket]")

    val cassandraConnector = CassandraConnector(df.sqlContext.sparkContext.getConf)

    import df.sqlContext.implicits._

    events.value.foreach(et => {
      println(s"\tSaving event $et")
      val dfe = df.where($"event" eqNullSafe et)

      dfe.foreachPartition(partition => {
        cassandraConnector.withSessionDo { session =>
          val prepared = session.prepare("UPDATE lambda_poc.batch_events SET count = ? WHERE event = ? AND bucket = ? AND bdate = ? IF count != ?;")

          partition.grouped(500).foreach(group => {
            val batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);

            group.foreach(row => {
              val count = row.getAs[java.lang.Long]("count")
              val date = row.getAs[Date]("bdate")

              batchStatement.add(prepared.bind(count, et, bucket, date, count))
            })

            session.execute(batchStatement)
          })
        }
      })
    })
  }

}

// NOTES
//Use partitioning to control the parallelism for writing to your data storage. Your data storage may not support too many concurrent connections.

//Use batch statement!

//Consider a static pool of db connection on each spark worker

//If you are writing to a sharded data storage, partition your RDD to match your sharding strategy.
//  That way each of your Spark workers only connects to one database shard, rather than each Spark worker connecting to every database shard.
