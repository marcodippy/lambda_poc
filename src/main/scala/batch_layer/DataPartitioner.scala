package batch_layer

import com.databricks.spark.avro._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object DataPartitioner extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("DataPartitioner").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")

  import sqlContext.implicits._

  val hc = new Configuration();
  hc.set("fs.default.name", "hdfs://localhost:9000");
  val fs = FileSystem.get(hc);

  val newdata_dir = new org.apache.hadoop.fs.Path("/new_data/kafka/events_topic")
  val exists = fs.exists(newdata_dir)
  println(exists)
  val files = fs.listFiles(newdata_dir, true)

  val startTime = new DateTime()

  while (files.hasNext) {
    val next = files.next()

    if (next.isFile) {
      val filePath = next.getPath

      //.tmp files are currently used by Flume
      if (!filePath.toString.endsWith(".tmp")) {
        println(s"Processing file ${filePath.toString}")

        val json = sqlContext.read.json(filePath.toString)
          .withColumn("year", year(col("timestamp")))
          .withColumn("month", month(col("timestamp")))
          .withColumn("day", dayofmonth(col("timestamp")))
          .withColumn("hour", hour(col("timestamp")))
          .withColumn("minute", minute(col("timestamp")))

        json.write.mode(SaveMode.Append).partitionBy("year", "month", "day").avro("hdfs://localhost:9000/tmp/output")
        //fs.delete(filePath, false)
      }
    }
  }

  sc.stop()

  val endTime = new DateTime()
  println(s"Partitioning completed in ${endTime.getMillis - startTime.getMillis}ms")
}
