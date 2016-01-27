package batch_layer

import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.sql.functions._

object Test extends App {

  val conf = new SparkConf().setAppName("BatchEventCounter").setMaster("local[1]").set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cassandra.output.batch.size.rows", "100000")
    .set("spark.cassandra.output.batch.size.bytes", "1000000")
    .set("spark.cassandra.output.concurrent.writes", "10")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  val rdd = sc.parallelize(
    List(
      ("LOGIN", "H", "2015-06-25 00:00", 500),
      ("LOGIN", "H", "2015-06-25 00:00", 500),

      ("LOGIN", "H", "2015-06-25 12:00", 1100),
      ("LOGIN", "H", "2015-06-25 12:00", 1100),

      ("LOGIN", "H", "2015-06-25 00:01", 101),
      ("LOGIN", "H", "2015-06-25 02:00", 2),
      ("LOGIN", "H", "2015-06-25 03:00", 3),
      ("LOGIN", "H", "2015-06-25 03:30", 3),
      ("LOGIN", "H", "2015-06-26 04:00", 4),
      ("LOGIN", "H", "2015-06-26 04:01:00", 4),
      ("LOGIN", "H", "2015-06-26 04:01:23", 4),
      ("LOGIN", "H", "2015-06-26 05:00", 5),
      ("LOGIN", "H", "2015-06-26 12:00", 200),
      ("LOGIN", "H", "2015-06-27 12:01", 201),
      ("LOGIN", "H", "2015-06-27 13:00", 202),
      ("LOGIN", "H", "2016-06-26 12:00", 200),
      ("LOGIN", "H", "2016-06-27 12:01", 201)
    )
  )

  val df = rdd.toDF("event", "bucket", "bdate", "count")
  df.show()

  df.groupBy(col("event"), col("bucket"), date_format(col("bdate"), "yyyy-MM-dd HH:mm")).agg(sum("count") as "count").show()
  df.groupBy(col("event"), col("bucket"), date_format(col("bdate"), "yyyy-MM-dd HH")).agg(sum("count") as "count").show()
  df.groupBy(col("event"), col("bucket"), date_format(col("bdate"), "yyyy-MM-dd")).agg(sum("count") as "count").show()
  df.groupBy(col("event"), col("bucket"), date_format(col("bdate"), "yyyy-MM")).agg(sum("count") as "count").show()
  df.groupBy(col("event"), col("bucket"), date_format(col("bdate"), "yyyy")).agg(sum("count") as "count").show()

  System.exit(0)
}
