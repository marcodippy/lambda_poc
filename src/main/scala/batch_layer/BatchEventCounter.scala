package batch_layer

import com.databricks.spark.avro._
import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object BatchEventCounter extends App {

  Logger.getRootLogger.setLevel(Level.WARN)

  val conf = new SparkConf()
    .setAppName("BatchEventCounter")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "127.0.0.1")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val cc = new CassandraSQLContext(sc)
  cc.setKeyspace("lambda_poc")

  val connector = CassandraConnector(conf)

  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect()
  session.execute("CREATE KEYSPACE IF NOT EXISTS lambda_poc WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
  session.execute("CREATE TABLE IF NOT EXISTS lambda_poc.batch_events (event text, bucket text, bdate timestamp, count bigint, PRIMARY KEY ((event, bucket), bdate) );")
  session.execute("TRUNCATE lambda_poc.batch_events;")

  val df = sqlContext.read.avro("/tmp/output").cache()

  val bdate_h = udf(
    (year: Int, month: Int, day: Int, hour: Int) => new java.sql.Timestamp(new DateTime(year, month, day, hour, 0).toDate.getTime)
  )

  val bdate_d = udf(
    (year: Int, month: Int, day: Int) => new java.sql.Timestamp(new DateTime(year, month, day, 0, 0).toDate.getTime)
  )

  val bdate_m = udf(
    (year: Int, month: Int) => new java.sql.Timestamp(new DateTime(year, month, 1, 0, 0).toDate.getTime)
  )

  val bdate_y = udf(
    (year: Int) => new java.sql.Timestamp(new DateTime(year, 1, 1, 0, 0).toDate.getTime)
  )

  val eventsPerHour = df.groupBy("type", "year", "month", "day", "hour").count().withColumn("bdate", bdate_h(col("year"), col("month"), col("day"), col("hour"))).cache()
  val eventsPerDay = eventsPerHour.groupBy("type", "year", "month", "day").sum("count").withColumnRenamed("sum(count)", "count").withColumn("bdate", bdate_d(col("year"), col("month"), col("day"))).cache()
  val eventsPerMonth = eventsPerDay.groupBy("type", "year", "month").sum("count").withColumnRenamed("sum(count)", "count").withColumn("bdate", bdate_m(col("year"), col("month"))).cache()
  val eventsPerYear = eventsPerMonth.groupBy("type", "year").sum("count").withColumnRenamed("sum(count)", "count").withColumn("bdate", bdate_y(col("year"))).cache()


  eventsPerHour.show()
  eventsPerDay.show()
  eventsPerMonth.show()
  eventsPerYear.show()

  eventsPerHour.foreach(row => {
    connector.withSessionDo { session =>
      val count = row(5).asInstanceOf[java.lang.Long]
      val event = row(0).toString
      val bucket = "H"
      val bdate = row(6).asInstanceOf[java.sql.Timestamp]

      val prepared = session.prepare("INSERT INTO lambda_poc.batch_events (count, event, bucket, bdate) VALUES (?,?,?,?)")
      session.execute(prepared.bind(count, event, bucket, bdate))
    }
  })

  eventsPerDay.foreach(row => {
    connector.withSessionDo { session =>
      val count = row(4).asInstanceOf[java.lang.Long]
      val event = row(0).toString
      val bucket = "D"
      val bdate = row(5).asInstanceOf[java.sql.Timestamp]

      val prepared = session.prepare("INSERT INTO lambda_poc.batch_events (count, event, bucket, bdate) VALUES (?,?,?,?)")
      session.execute(prepared.bind(count, event, bucket, bdate))
    }
  })

  eventsPerMonth.foreach(row => {
    connector.withSessionDo { session =>
      val count = row(3).asInstanceOf[java.lang.Long]
      val event = row(0).toString
      val bucket = "M"
      val bdate = row(4).asInstanceOf[java.sql.Timestamp]

      val prepared = session.prepare("INSERT INTO lambda_poc.batch_events (count, event, bucket, bdate) VALUES (?,?,?,?)")
      session.execute(prepared.bind(count, event, bucket, bdate))
    }
  })

  eventsPerYear.foreach(row => {
    connector.withSessionDo { session =>
      val count = row(2).asInstanceOf[java.lang.Long]
      val event = row(0).toString
      val bucket = "Y"
      val bdate = row(3).asInstanceOf[java.sql.Timestamp]

      val prepared = session.prepare("INSERT INTO lambda_poc.batch_events (count, event, bucket, bdate) VALUES (?,?,?,?)")
      session.execute(prepared.bind(count, event, bucket, bdate))
    }
  })

  sc.stop()
}
