package batch_layer

import batch_layer.DateUtils._
import com.databricks.spark.avro._
import com.datastax.driver.core.{PreparedStatement, Session, Cluster}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object BatchEventCounter extends App {

  def getSession(connector: CassandraConnector): Session = Cluster.builder().addContactPoint("127.0.0.1").build().connect()

  def prepareDatabase(connector: CassandraConnector) = {
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS lambda_poc WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS lambda_poc.batch_events (event text, bucket text, bdate timestamp, count bigint, PRIMARY KEY ((event, bucket), bdate) );")
    session.execute("TRUNCATE lambda_poc.batch_events;")
    session.close()
  }

  Logger.getRootLogger.setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("BatchEventCounter").setMaster("local[*]").set("spark.cassandra.connection.host", "127.0.0.1")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val cc = new CassandraSQLContext(sc)
  cc.setKeyspace("lambda_poc")

  val connector = CassandraConnector(conf)
  prepareDatabase(connector)

  val df = sqlContext.read.avro("/tmp/output")

  val eventsPerHour = df.groupBy("type", "year", "month", "day", "hour").count().withColumn("bdate", bdate_h(col("year"), col("month"), col("day"), col("hour"))).cache()
  val eventsPerDay = eventsPerHour.groupBy("type", "year", "month", "day").sum("count").withColumnRenamed("sum(count)", "count").withColumn("bdate", bdate_d(col("year"), col("month"), col("day"))).cache()
  val eventsPerMonth = eventsPerDay.groupBy("type", "year", "month").sum("count").withColumnRenamed("sum(count)", "count").withColumn("bdate", bdate_m(col("year"), col("month"))).cache()
  val eventsPerYear = eventsPerMonth.groupBy("type", "year").sum("count").withColumnRenamed("sum(count)", "count").withColumn("bdate", bdate_y(col("year")))

  def saveToCassandra(session: Session, row: Row, countIndex: Int, eventTypeIndex: Int, bdateIndex: Int, bucket: String) = {
    val count = row(countIndex).asInstanceOf[java.lang.Long]
    val event = row(eventTypeIndex).toString
    val bdate = row(bdateIndex).asInstanceOf[java.sql.Timestamp]

    val prepared = session.prepare("INSERT INTO lambda_poc.batch_events (count, event, bucket, bdate) VALUES (?,?,?,?)")
    session.execute(prepared.bind(count, event, bucket, bdate))
  }

  //Use partitioning to control the parallelism for writing to your data storage. Your data storage may not support too many concurrent connections.
  //use batch!
  //consider a static pool of db connection on each spark worker
  //  If you are writing to a sharded data storage, partition your RDD to match your sharding strategy.
  //  That way each of your Spark workers only connects to one database shard, rather than each Spark worker connecting to every database shard.


  eventsPerHour.foreachPartition(_.foreach(row =>
    connector.withSessionDo { session => saveToCassandra(session, row, 5, 0, 6, "H") }
  ))

  eventsPerDay.foreachPartition(_.foreach(row =>
    connector.withSessionDo { session => saveToCassandra(session, row, 4, 0, 5, "D") }
  ))

  eventsPerMonth.foreachPartition(_.foreach(row =>
    connector.withSessionDo { session => saveToCassandra(session, row, 3, 0, 4, "M") }
  ))

  eventsPerYear.foreachPartition(_.foreach(row =>
    connector.withSessionDo { session => saveToCassandra(session, row, 2, 0, 3, "Y") }
  ))

  sc.stop()
  System.exit(0)
}
