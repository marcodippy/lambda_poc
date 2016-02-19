package batch_layer

import java.util.Date

import com.datastax.driver.core.BatchStatement
import com.datastax.spark.connector.cql.CassandraConnector
import model.BucketModel.BucketTypes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import utils.DataFrameUtils._
import utils.Environment
import utils.Utils.measureTime

object DataProcessor {

  def main(args: Array[String]) {
    val sqlContext = Environment.SPARK.newSqlContext("DataProcessor")

    //PrepareDatabase.prepareBatchDatabase("127.0.0.1")

    measureTime {
      processData(sqlContext, Environment.HDFS.EVENTS_MASTERDATASET_DIR)
    }

    sqlContext.sparkContext.stop()
    System.exit(0)
  }

  def processData(sqlContext: SQLContext, inputDir: String) = {
    //TODO tune the number of partitions here
    //initially the number of partitions should be the same as the HDFS blocks (it depends also on how many file you're retrieving)
    //consider also to repartition data by event before the aggregation
    //anyway, probably the bottelneck is only on the cassandra write side
    val df = sqlContext.read.parquet(inputDir).coalesce(4).cache()

    println("Retrieving distinct event types...")
    //TODO if you keep an anagraphic of the events types you can load it once in the main driver and broadcast it to each worker
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

    val cassandraConnector = CassandraConnector(df.sqlContext.sparkContext.getConf)

    saveToCassandra(eventsPerMinute.withBDateColumn(BucketTypes.minute), BucketTypes.minute, EVENT_TYPES, cassandraConnector)
    saveToCassandra(eventsPerHour.withBDateColumn(BucketTypes.hour), BucketTypes.hour, EVENT_TYPES, cassandraConnector)
    saveToCassandra(eventsPerDay.withBDateColumn(BucketTypes.day), BucketTypes.day, EVENT_TYPES, cassandraConnector)
    saveToCassandra(eventsPerMonth.withBDateColumn(BucketTypes.month), BucketTypes.month, EVENT_TYPES, cassandraConnector)
    saveToCassandra(eventsPerYear.withBDateColumn(BucketTypes.year), BucketTypes.year, EVENT_TYPES, cassandraConnector)
  }

  //TODO try other approaches to improve performances:
  // 1) repartition by event
  // 2) sortWithinPartition; you can iterate only once over the rows! You could use a foldLeft...
  private def saveToCassandra(df: DataFrame, bucket: BucketTypes.Value, events: Broadcast[Seq[String]], cassandraConnector: CassandraConnector) = {
    println(s"Saving data with bucket [$bucket]")

    import df.sqlContext.implicits._

    events.value.foreach(et => {
      println(s"\tSaving event $et")
      //TODO probably you can optimize it, instead of querying for each event, you can iterate only once...
      //or you can sortWithinPartition and then scan sequencially the rows
      val dfe = df.where($"event" <=> et)

      dfe.foreachPartition(partition => {
        cassandraConnector.withSessionDo { session =>
          val prepared = session.prepare("UPDATE lambda_poc.batch_events SET count = ? WHERE event = ? AND bucket = ? AND bdate = ? IF count != ?;")

          //TODO tune the number of statements in a batch
          partition.grouped(500).foreach(group => {
            val batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);

            group.foreach(row => {
              val count = row.getAs[java.lang.Long]("count")
              val date = row.getAs[Date]("bdate")

              batchStatement.add(prepared.bind(count, et, bucket.toString, date, count))
            })

            session.execute(batchStatement)
          })
        }
      })
    })
  }
}