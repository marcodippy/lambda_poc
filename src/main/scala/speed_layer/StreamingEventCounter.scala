package speed_layer

import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.cql.CassandraConnector
import kafka.serializer.StringDecoder
import model.Event
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingEventCounter extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf = new SparkConf()
    .setAppName("StreamingEventCounter")
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "127.0.0.1")

  val ssc = new StreamingContext(conf, Seconds(4))

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

  val bucketedEvents = stream.flatMap {
    jsonEvent =>
      val e = Event.fromJson(jsonEvent._2)
      e.buckets.map(b => (b, e.`type`))
  }

  val bucketedEventCounts = bucketedEvents
    .groupByKey
    .map { case (bucket, events) =>
      val countsByEventType = events.groupBy(identity).mapValues(_.size)
      (bucket, countsByEventType)
    }

  //TODO use batch updates!

  bucketedEventCounts.foreachRDD { rdd =>
    rdd.foreach { case (bucket, aggregates) =>
      aggregates.foreach { case (eventType, count) =>

        println(s"${bucket.toString} - $eventType - ${count.toInt}")

        connector.withSessionDo { session =>
          val prepared = session.prepare("UPDATE lambda_poc.events SET count = count + ? WHERE event = ? AND bucket = ? AND bdate = ? ")
          session.execute(prepared.bind(new java.lang.Long(count), eventType, bucket.bucket, bucket.date.toDate))
        }

      }
    }

  }

  ssc.start()
  ssc.awaitTermination()
}
