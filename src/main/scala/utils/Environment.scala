package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Environment {

  object HDFS {
    val HOST = "hdfs://localhost:9000"
    val FLUME_DATA_DIR = "/new_data/kafka/events_topic"
    val EVENTS_MASTERDATASET_DIR = HOST + "/events"
  }

  object CASSANDRA {
    val HOST = "127.0.0.1"
  }

  object SPARK {
    val MASTER = "local[*]"

    def newConf(appName: String): SparkConf = {
      new SparkConf().setAppName(appName).setMaster(SPARK.MASTER)
        .set("spark.cassandra.connection.host", CASSANDRA.HOST)
        .set("spark.sql.shuffle.partitions", "1") //tune this value
      //.set("spark.cassandra.output.batch.size.rows", "1") //tune this value
      //.set("spark.cassandra.output.batch.size.bytes", "4096") //tune this value
      //.set("spark.cassandra.output.concurrent.writes", "5") //tune this value
      //.set("spark.eventLog.enabled", "true")
    }

    def newContext(appName: String): SparkContext = {
      val sc = new SparkContext(newConf(appName))
      Logger.getRootLogger.setLevel(Level.WARN)
      sc
    }

    def newStreamingContext(appName: String, duration: Duration): StreamingContext = {
      val ssc = new StreamingContext(newConf(appName), duration)
      Logger.getRootLogger.setLevel(Level.WARN)
      ssc
    }

    def newSqlContext(appName: String): SQLContext = {
      new SQLContext(newContext(appName))
    }
  }

  object KAFKA {
    val BROKER_LIST = "localhost:9092"
  }

}
