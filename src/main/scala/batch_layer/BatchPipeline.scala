package batch_layer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object BatchPipeline {
  val HDFS_URL = "hdfs://localhost:9000"
  val NEW_DATA_DIR = "/new_data/kafka/events_topic"
  val OUTPUT_DIR = "hdfs://localhost:9000/events"
  val CASSANDRA_HOST = "127.0.0.1"

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    //ADD TIME MEASURE!

    val sc = new SparkContext(getSparkConf())
    val sqlContext = new SQLContext(sc)

    prepareEnvForTest(sc, HDFS_URL, OUTPUT_DIR)

    DataPreProcessing.preProcessData(sqlContext, HDFS_URL, NEW_DATA_DIR, OUTPUT_DIR, false)

    DataProcessor.processData(sqlContext, OUTPUT_DIR)

    //add realtime views expiration mechanism here (maybe)

    System.exit(0)
  }

  def getSparkConf(): SparkConf =
    new SparkConf().setAppName("BatchePipeline").setMaster("local[*]")
      .set("spark.cassandra.connection.host", CASSANDRA_HOST)
      .set("spark.sql.shuffle.partitions", "1") //tune this value
  //    .set("spark.cassandra.output.batch.size.rows", "1") //tune this value
  //    .set("spark.cassandra.output.batch.size.bytes", "4096") //tune this value
  //    .set("spark.cassandra.output.concurrent.writes", "5") //tune this value


  def prepareEnvForTest(sc: SparkContext, hdfsUrl: String, outputDir: String) {
    HdfsUtils.deleteFile(HdfsUtils.getFileSystem(hdfsUrl), outputDir, true)
    DataProcessor.prepareDatabase(sc)
  }
}
