package batch_layer

import utils.Environment
import utils.Utils.measureTime

object BatchPipeline {

  def main(args: Array[String]) {
    val sqlContext = Environment.SPARK.newSqlContext("BatchPipeline")

    prepareEnvForTest(Environment.CASSANDRA.HOST, Environment.HDFS.HOST, Environment.HDFS.EVENTS_MASTERDATASET_DIR)

    measureTime {
      DataPreProcessing.preProcessData(sqlContext, Environment.HDFS.HOST, Environment.HDFS.FLUME_DATA_DIR, Environment.HDFS.EVENTS_MASTERDATASET_DIR, false)
    }
    measureTime {
      DataProcessor.processData(sqlContext, Environment.HDFS.EVENTS_MASTERDATASET_DIR)
    }

    sqlContext.sparkContext.stop()
    System.exit(0)
  }

  def prepareEnvForTest(cassandraHost: String, hdfsUrl: String, outputDir: String) {
    HdfsUtils.deleteFile(HdfsUtils.getFileSystem(hdfsUrl), outputDir, true)
    test.PrepareDatabase.prepareBatchDatabase(cassandraHost)
  }
}
