package batch_layer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import utils.DataFrameUtils._

object DataPreProcessing {
  def main(args: Array[String]): Unit = {
    val startTime = new DateTime()

    val sc = new SparkContext(new SparkConf().setAppName("DataPreProcessing").setMaster("local[*]").set("spark.eventLog.enabled", "true"))
    Logger.getRootLogger.setLevel(Level.WARN)

    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")

    preProcessData(sqlContext, "hdfs://localhost:9000", "/new_data/kafka/events_topic", "hdfs://localhost:9000/events_parquet")

    sc.stop()

    val endTime = new DateTime()
    println(s"Partitioning completed in ${endTime.getMillis - startTime.getMillis}ms")
  }

  def preProcessData(sqlContext: SQLContext, hdfsUrl: String, inputPath: String, outputPath: String, deleteInputFiles: Boolean = false) = {
    val fs = HdfsUtils.getFileSystem(hdfsUrl)
    val files = HdfsUtils.listFiles(fs, inputPath, true)(fileName => !fileName.endsWith(".tmp"))

    files match {
      case None => println("Source dir doesn't exist")
      case Some(x) => {
        x.foreach(file => {
          println(s"Processing file $file")

          val dataFrame = getDataFrame(sqlContext, file)
          writeDataFrame(sqlContext, dataFrame, outputPath, Seq("year", "month", "day"))

          if (deleteInputFiles)
            HdfsUtils.deleteFile(fs, file, false)
        })
      }
    }
  }

  private def getDataFrame(sqlContext: SQLContext, sourceFilePath: String): DataFrame = {
    sqlContext.read.json(sourceFilePath)
      .withColumn("year", yearCol("timestamp"))
      .withColumn("month", monthCol("timestamp"))
      .withColumn("day", dayCol("timestamp"))
      .withColumn("hour", hourCol("timestamp"))
      .withColumn("minute", minuteCol("timestamp"))
  }

  private def writeDataFrame(sqlContext: SQLContext, df: DataFrame, outputDir: String, partitionBy: Seq[String]) = {
    import com.databricks.spark.avro._
    sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
    //partitionBy is forcing me to add additional columns to the dataset (all the partition keys). Find out if
    //there is an easy way to avoid that
    df.write.mode(SaveMode.Append).partitionBy(partitionBy: _*).parquet(outputDir)
  }

}
