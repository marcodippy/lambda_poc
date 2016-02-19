package batch_layer

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import utils.Utils.measureTime
import utils.DataFrameUtils._
import utils.Environment

object DataPreProcessing {
  def main(args: Array[String]): Unit = {
    val sqlContext = Environment.SPARK.newSqlContext("DataPreProcessing")

    measureTime {
      preProcessData(sqlContext, Environment.HDFS.HOST, Environment.HDFS.FLUME_DATA_DIR, Environment.HDFS.EVENTS_MASTERDATASET_DIR)
    }

    sqlContext.sparkContext.stop()
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

  private def getDataFrame(sqlContext: SQLContext, sourceFilePath: String): DataFrame =
    sqlContext.read.json(sourceFilePath)
      .withColumn("year", yearCol("timestamp"))
      .withColumn("month", monthCol("timestamp"))
      .withColumn("day", dayCol("timestamp"))
      .withColumn("hour", hourCol("timestamp"))
      .withColumn("minute", minuteCol("timestamp"))

  //partitionBy is forcing me to add additional columns to the dataset (all the partition keys).
  //Find out if there is an easy way to avoid that
  private def writeDataFrame(sqlContext: SQLContext, df: DataFrame, outputDir: String, partitionBy: Seq[String]) =
    df.write.mode(SaveMode.Append).partitionBy(partitionBy: _*).parquet(outputDir)

}
