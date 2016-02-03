package utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import utils.DateUtils._
import org.apache.spark.sql.Column

object DataFrameUtils {

  implicit class ColumnDecorator(val df: DataFrame) {
    def withBDateColumn(bucket: String): DataFrame = {
      df.withColumn("bdate",
        bucket match {
          case "m" => bdate_min(col("year"), col("month"), col("day"), col("hour"), col("minute"))
          case "H" => bdate_h(col("year"), col("month"), col("day"), col("hour"))
          case "D" => bdate_d(col("year"), col("month"), col("day"))
          case "M" => bdate_m(col("year"), col("month"))
          case "Y" => bdate_y(col("year"))
        }
      )
    }

    def withBucketColumn(bucket: String): DataFrame = {
      df.withColumn("bucket", lit(bucket))
    }

    def withBucketAndBDateColumns(bucket: String): DataFrame = {
      df.withBucketColumn(bucket).withBDateColumn(bucket)
    }
  }

  def yearCol(fromField: String) : Column = {
    year(col(fromField)) as "year"
  }

  def monthCol(fromField: String) : Column = {
    month(col(fromField)) as "month"
  }

  def dayCol(fromField: String) : Column = {
    dayofmonth(col(fromField)) as "day"
  }

  def hourCol(fromField: String) : Column = {
    hour(col(fromField)) as "hour"
  }

  def minuteCol(fromField: String) : Column = {
    minute(col(fromField)) as "minute"
  }
}
