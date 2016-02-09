package utils

import model.BucketModel._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, UserDefinedFunction}
import utils.DateUtils._

object DataFrameUtils {

  implicit class ColumnDecorator(val df: DataFrame) {
    def withBDateColumn(bucketType: BucketTypes.Value): DataFrame = {
      df.withColumn("bdate",
        bucketType match {
          case BucketTypes.minute => bucketStartDateCol(BucketTypes.minute)(col("year"), col("month"), col("day"), col("hour"), col("minute"))
          case BucketTypes.hour => bucketStartDateCol(BucketTypes.hour)(col("year"), col("month"), col("day"), col("hour"))
          case BucketTypes.day => bucketStartDateCol(BucketTypes.day)(col("year"), col("month"), col("day"))
          case BucketTypes.month => bucketStartDateCol(BucketTypes.month)(col("year"), col("month"))
          case BucketTypes.year => bucketStartDateCol(BucketTypes.year)(col("year"))
        }
      )
    }

    def withBucketColumn(bucketType: BucketTypes.Value): DataFrame = {
      df.withColumn("bucket", lit(bucketType.toString))
    }

    def withBucketAndBDateColumns(bucketType: BucketTypes.Value): DataFrame = {
      df.withBucketColumn(bucketType).withBDateColumn(bucketType)
    }
  }

  def yearCol(fromField: String): Column = {
    year(col(fromField)) as "year"
  }

  def monthCol(fromField: String): Column = {
    month(col(fromField)) as "month"
  }

  def dayCol(fromField: String): Column = {
    dayofmonth(col(fromField)) as "day"
  }

  def hourCol(fromField: String): Column = {
    hour(col(fromField)) as "hour"
  }

  def minuteCol(fromField: String): Column = {
    minute(col(fromField)) as "minute"
  }

  def bucketStartDateCol(bucketType: BucketTypes.Value, timestampFieldName: String): Column = {
    bucketType match {
      case BucketTypes.minute => bucketStartDateCol(BucketTypes.minute)(yearCol(timestampFieldName), monthCol(timestampFieldName), dayCol(timestampFieldName), hourCol(timestampFieldName), minuteCol(timestampFieldName))
      case BucketTypes.hour => bucketStartDateCol(BucketTypes.hour)(yearCol(timestampFieldName), monthCol(timestampFieldName), dayCol(timestampFieldName), hourCol(timestampFieldName))
      case BucketTypes.day => bucketStartDateCol(BucketTypes.day)(yearCol(timestampFieldName), monthCol(timestampFieldName), dayCol(timestampFieldName))
      case BucketTypes.month => bucketStartDateCol(BucketTypes.month)(yearCol(timestampFieldName), monthCol(timestampFieldName))
      case BucketTypes.year => bucketStartDateCol(BucketTypes.year)(yearCol(timestampFieldName))
    }
  }

  def bucketStartDateCol(bucketType: BucketTypes.Value): UserDefinedFunction = {
    bucketType match {
      case BucketTypes.minute => bdate_min
      case BucketTypes.hour => bdate_h
      case BucketTypes.day => bdate_d
      case BucketTypes.month => bdate_m
      case BucketTypes.year => bdate_y
      case _ => throw new IllegalArgumentException
    }
  }

  private val bdate_min = udf(
    (year: Int, month: Int, day: Int, hour: Int, minute: Int) => toSqlTimestamp(year, month, day, hour, minute)
  )

  private val bdate_h = udf(
    (year: Int, month: Int, day: Int, hour: Int) => toSqlTimestamp(year, month, day, hour, 0)
  )

  private val bdate_d = udf(
    (year: Int, month: Int, day: Int) => toSqlTimestamp(year, month, day, 0, 0)
  )

  private val bdate_m = udf(
    (year: Int, month: Int) => toSqlTimestamp(year, month, 1, 0, 0)
  )

  private val bdate_y = udf(
    (year: Int) => toSqlTimestamp(year, 1, 1, 0, 0)
  )

}
