package batch_layer

import org.apache.spark.sql.functions._
import org.joda.time.{DateTimeZone, DateTime}

object DateUtils {

  def toSqlTimestamp(year: Int, month: Int, day: Int, hour: Int, minute: Int): java.sql.Timestamp =
    new java.sql.Timestamp(new DateTime(year, month, day, hour, minute, 0, DateTimeZone.getDefault).getMillis)

  val bdate_min = udf(
    (year: Int, month: Int, day: Int, hour: Int, minute: Int) => toSqlTimestamp(year, month, day, hour, minute)
  )

  val bdate_h = udf(
    (year: Int, month: Int, day: Int, hour: Int) => toSqlTimestamp(year, month, day, hour, 0)
  )

  val bdate_d = udf(
    (year: Int, month: Int, day: Int) => toSqlTimestamp(year, month, day, 0, 0)
  )

  val bdate_m = udf(
    (year: Int, month: Int) => toSqlTimestamp(year, month, 1, 0, 0)
  )

  val bdate_y = udf(
    (year: Int) => toSqlTimestamp(year, 1, 1, 0, 0)
  )

}
