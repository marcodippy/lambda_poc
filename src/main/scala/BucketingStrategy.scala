import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.DateUtils

case class Bucket(bucket: String, date: Date)

object Minute {
  def apply(date: Date): Bucket = new Bucket("M", DateUtils.truncate(date, Calendar.MINUTE))
}

object Hour {
  def apply(date: Date): Bucket = new Bucket("H", DateUtils.truncate(date, Calendar.HOUR))
}

object Day {
  def apply(date: Date): Bucket = new Bucket("D", DateUtils.truncate(date, Calendar.DAY_OF_MONTH))
}

object BucketList {
  def apply(date: Date): List[Bucket] = List(
    Minute(date),
    Hour(date),
    Day(date)
  )
}