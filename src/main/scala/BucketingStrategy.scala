import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.DateUtils
import org.joda.time.DateTime

case class Bucket(bucket: String, date: DateTime)

object Minute {
  def apply(date: DateTime): Bucket = new Bucket("M", date.minuteOfHour().roundFloorCopy())
}

object Hour {
  def apply(date: DateTime): Bucket = new Bucket("H", date.hourOfDay().roundFloorCopy())
}

object Day {
  def apply(date: DateTime): Bucket = new Bucket("D", date.dayOfMonth().roundFloorCopy())
}

object BucketList {
  def apply(date: DateTime): List[Bucket] = List(
    Minute(date),
    Hour(date),
    Day(date)
  )
}