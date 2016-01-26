package model

import org.joda.time.DateTime

case class Bucket(bucket: String, date: DateTime)

object Minute {
  def apply(date: DateTime): Bucket = new Bucket("m", date.minuteOfHour().roundFloorCopy())
}

object Hour {
  def apply(date: DateTime): Bucket = new Bucket("H", date.hourOfDay().roundFloorCopy())
}

object Day {
  def apply(date: DateTime): Bucket = new Bucket("D", date.dayOfMonth().roundFloorCopy())
}

object Month {
  def apply(date: DateTime): Bucket = new Bucket("M", date.monthOfYear().roundFloorCopy())
}

object Year {
  def apply(date: DateTime): Bucket = new Bucket("Y", date.year().roundFloorCopy())
}

object BucketList {
  def apply(date: DateTime): List[Bucket] = List(
    Minute(date),
    Hour(date),
    Day(date),
    Month(date),
    Year(date)
  )
}