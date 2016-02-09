package model

import org.joda.time.DateTime

import scala.collection.immutable.SortedSet

object BucketModel {

  object BucketTypes extends Enumeration {
    val minute = Value(1, "m")
    val hour = Value(2, "H")
    val day = Value(3, "D")
    val month = Value(4, "M")
    val year = Value(5, "Y")


    //TODO questo fallo che restituisce BucketTypes.Values
    def asList: List[String] = {
      this.values.map(_.toString).toList
    }
  }

  sealed trait Bucket {
    val `type`: BucketTypes.Value
    val start: DateTime

    def end: DateTime
  }

  object Bucket {
    def getByType(`type`: BucketTypes.Value, start: DateTime): Bucket = `type` match {
      case BucketTypes.minute => minute(start)
      case BucketTypes.hour => hour(start)
      case BucketTypes.day => day(start)
      case BucketTypes.month => month(start)
      case BucketTypes.year => year(start)
    }

    def minute(start: DateTime): Bucket = Minute(start.minuteOfHour().roundFloorCopy())

    def hour(start: DateTime): Bucket = Hour(start.hourOfDay().roundFloorCopy())

    def day(start: DateTime): Bucket = Day(start.dayOfMonth().roundFloorCopy())

    def month(start: DateTime): Bucket = Month(start.monthOfYear().roundFloorCopy())

    def year(start: DateTime): Bucket = Year(start.year().roundFloorCopy())

    private case class Minute(override val start: DateTime) extends Bucket {
      override val `type`: BucketTypes.Value = BucketTypes.minute

      override def end: DateTime = start.plusMinutes(1)
    }

    private case class Hour(override val start: DateTime) extends Bucket {
      override val `type`: BucketTypes.Value = BucketTypes.hour

      override def end: DateTime = start.plusHours(1)
    }

    private case class Day(override val start: DateTime) extends Bucket {
      override val `type`: BucketTypes.Value = BucketTypes.day

      override def end: DateTime = start.plusDays(1)
    }

    private case class Month(override val start: DateTime) extends Bucket {
      override val `type`: BucketTypes.Value = BucketTypes.month

      override def end: DateTime = start.plusMonths(1)
    }

    private case class Year(override val start: DateTime) extends Bucket {
      override val `type`: BucketTypes.Value = BucketTypes.year

      override def end: DateTime = start.plusYears(1)
    }

  }

}
