package serving_layer

import com.datastax.driver.core.Row
import org.joda.time.DateTime

case class Range(left: DateTime, right: DateTime) {
  require(right.isAfter(left) || (left equals right))

  def lengthInMillis = this.right.getMillis - this.left.getMillis

  def startsWith(otherRange: Range): Boolean = this.left equals otherRange.left

  def endsWith(otherRange: Range): Boolean = this.right equals otherRange.right

  def isWider(otherRange: Range): Boolean = this.lengthInMillis > otherRange.lengthInMillis

  def isEmpty: Boolean = this.left equals this.right
}

object EventRow {
  def fromRow(row: Row): EventRow = {
    EventRow(
      row.getString("event"),
      Bucket.get(row.getString("bucket"), new DateTime(row.getTimestamp("bdate"))),
      row.getLong("count"))
  }
}

case class EventRow(event: String, bucket: Bucket, count: Long)

sealed trait Bucket {
  val b_type: String
  val date: DateTime

  def end: DateTime
}

object Bucket {
  def get(btype: String, date: DateTime): Bucket = btype match {
    case "m" => minute(date)
    case "H" => hour(date)
    case "D" => day(date)
    case "M" => month(date)
    case "Y" => year(date)
  }

  val bucketList = List("Y", "M", "D", "H", "m")

  def minute(date: DateTime): Bucket = Minute(date.secondOfMinute().roundFloorCopy())

  def hour(date: DateTime): Bucket = Hour(date.hourOfDay().roundFloorCopy())

  def day(date: DateTime): Bucket = Day(date.dayOfMonth().roundFloorCopy())

  def month(date: DateTime): Bucket = Month(date.monthOfYear().roundFloorCopy())

  def year(date: DateTime): Bucket = Year(date.year().roundFloorCopy())

  private case class Minute(override val date: DateTime) extends Bucket {
    override val b_type = "m"

    override def end: DateTime = date.plusMinutes(1)
  }

  private case class Hour(override val date: DateTime) extends Bucket {
    override val b_type = "H"

    override def end: DateTime = date.plusHours(1)
  }

  private case class Day(override val date: DateTime) extends Bucket {
    override val b_type = "D"

    override def end: DateTime = date.plusDays(1)
  }

  private case class Month(override val date: DateTime) extends Bucket {
    override val b_type = "M"

    override def end: DateTime = date.plusMonths(1)
  }

  private case class Year(override val date: DateTime) extends Bucket {
    override val b_type = "Y"

    override def end: DateTime = date.plusYears(1)
  }

}