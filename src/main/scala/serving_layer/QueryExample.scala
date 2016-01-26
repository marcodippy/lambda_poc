package serving_layer

import java.sql.Timestamp

import com.datastax.driver.core.{Cluster, Row}
import org.joda.time.DateTime

import scala.collection.JavaConversions._

object QueryExample extends App {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
  val session = cluster.connect("lambda_poc");

  def getEvents(event: String, bucket: String, range: Range): List[EventRow] = {
    def toSqlTimestamp(datetime: DateTime) = new Timestamp(datetime.getMillis)

    println(s" QRY => $bucket - $range")

    val prepared = session.prepare("SELECT * FROM events where event = ? and bucket = ? and bdate >= ? and bdate < ? ORDER BY bdate");
    val results = session.execute(prepared.bind(event, bucket, toSqlTimestamp(range.left), toSqlTimestamp(range.right)))
    val ret = results.iterator().toList.map(EventRow.fromRow(_)).filter(e => ((e.bucket.end.isBefore(range.right)) || (e.bucket.end.isEqual(range.right))))

    println(s"   rows = ${ret.length}")

    ret
  }

  //restituisci la lista delle righe, la somma la fai dopo...

  def get_events(event: String, bucket: String, rangeLeft: Range, rangeRight: Range): List[EventRow] = {

    if (rangeLeft eq rangeRight) {
      getEvents(event, bucket, rangeLeft)
    }
    else {
      val el = if (rangeLeft.isEmpty) List.empty[EventRow]
      else getEvents(event, bucket, rangeLeft)

      val er = if (rangeRight.isEmpty) List.empty[EventRow]
      else getEvents(event, bucket, rangeRight)

      el ::: er
    }
  }


  def gete(event: String, range: Range, bl: List[String]) = {

    def go(event: String, rangeLeft: Range, rangeRight: Range, blist: List[String], results: List[EventRow]): List[EventRow] = {
      println(s"\n**** $rangeLeft   -   $rangeRight ****")

      if ((rangeLeft.right equals rangeLeft.right) && (rangeRight.right equals rangeRight.right)) {
        results
      }

      if (blist.isEmpty) {
        results
      }
      else {
        val buck = blist.head

        val events = get_events(event, buck, rangeLeft, rangeRight)

        if (events.isEmpty) {
          go(event, rangeLeft, rangeRight, blist.tail, results)
        }
        else {
          var l = events.head.bucket.date
          var r = events.last.bucket.end

//          if ((l equals rangeLeft.left) && (r equals rangeRight.right)) {
//            results ::: events
//          }

            if (rangeRight.left equals rangeRight.right) {
              r = rangeRight.left
            }

            if (rangeLeft.left equals rangeLeft.right) {
              l = rangeLeft.left
            }

            println(s"-- $l - $r")
            go(event, Range(rangeLeft.left, l), Range(r, rangeRight.right), blist.tail, results ::: events)
        }
      }
    }

    go(event, range, range, bl, List.empty[EventRow])
  }


  val bucketList = List("Y", "M", "D", "H", "m")

  println(gete("LOGIN_MOBILE", Range(new DateTime(2014, 1, 1, 13, 12, 0), new DateTime(2014, 2, 1, 15, 22, 0)), bucketList).map(_.count).sum)

  System.exit(0)
}


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


trait Bucket {
  val b_type: String

  def nextBB: Bucket

  def prevBB: Bucket

  val date: DateTime

  def end: DateTime

  def toBiggerBucket: Bucket
}

object Bucket {
  def get(btype: String, date: DateTime): Bucket = btype match {
    case "m" => minute(date)
    case "H" => hour(date)
    case "D" => day(date)
    case "M" => month(date)
    case "Y" => year(date)
  }

  def minute(date: DateTime): Bucket = Minute(date.secondOfMinute().roundFloorCopy())

  def hour(date: DateTime): Bucket = Hour(date.hourOfDay().roundFloorCopy())

  def day(date: DateTime): Bucket = Day(date.dayOfMonth().roundFloorCopy())

  def month(date: DateTime): Bucket = Month(date.monthOfYear().roundFloorCopy())

  def year(date: DateTime): Bucket = Year(date.year().roundFloorCopy())

  private case class Minute(override val date: DateTime) extends Bucket {
    override val b_type = "m"

    override def prevBB: Bucket = hour(date.minusHours(1))

    override def nextBB: Bucket = hour(date.plusHours(1))

    override def end: DateTime = date.plusMinutes(1)

    override def toBiggerBucket: Bucket = hour(date)
  }

  private case class Hour(override val date: DateTime) extends Bucket {
    override val b_type = "H"

    override def prevBB: Bucket = day(date.minusDays(1))

    override def nextBB: Bucket = day(date.plusDays(1))

    override def end: DateTime = date.plusHours(1)

    override def toBiggerBucket: Bucket = day(date)
  }

  private case class Day(override val date: DateTime) extends Bucket {
    override val b_type = "D"

    override def prevBB: Bucket = month(date.minusMonths(1))

    override def nextBB: Bucket = month(date.plusMonths(1))

    override def end: DateTime = date.plusDays(1)

    override def toBiggerBucket: Bucket = month(date)
  }

  private case class Month(override val date: DateTime) extends Bucket {
    override val b_type = "M"

    override def prevBB: Bucket = year(date.minusYears(1))

    override def nextBB: Bucket = year(date.plusYears(1))

    override def end: DateTime = date.plusMonths(1)

    override def toBiggerBucket: Bucket = year(date)
  }

  private case class Year(override val date: DateTime) extends Bucket {
    override val b_type = "Y"

    override def prevBB: Bucket = year(date.minusYears(1))

    override def nextBB: Bucket = year(date.plusYears(1))

    override def end: DateTime = date.plusYears(1)

    override def toBiggerBucket: Bucket = year(date) //mmm
  }

}