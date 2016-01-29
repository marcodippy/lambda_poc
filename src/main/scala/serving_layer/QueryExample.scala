package serving_layer

import java.sql.Timestamp

import com.datastax.driver.core.{Cluster, Row}
import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.collection.JavaConversions._

object QueryExample extends App {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
  val session = cluster.connect("lambda_poc");

  def getEventsFromDb(event: String, bucket: String, range: Range): List[EventRow] = {
    println(s" QRY => $bucket - $range")

    val prepared = session.prepare("SELECT * FROM events where event = ? and bucket = ? and bdate >= ? and bdate < ? ORDER BY bdate"); //move
    val results = session.execute(prepared.bind(event, bucket, new Timestamp(range.left.getMillis), new Timestamp(range.right.getMillis)))
    val ret = results.iterator().toList.map(EventRow.fromRow(_)).filter(e => ((e.bucket.end.isBefore(range.right)) || (e.bucket.end.isEqual(range.right))))

    println(s"   rows = ${ret.length}")
    ret
  }

  def getEventsInRanges(event: String, bucket: String, rangeLeft: Range, rangeRight: Range): List[EventRow] = {
    if (rangeLeft equals rangeRight) {
      getEventsFromDb(event, bucket, rangeLeft)
    }
    else {
      val el = if (rangeLeft.isEmpty) List.empty[EventRow]
      else getEventsFromDb(event, bucket, rangeLeft)

      val er = if (rangeRight.isEmpty) List.empty[EventRow]
      else getEventsFromDb(event, bucket, rangeRight)

      el ::: er
    }
  }

  def getEvents(event: String, range: Range, bl: List[String]) = {

    @tailrec
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

        val events = getEventsInRanges(event, buck, rangeLeft, rangeRight)

        if (events.isEmpty) {
          go(event, rangeLeft, rangeRight, blist.tail, results)
        }
        else {
          var l = events.head.bucket.date
          var r = events.last.bucket.end

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


  println(getEvents("LOGIN", Range(new DateTime(2015, 12, 25, 15, 0, 0), new DateTime(2015, 12, 25, 18, 30, 0)), Bucket.bucketList).map(_.count).sum)

  System.exit(0)
}
