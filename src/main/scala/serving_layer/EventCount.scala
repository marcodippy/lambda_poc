package serving_layer

import java.sql.Timestamp

import com.datastax.driver.core.Cluster
import org.joda.time.DateTime
import scala.collection.JavaConversions._

//TODO refactoring!
object EventCount {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
  val session = cluster.connect("lambda_poc");
  val RT_EVENTS_PREPARED_STATEMENT = session.prepare("SELECT * FROM events where event = ? and bucket = ? and bdate >= ? and bdate < ? ORDER BY bdate");
  val BATCH_EVENTS_PREPARED_STATEMENT = session.prepare("SELECT * FROM batch_events where event = ? and bucket = ? and bdate >= ? and bdate < ? ORDER BY bdate");

  private def getEventsFromDb(event: String, bucket: String, range: Range, table: String): List[EventRow] = {
    println(s" QRY => $bucket - $range")

    val prepared = table match {
      case "batch" => BATCH_EVENTS_PREPARED_STATEMENT
      case "realtime" => RT_EVENTS_PREPARED_STATEMENT
    }

    val results = session.execute(prepared.bind(event, bucket, new Timestamp(range.left.getMillis), new Timestamp(range.right.getMillis)))
    val ret = results.iterator().toList.map(EventRow.fromRow(_)).filter(e => ((e.bucket.end.isBefore(range.right)) || (e.bucket.end.isEqual(range.right))))

    println(s"   rows = ${ret.length}")
    ret
  }

  private def getEventsInRanges(event: String, bucket: String, rangeLeft: Range, rangeRight: Range, table: String): List[EventRow] = {
    if (rangeLeft equals rangeRight) {
      getEventsFromDb(event, bucket, rangeLeft, table)
    }
    else {
      val el = if (rangeLeft.isEmpty) List.empty[EventRow]
      else getEventsFromDb(event, bucket, rangeLeft, table)

      val er = if (rangeRight.isEmpty) List.empty[EventRow]
      else getEventsFromDb(event, bucket, rangeRight, table)

      el ::: er
    }
  }

  private def getEvents(event: String, range: Range, bl: List[String], table: String) = {

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

        val events = getEventsInRanges(event, buck, rangeLeft, rangeRight, table)

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

  private def sortByBucketDate(e1: EventRow, e2: EventRow): Boolean = e1.bucket.date.isBefore(e2.bucket.date)

  def countEventsByRange(event: String, range: Range): Long = {
    println("RETRIEVING REALTIME DATA")
    val realTimeEvents = getEvents(event, range, Bucket.bucketList, "realtime").sortWith(sortByBucketDate)
    println(s"****** realtime count ${realTimeEvents.map(_.count).sum}")

    val nrange = realTimeEvents match {
      case x :: _ => Range(range.left, x.bucket.date)
      case _ => range
    }

    println("RETRIEVING BATCH DATA\n\n")

    val batchEvents = getEvents(event, nrange, Bucket.bucketList, "batch").sortWith(sortByBucketDate)
    println(s"****** batch count ${realTimeEvents.map(_.count).sum}")

    batchEvents union realTimeEvents map (_.count) sum
  }

  def getEventCountByRangeAndBucket(event: String, range: Range, bucket: String) : Seq[EventRow]= {
    val realTimeEvents = getEvents(event, range, List(bucket), "realtime").sortWith(sortByBucketDate)

    val nrange = realTimeEvents match {
      case x :: _ => Range(range.left, x.bucket.date)
      case _ => range
    }

    val batchEvents = getEvents(event, nrange, List(bucket), "batch").sortWith(sortByBucketDate)
    batchEvents union realTimeEvents
  }


  def main(args: Array[String]) {
    println(countEventsByRange("LOGIN_MOBILE", Range(new DateTime(2015, 12, 25, 15, 0, 0), new DateTime(2015, 12, 25, 18, 30, 0))))
    System.exit(0)
  }

}
