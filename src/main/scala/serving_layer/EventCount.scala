package serving_layer

import java.sql.Timestamp

import com.datastax.driver.core.Cluster
import model.BucketModel.BucketTypes
import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import utils.DateUtils._

//TODO refactoring!
object EventCount {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
  val session = cluster.connect("lambda_poc");
  val RT_EVENTS_PREPARED_STATEMENT = session.prepare("SELECT * FROM events where event = ? and bucket = ? and bdate >= ? and bdate < ? ORDER BY bdate");
  val BATCH_EVENTS_PREPARED_STATEMENT = session.prepare("SELECT * FROM batch_events where event = ? and bucket = ? and bdate >= ? and bdate < ? ORDER BY bdate");

  private def getEventsFromDb(event: String, bucket: String, range: Range, table: String): List[EventRow] = {
    val prepared = table match {
      case "batch" => BATCH_EVENTS_PREPARED_STATEMENT
      case "realtime" => RT_EVENTS_PREPARED_STATEMENT
    }

    val results = session.execute(prepared.bind(event, bucket, new Timestamp(range.left.getMillis), new Timestamp(range.right.getMillis)))
    val ret = results.iterator().toList.map(EventRow.fromRow(_)).filter(e => ((e.bucket.end.isBefore(range.right)) || (e.bucket.end.isEqual(range.right))))

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

    @tailrec
    def go(event: String, rangeLeft: Range, rangeRight: Range, blist: List[String], results: List[EventRow]): List[EventRow] = {
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
          var l = events.head.bucket.start
          var r = events.last.bucket.end

          if (rangeRight.left equals rangeRight.right) {
            r = rangeRight.left
          }

          if (rangeLeft.left equals rangeLeft.right) {
            l = rangeLeft.left
          }

          go(event, Range(rangeLeft.left, l), Range(r, rangeRight.right), blist.tail, results ::: events)
        }
      }
    }

    go(event, range, range, bl, List.empty[EventRow])
  }

  private def sortByBucketDate(e1: EventRow, e2: EventRow): Boolean = e1.bucket.start.isBefore(e2.bucket.start)

  def countEventsByRange(event: String, range: Range): Long = {
    val realTimeEvents = getEvents(event, range, BucketTypes.asList, "realtime").sortWith(sortByBucketDate)

    val nrange = realTimeEvents match {
      case x :: _ => Range(range.left, x.bucket.start)
      case _ => range
    }

    val batchEvents = getEvents(event, nrange, BucketTypes.asList, "batch").sortWith(sortByBucketDate)

    batchEvents union realTimeEvents map (_.count) sum
  }

  def getEventCountByRangeAndBucket(event: String, range: Range, bucket: String): Seq[EventRow] = {
    val realTimeEvents = getEvents(event, range, List(bucket), "realtime").sortWith(sortByBucketDate)

    val nrange = realTimeEvents match {
      case x :: _ => Range(range.left, x.bucket.start)
      case _ => range
    }

    val batchEvents = getEvents(event, nrange, List(bucket), "batch").sortWith(sortByBucketDate)
    batchEvents union realTimeEvents
  }

  def getEventsByRange(event: String, range: Range) : Seq[EventRow] = {
    val realTimeEvents = getEvents(event, range, BucketTypes.asList, "realtime").sortWith(sortByBucketDate)

    val nrange = realTimeEvents match {
      case x :: _ => Range(range.left, x.bucket.start)
      case _ => range
    }

    val batchEvents = getEvents(event, nrange, BucketTypes.asList, "batch").sortWith(sortByBucketDate)
    batchEvents union realTimeEvents
  }


  def getTotalEventsCount(range: Range): Seq[EventRow] = {
    Events.EVENT_TYPES.flatMap(eventType => {
      getEventsByRange(eventType, range)
    })
  }

  def main(args: Array[String]) {
    println(countEventsByRange("LOGIN_MOBILE", Range(new DateTime(2015, 12, 25, 15, 0, 0), new DateTime(2015, 12, 25, 18, 30, 0))))
    System.exit(0)
  }

}