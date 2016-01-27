package serving_layer

import java.sql.Timestamp

import com.datastax.driver.core.Cluster
import org.joda.time.DateTime

import scala.collection.JavaConversions._

object Queries {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
  val session = cluster.connect("lambda_poc");

  def getEventsFromDb(event: String, bucket: String, range: Range): List[EventRow] = {
    println(s" QRY => $bucket - $range")

    val prepared = session.prepare("SELECT * FROM events where event = ? and bucket = ? and bdate >= ? and bdate < ? ORDER BY bdate");
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

  def retrieveEvents(event: String, range: Range): List[EventRow] = {
    //    val rt_events = getEvents(event, range)(getEventsFromRealTimeDb)
    //    val batch_events = getEvents(event, range)(getEventsFromBatchDb)
    //
    //    rt_events match {
    //      case x => batch_events.filter(e => (e.bucket.end.isBefore(x.head.bucket.date) || (e.bucket.end.isEqual(x.head.bucket.date)))) ::: x
    //      case _ => batch_events
    //    }

    getEvents(event, Range(new DateTime(2014, 1, 1, 13, 12, 0), new DateTime(2015, 1, 1, 0, 0, 0)), Bucket.bucketList)
  }

  def retrieveEvents2(event: String, range: Range): List[EventRow] = {
    //    val rt_events = getEvents(event, range)(getEventsFromRealTimeDb)
    //    val batch_events = getEvents(event, range)(getEventsFromBatchDb)
    //
    //    rt_events match {
    //      case x => batch_events.filter(e => (e.bucket.end.isBefore(x.head.bucket.date) || (e.bucket.end.isEqual(x.head.bucket.date)))) ::: x
    //      case _ => batch_events
    //    }

    getEvents(event, Range(new DateTime(2014, 1, 1, 13, 12, 0), new DateTime(2015, 1, 1, 0, 0, 0)), List("M"))
  }
}
