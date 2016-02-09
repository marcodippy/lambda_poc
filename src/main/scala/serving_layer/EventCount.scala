package serving_layer

import model.BucketModel.BucketTypes
import org.joda.time.DateTime
import serving_layer.services.{BatchEventServiceComponentImpl, EventServiceComponent, RealTimeEventServiceComponentImpl}
import utils.DateUtils._

import scala.annotation.tailrec

//TODO refactoring!
object EventCount {

  val realTimeEventServiceComponent = RealTimeEventServiceComponentImpl
  val batchEventServiceComponent = BatchEventServiceComponentImpl


  private def getEvents(event: String, range: Range, bl: List[String], eventServiceComponent: EventServiceComponent) = {

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

        val events = eventServiceComponent.eventService.getEventsInRanges(event, BucketTypes.withName(buck), rangeLeft, rangeRight)

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
    val realTimeEvents = getEvents(event, range, BucketTypes.asList, realTimeEventServiceComponent).sortWith(sortByBucketDate)

    val nrange = realTimeEvents match {
      case x :: _ => Range(range.left, x.bucket.start)
      case _ => range
    }

    val batchEvents = getEvents(event, nrange, BucketTypes.asList, batchEventServiceComponent).sortWith(sortByBucketDate)

    batchEvents union realTimeEvents map (_.count) sum
  }

  def getEventCountByRangeAndBucket(event: String, range: Range, bucket: String): Seq[EventRow] = {
    val realTimeEvents = getEvents(event, range, List(bucket), realTimeEventServiceComponent).sortWith(sortByBucketDate)

    val nrange = realTimeEvents match {
      case x :: _ => Range(range.left, x.bucket.start)
      case _ => range
    }

    val batchEvents = getEvents(event, nrange, List(bucket), batchEventServiceComponent).sortWith(sortByBucketDate)
    batchEvents union realTimeEvents
  }

  def getEventsByRange(event: String, range: Range) : Seq[EventRow] = {
    val realTimeEvents = getEvents(event, range, BucketTypes.asList, realTimeEventServiceComponent).sortWith(sortByBucketDate)

    val nrange = realTimeEvents match {
      case x :: _ => Range(range.left, x.bucket.start)
      case _ => range
    }

    val batchEvents = getEvents(event, nrange, BucketTypes.asList, batchEventServiceComponent).sortWith(sortByBucketDate)
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