package serving_layer

import model.BucketModel.BucketTypes
import org.joda.time.DateTime
import serving_layer.services.{BatchEventServiceComponentImpl, EventServiceComponent, RealTimeEventServiceComponentImpl}
import utils.DateUtils._

import scala.annotation.tailrec

object EventCount {

  //TODO inject
  val realTimeEventServiceComponent = RealTimeEventServiceComponentImpl
  val batchEventServiceComponent = BatchEventServiceComponentImpl

  private def getEvents(event: String, range: Range, bl: List[String], eventServiceComponent: EventServiceComponent) = {

    @tailrec
    def go(event: String, rangeLeft: Range, rangeRight: Range, blist: List[String], results: List[EventRow]): List[EventRow] = {
      if ((rangeLeft.isEmpty && rangeRight.isEmpty) || blist.isEmpty) {
        results
      }
      else {
        val bucket = BucketTypes.withName(blist.head)

        eventServiceComponent.eventService.getEventsInRanges(event, bucket, rangeLeft, rangeRight) match {
          case Nil => go(event, rangeLeft, rangeRight, blist.tail, results)
          case events => {
            val newRangeLeft = rangeLeft.left match {
              case rangeLeft.right => Range(rangeLeft.left, rangeLeft.left)
              case _ => Range(rangeLeft.left, events.head.bucket.start)
            }

            val newRangeRight = rangeRight.left match {
              case rangeRight.right => Range(rangeRight.right, rangeRight.right)
              case _ => Range(events.last.bucket.end, rangeRight.right)
            }

            go(event, newRangeLeft, newRangeRight, blist.tail, results ::: events)
          }
        }

      }
    }

    go(event, range, range, bl, List.empty[EventRow])
  }

  private def sortByBucketDate(e1: EventRow, e2: EventRow): Boolean = e1.bucket.start.isBefore(e2.bucket.start)

  private def getEventCountByRangeAndBucket(event: String, range: Range, buckets: List[String]): Seq[EventRow] = {
    val realTimeEvents = getEvents(event, range, buckets, realTimeEventServiceComponent).sortWith(sortByBucketDate)

    val nrange = realTimeEvents match {
      case x :: _ => Range(range.left, x.bucket.start)
      case _ => range
    }

    val batchEvents = getEvents(event, nrange, buckets, batchEventServiceComponent).sortWith(sortByBucketDate)
    batchEvents union realTimeEvents
  }

  def getEventCountByRangeAndBucket(event: String, range: Range, bucket: String): Seq[EventRow] = {
    getEventCountByRangeAndBucket(event, range, List(bucket))
  }

  def getEventsByRange(event: String, range: Range): Seq[EventRow] = {
    getEventCountByRangeAndBucket(event, range, BucketTypes.asList)
  }

  def countEventsByRange(event: String, range: Range): Long = {
    getEventCountByRangeAndBucket(event, range, BucketTypes.asList) map (_.count) sum
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