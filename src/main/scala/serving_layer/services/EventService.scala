package serving_layer.services

import com.datastax.driver.core.Cluster
import model.BucketModel.BucketTypes
import serving_layer.EventRow
import serving_layer.repositories.{BatchEventRepositoryComponent, RealTimeEventRepositoryComponent, EventRepositoryComponent}
import utils.DateUtils.Range


trait EventServiceComponent {
  def eventService: EventService

  trait EventService {
    def getEventsInRanges(event: String, bucket: BucketTypes.Value, rangeLeft: Range, rangeRight: Range): List[EventRow]
  }

}

trait DefaultEventServiceComponent extends EventServiceComponent {
  this: EventRepositoryComponent =>

  def eventService = new DefaultEventService

  class DefaultEventService extends EventService {
    override def getEventsInRanges(event: String, bucket: BucketTypes.Value, rangeLeft: Range, rangeRight: Range): List[EventRow] = {
      if (rangeLeft equals rangeRight) {
        eventRepository.getByBucketAndDateRange(event, bucket, rangeLeft)
      }
      else {

        val el = if (rangeLeft.isEmpty) List.empty[EventRow]
        else eventRepository.getByBucketAndDateRange(event, bucket, rangeLeft)

        val er = if (rangeRight.isEmpty) List.empty[EventRow]
        else eventRepository.getByBucketAndDateRange(event, bucket, rangeRight)

        el ::: er
      }
    }
  }

}



object RealTimeEventServiceComponentImpl extends
  DefaultEventServiceComponent with RealTimeEventRepositoryComponent {
  val session = Cluster.builder().addContactPoint("127.0.0.1").build().connect("lambda_poc")
}

object BatchEventServiceComponentImpl extends
  DefaultEventServiceComponent with BatchEventRepositoryComponent {
  val session = Cluster.builder().addContactPoint("127.0.0.1").build().connect("lambda_poc")
}

object ApplicationLive {
  val eventService = RealTimeEventServiceComponentImpl.eventService
}
