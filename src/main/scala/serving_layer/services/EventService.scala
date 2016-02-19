package serving_layer.services

import com.datastax.driver.core.Cluster
import model.BucketModel.BucketTypes
import serving_layer.EventRow
import serving_layer.repositories.{BatchEventRepositoryComponent, RealTimeEventRepositoryComponent, EventRepositoryComponent}
import utils.DateUtils.Range
import utils.Environment


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
    override def getEventsInRanges(event: String, bucket: BucketTypes.Value, rangeLeft: Range, rangeRight: Range): List[EventRow] =
      rangeLeft match {
        case rl if rl equals rangeRight => eventRepository.getByBucketAndDateRange(event, bucket, rangeLeft)
        case _ => eventRepository.getByBucketAndDateRange(event, bucket, rangeLeft) ::: eventRepository.getByBucketAndDateRange(event, bucket, rangeRight)
      }
  }

}

object RealTimeEventServiceComponentImpl extends
  DefaultEventServiceComponent with RealTimeEventRepositoryComponent {
  val session = Cluster.builder().addContactPoint(Environment.CASSANDRA.HOST).build().connect("lambda_poc")
}

object BatchEventServiceComponentImpl extends
  DefaultEventServiceComponent with BatchEventRepositoryComponent {
  val session = Cluster.builder().addContactPoint(Environment.CASSANDRA.HOST).build().connect("lambda_poc")
}