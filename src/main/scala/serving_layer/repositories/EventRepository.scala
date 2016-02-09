package serving_layer.repositories

import java.sql.Timestamp

import com.datastax.driver.core.{Cluster, Session}
import model.BucketModel.BucketTypes
import serving_layer.EventRow
import utils.DateUtils.Range
import scala.collection.JavaConversions._

trait EventRepository {
  def getByBucketAndDateRange(event: String, bucket: BucketTypes.Value, range: Range): Seq[EventRow]
}

trait EventRepositoryComponent {
  def realTimeEventRepo: EventRepository

  def batchEventRepo: EventRepository
}

trait CassandraEventRepositoryComponent extends EventRepositoryComponent {
  val session: Session

  def realTimeEventRepo = new RealTimeEventRepository(session)

  def batchEventRepo = new BatchEventRepository(session)

  class RealTimeEventRepository(val session: Session) extends EventRepository {
    val RT_EVENTS_PREPARED_STATEMENT = session.prepare("SELECT * FROM events where event = ? and bucket = ? and bdate >= ? and bdate < ? ORDER BY bdate");

    override def getByBucketAndDateRange(event: String, bucket: BucketTypes.Value, range: Range): Seq[EventRow] = {
      val results = session.execute(RT_EVENTS_PREPARED_STATEMENT.bind(event, bucket, new Timestamp(range.left.getMillis), new Timestamp(range.right.getMillis)))
      results.iterator().toList.map(EventRow.fromRow(_)).filter(e => ((e.bucket.end.isBefore(range.right)) || (e.bucket.end.isEqual(range.right))))
    }
  }

  class BatchEventRepository(val session: Session) extends EventRepository {
    val BATCH_EVENTS_PREPARED_STATEMENT = session.prepare("SELECT * FROM batch_events where event = ? and bucket = ? and bdate >= ? and bdate < ? ORDER BY bdate");

    override def getByBucketAndDateRange(event: String, bucket: BucketTypes.Value, range: Range): Seq[EventRow] = {
      val results = session.execute(BATCH_EVENTS_PREPARED_STATEMENT.bind(event, bucket, new Timestamp(range.left.getMillis), new Timestamp(range.right.getMillis)))
      results.iterator().toList.map(EventRow.fromRow(_)).filter(e => ((e.bucket.end.isBefore(range.right)) || (e.bucket.end.isEqual(range.right))))
    }
  }

}

trait EventServiceComponent {
  def eventService: EventService

  trait EventService {
    def getByBucketAndDateRange(event: String, bucket: BucketTypes.Value, range: Range): Seq[EventRow]
  }
}

trait DefaultEventServiceComponent extends EventServiceComponent {
  this: EventRepositoryComponent =>

  def eventService = new DefaultEventService

  class DefaultEventService extends EventService {
    //    def findAll = realTimeEventRepo.getByBucketAndDateRange(event: String, bucket: BucketTypes.Value, range: Range)

    //    def save(user: User) {
    //      userUpdater.save(user: User) qui usi il repository iniettato
    //    }
    override def getByBucketAndDateRange(event: String, bucket: BucketTypes.Value, range: Range): Seq[EventRow] = ???
  }

}

object EventServiceComponentImpl extends
  DefaultEventServiceComponent with CassandraEventRepositoryComponent {
  val session = Cluster.builder().addContactPoint("127.0.0.1").build().connect("lambda_poc")
}

object ApplicationLive {
  val eventService = EventServiceComponentImpl.eventService
}