package serving_layer.repositories

import java.sql.Timestamp

import com.datastax.driver.core.Session
import model.BucketModel.BucketTypes
import serving_layer.EventRow
import utils.DateUtils.Range

import scala.collection.JavaConversions._

trait EventRepositoryComponent {
  def eventRepository: EventRepository

  trait EventRepository {
    def getByBucketAndDateRange(event: String, bucket: BucketTypes.Value, range: Range): List[EventRow]
  }

}

trait RealTimeEventRepositoryComponent extends EventRepositoryComponent {
  val session: Session

  override def eventRepository = new RealTimeEventRepository(session)

  class RealTimeEventRepository(val session: Session) extends EventRepository {
    val RT_EVENTS_PREPARED_STATEMENT = session.prepare("SELECT * FROM events where event = ? and bucket = ? and bdate >= ? and bdate < ? ORDER BY bdate");

    override def getByBucketAndDateRange(event: String, bucket: BucketTypes.Value, range: Range): List[EventRow] = {
      if (range.isEmpty) List.empty[EventRow]
      else {
        val results = session.execute(RT_EVENTS_PREPARED_STATEMENT.bind(event, bucket.toString, new Timestamp(range.left.getMillis), new Timestamp(range.right.getMillis)))
        results.iterator().toList.map(EventRow.fromRow(_)).filter(e => ((e.bucket.end.isBefore(range.right)) || (e.bucket.end.isEqual(range.right))))
      }
    }
  }

}

trait BatchEventRepositoryComponent extends EventRepositoryComponent {
  val session: Session

  override def eventRepository = new BatchEventRepository(session)

  class BatchEventRepository(val session: Session) extends EventRepository {
    val BATCH_EVENTS_PREPARED_STATEMENT = session.prepare("SELECT * FROM batch_events where event = ? and bucket = ? and bdate >= ? and bdate < ? ORDER BY bdate");

    override def getByBucketAndDateRange(event: String, bucket: BucketTypes.Value, range: Range): List[EventRow] = {
      if (range.isEmpty) List.empty[EventRow]
      else {
        val results = session.execute(BATCH_EVENTS_PREPARED_STATEMENT.bind(event, bucket.toString, new Timestamp(range.left.getMillis), new Timestamp(range.right.getMillis)))
        results.iterator().toList.map(EventRow.fromRow(_)).filter(e => ((e.bucket.end.isBefore(range.right)) || (e.bucket.end.isEqual(range.right))))
      }
    }
  }

}