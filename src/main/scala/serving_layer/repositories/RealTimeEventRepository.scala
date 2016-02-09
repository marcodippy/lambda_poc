package serving_layer.repositories

import com.datastax.driver.core.Session
import model.BucketModel.BucketTypes
import serving_layer.EventRow
import utils.DateUtils.Range

class RealTimeEventRepository(val session: Session) extends EventRepository {
  override def getByBucketAndDateRange(event: String, bucket: BucketTypes.Value, range: Range): Seq[EventRow] = ???
}

class BatchEventRepository(val session: Session) extends EventRepository {
  override def getByBucketAndDateRange(event: String, bucket: BucketTypes.Value, range: Range): Seq[EventRow] = ???
}
