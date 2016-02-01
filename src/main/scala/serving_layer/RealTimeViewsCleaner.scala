package serving_layer

import java.sql.Timestamp

import com.datastax.driver.core.Cluster
import org.joda.time.DateTime

object RealTimeViewsCleaner {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
  val session = cluster.connect("lambda_poc");
  val DELETE_PREPARED_STATEMENT = session.prepare("DELETE FROM events WHERE event = ? and bucket = ? and bdate < ?");

  def expireData(time: DateTime) = {
    for (
      event <- model.Event.EVENT_TYPES;
      bucket <- model.BucketList.bucketSymbols
    ) {
      session.execute(DELETE_PREPARED_STATEMENT.bind(event, bucket, new Timestamp(time.getMillis)))
    }
  }
}
