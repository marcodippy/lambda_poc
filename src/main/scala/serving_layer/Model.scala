package serving_layer

import com.datastax.driver.core.Row
import model.BucketModel.{Bucket, BucketTypes}
import org.joda.time.DateTime

object EventRow {
  def fromRow(row: Row): EventRow = {
    EventRow(
      row.getString("event"),
      Bucket.getByType(BucketTypes.withName(row.getString("bucket")), new DateTime(row.getTimestamp("bdate"))),
      row.getLong("count"))
  }
}

case class EventRow(event: String, bucket: Bucket, count: Long)

object Events {
  val EVENT_TYPES = Seq(
    "LOGIN_WEB", "LOGIN_MOBILE", "BANK_TRANSFER",
    "PAY_BILL", "REQUEST_CREDIT_CARD", "VIEW_STATEMENT",
    "REQUEST_MORTGAGE", "PAY_CREDIT_CARD", "CREATE_ACCOUNT",
    "ENABLE_APPLE_PAY", "REQUEST_SMS", "REQUEST_PAPER_STATEMENT"
  )
}