import java.text.SimpleDateFormat
import java.util.Date

import org.json4s._
import org.json4s.jackson.JsonMethods._

object Event {
  private val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  def convertStringToDate(dateString: String): Date = format.parse(dateString)

  def fromJson(jsonEvent: String): Event = {
    implicit val formats = DefaultFormats
    val parsed = parse(jsonEvent)
    parsed.extract[Event]
  }

}

case class Event(timestamp: String, `type`: String) {
  val buckets = BucketList(Event.convertStringToDate(timestamp))
}