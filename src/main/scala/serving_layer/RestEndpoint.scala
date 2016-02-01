package serving_layer

import akka.actor.ActorSystem
import com.datastax.driver.core.Cluster
import org.joda.time.DateTime
import spray.http.HttpHeaders.RawHeader
import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import spray.routing.SimpleRoutingApp

object MJsonImplicits extends DefaultJsonProtocol {
  implicit val impEvent = jsonFormat2(WEvent)
  case class WEvent(date: String, count: Long)
}

object RestEndpoint extends App with SimpleRoutingApp with SprayJsonSupport {
  implicit val system = ActorSystem("spray-actor")

  startServer(interface = "localhost", port = 9999) {
    import MJsonImplicits._

    respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")) {
      path("lambda" / "queries") {
        get {
          complete {
            <html>

              <h2>Count events in a period</h2>
              <form action="./countbyperiod" method="GET">
                <input type="text" name="event"/>
                <input type="datetime-local" name="from"/>
                <input type="datetime-local" name="to"/>
                <input type="submit" value="go"/>
              </form>

              <br/>
              <br/>

              <h2>Get timeseries</h2>
              <form action="./timeseries" method="GET">
                <input type="text" name="event"/>
                <select name="bucket">
                  <option value="m">Minute</option>
                  <option value="H">Hour</option>
                  <option value="D">Day</option>
                  <option value="M">Month</option>
                  <option value="Y">Year</option>
                </select>
                <input type="datetime-local" name="from"/>
                <input type="datetime-local" name="to"/>
                <input type="submit" value="go"/>
              </form>

            </html>
          }
        }
      } ~
        path("lambda" / "countbyperiod") {
          get {
            parameters('from, 'to, 'event) { (from, to, event) =>
              complete {
                val result = EventCount.countEventsByRange(event, new Range(new DateTime(from), new DateTime(to)))
                (event, from, to, result)
              }
            }
          }
        } ~
        path("lambda" / "timeseries") {
          get {
            parameters('from, 'to, 'event, 'bucket) { (from, to, event, bucket) =>
              complete {
                val results = EventCount.getEventCountByRangeAndBucket(event, new Range(new DateTime(from), new DateTime(to)), bucket)
                results.map(e => WEvent(  e.bucket.date.toString("yyyy-MM-dd HH:mm"), e.count))
              }
            }
          }
        }
    }


  }
}

