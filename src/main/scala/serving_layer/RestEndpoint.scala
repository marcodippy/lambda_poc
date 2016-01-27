package serving_layer

import akka.actor.ActorSystem
import com.datastax.driver.core.Cluster
import org.joda.time.DateTime
import spray.http.HttpHeaders.RawHeader
import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import spray.routing.SimpleRoutingApp

object MJsonImplicits extends DefaultJsonProtocol {
  implicit val impPerson = jsonFormat2(WEvent)

  case class WEvent(  date: String, count: Long)
}


object RestEndpoint extends App with SimpleRoutingApp with SprayJsonSupport {
  implicit val system = ActorSystem("spray-actor")

  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

  startServer(interface = "localhost", port = 9999) {
    import MJsonImplicits._

    respondWithHeader(RawHeader("Access-Control-Allow-Origin", "*")){
      path("lambda" / "form") {
        get {
          complete {
            <html>
              <h1>Say hello to spray</h1>
              <form action="./query" method="GET">
                <input type="datetime-local" name="from"/>
                <input type="datetime-local" name="to"/>
                <input type="submit" value="pd"/>
              </form>
            </html>
          }
        }
      } ~
        path("lambda" / "query") {
          get {
            parameters('from, 'to) { (from, to) =>
              complete {
                val results = Queries.retrieveEvents2("LOGIN_MOBILE", new Range(new DateTime(from), new DateTime(to)))
                results.map(e => WEvent(  e.bucket.date.toString("yyyy-MM-dd"), e.count))
              }
            }
          }
        }

    }


  }
}

