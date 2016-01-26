package serving_layer

import akka.actor.ActorSystem
import com.datastax.driver.core.{Row, ResultSet, Cluster}
import spray.routing.SimpleRoutingApp


object RestEndpoint extends App with SimpleRoutingApp {
  implicit val system = ActorSystem("spray-actor")

  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();


  startServer(interface = "localhost", port = 8080) {
    path("lambda" / "form") {
      get {
        complete {
          <html>
            <h1>Say hello to spray</h1>
            <form action="./query" method="GET">
              <input type="datetime-local" name="from" />
              <input type="datetime-local" name="to" />
              <input type="submit" value="pd" />
            </form>
          </html>
        }
      }
    } ~
      path("lambda" / "query") {
        get {
          parameters('from, 'to){ (from, to) =>
            complete {


              println(from)
              println(to)

              val session = cluster.connect("lambda_poc");
              val results = session.execute("SELECT * FROM batch_events");
              val iter = results.iterator();

              while (iter.hasNext) {
                val row = iter.next();
                println(row)
              }

              <h1>Say hello to asdasdadsas</h1>
            }
          }


        }
      }


  }
}

