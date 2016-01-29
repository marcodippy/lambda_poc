package test

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.joda.time.{Duration, Period, DateTime}

import scala.util.Random

object DataProducer {
  val KAFKA_BROKER_LIST = "localhost:9092"

  val EVENT_TYPES = Seq(
    "LOGIN_WEB", "LOGIN_MOBILE", "BANK_TRANSFER",
    "PAY_BILL", "REQUEST_CREDIT_CARD", "VIEW_STATEMENT",
    "REQUEST_MORTGAGE", "PAY_CREDIT_CARD", "CREATE_ACCOUNT",
    "ENABLE_APPLE_PAY", "REQUEST_SMS", "REQUEST_PAPER_STATEMENT"
  )

  val TIME_STEPS = Seq(
    new Period(0, 0, 1, 0),
    new Period(0, 0, 5, 0),
    new Period(0, 0, 10, 0),
    new Period(0, 0, 15, 0),
    new Period(0, 0, 20, 0),
    new Period(0, 0, 30, 0),
    new Period(0, 0, 45, 0),
    new Period(0, 1, 0, 0),
    new Period(0, 1, 15, 0)
  )

  val TEST_DATA : (Seq[String], Seq[Period]) =  (Seq("LOGIN"), Seq(new Period(0, 1, 0, 0)))

  def main(args: Array[String]) {
    val startTime = new DateTime()

    val producer = getProducer(KAFKA_BROKER_LIST)
    val messages = createData(TEST_DATA)

    var msgCount = 0;

    messages.foreach(msg => {
      msgCount += 1
      if (msgCount%10000 == 0){
        println(s"Messages sent -> $msgCount")
      }
      producer.send(new KeyedMessage[String, String]("events_topic", msg));
    })

    producer.close();

    println(s"${msgCount} messages sent in ${(new DateTime().getMillis - startTime.getMillis)} ms")
    System.exit(0)
  }

  def randomElem[A](seq: Seq[A], random: Random): A = seq(random.nextInt(seq.length))

  def dateRange(from: DateTime, to: DateTime, timeSteps: Seq[Period], random: Random): Iterator[DateTime] = {
    Iterator.iterate(from)(_.plus(randomElem(timeSteps, random))).takeWhile(!_.isAfter(to))
  }

  def getProducer(kafkaBrokerList: String): Producer[String, String] = {
    val props = new Properties()
    props.put("metadata.broker.list", kafkaBrokerList)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")
    //props.put("request.required.acks", "1")

    new Producer[String, String](new ProducerConfig(props))
  }

  def createData(testData: (Seq[String], Seq[Period])): Iterator[String] = {
    val random = new Random(12345)

    dateRange(new DateTime(2013, 12, 25, 0, 0), new DateTime(2016, 2, 3, 0, 0), testData._2, random)
      .map(d => (randomElem(testData._1, random), d.toString("yyyy-MM-dd HH:mm:ss")))
      .map(x => s"""{"event":"${x._1}", "timestamp":"${x._2}"}""")
  }
}

