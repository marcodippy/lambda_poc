package test

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.joda.time.{Duration, Period, DateTime}

import scala.util.Random

object DataProducer extends App {
  val startTime = new DateTime()

  val props = new Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  //props.put("partitioner.class", "com.colobu.kafka.SimplePartitioner")
  props.put("producer.type", "async")
  //props.put("request.required.acks", "1")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)

  val eventTypes = Seq(
    "LOGIN_WEB", "LOGIN_MOBILE", "BANK_TRANSFER",
    "PAY_BILL", "REQUEST_CREDIT_CARD", "VIEW_STATEMENT",
    "REQUEST_MORTGAGE", "PAY_CREDIT_CARD", "CREATE_ACCOUNT",
    "ENABLE_APPLE_PAY", "REQUEST_SMS", "REQUEST_PAPER_STATEMENT"
  )

  def randomElem[A](seq: Seq[A], random: Random): A = seq(random.nextInt(seq.length))

  val steps = Seq(
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

  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] = Iterator.iterate(from)(_.plus(randomElem(steps, new Random()))).takeWhile(!_.isAfter(to))

  val range = dateRange(new DateTime(2013, 12, 25, 0, 0), new DateTime(2016, 2, 3, 0, 0), new Period(0, 1, 0, 0))

  val messages = range.map(d => (randomElem(eventTypes, new Random()), d.toString("yyyy-MM-dd HH:mm:ss")))
    .map(x => s"""{"type":"${x._1}", "timestamp":"${x._2}"}""")

  messages.foreach(msg => {
    producer.send(new KeyedMessage[String, String]("events_topic", msg));
  })

  println(messages.length)

  producer.close();

  val endTime = new DateTime()

  val loadingTime = new Duration(endTime, startTime)
  println(s"Completed in ${loadingTime.getMillis} ms")

  System.exit(0)
}

