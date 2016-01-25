import com.databricks.spark.avro._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object DataPartitioner extends App {
  Logger.getRootLogger.setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("DataPartitioner").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  val hc = new Configuration();
  hc.set("fs.default.name", "hdfs://localhost:9000");
  val fs = FileSystem.get(hc);

  val newdata_dir = new org.apache.hadoop.fs.Path("/new_data/kafka/events_topic")
  val exists = fs.exists(newdata_dir)
  println(exists)

  val files = fs.listFiles(newdata_dir, true)

  while (files.hasNext) {
    val next = files.next()

    if (next.isFile) {
      val filePath = next.getPath
      val lines = sc.textFile(filePath.toString)

      val events = lines.map(line => EventRow.fromEvent(Event.fromJson(line)))

      val df = events.toDF()

      df.show()
      df.write.mode(SaveMode.Append).partitionBy("type", "year", "month", "day", "hour").avro("/tmp/output")
      //      fs.delete(filePath, false)
    }
  }

  sc.stop()
}

object EventRow {
  def fromEvent(event: Event): EventRow = {
    EventRow(
      event.year.toString,
      event.month.toString,
      event.day.toString,
      event.hour.toString,
      event.minute.toString,
      event.`type`,
      event.timestamp)
  }
}

case class EventRow(year: String, month: String, day: String, hour: String, minute: String, `type`: String, timestamp: String)

