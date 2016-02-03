package test

import com.datastax.driver.core.Cluster
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra.CassandraSQLContext

object PrepareDatabase extends App {

  def prepareRealTimeDatabase(cassandraHost: String) {
    val cluster = Cluster.builder().addContactPoint(cassandraHost).build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS lambda_poc WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS lambda_poc.events (event text, bucket text, bdate timestamp, count counter, PRIMARY KEY ((event, bucket), bdate) );")
    session.execute("TRUNCATE lambda_poc.events;")
  }

  def prepareBatchDatabase(cassandraHost: String) = {
    val cluster = Cluster.builder().addContactPoint(cassandraHost).build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS lambda_poc WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS lambda_poc.batch_events (event text, bucket text, bdate timestamp, count bigint, PRIMARY KEY ((event, bucket), bdate) );")
    session.execute("TRUNCATE lambda_poc.batch_events;")
    session.close()
  }

  prepareRealTimeDatabase("127.0.0.1")
  prepareBatchDatabase("127.0.0.1")
}
