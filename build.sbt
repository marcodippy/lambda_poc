lazy val commonSettings = Seq(
  organization := "org.mdipaola",
  version := "0.1.0",
  scalaVersion := "2.11.7"
)

val spray_verison = "1.3.1"
val spark_version = "1.6.0"
val spark_avro_version = "2.0.1"
val spark_cassandra_connector_version = "1.5.0-RC1"
val cassandra_driver_core_version = "3.0.0-rc1"

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "lambda_poc"
  ).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % spark_version,
      "org.apache.spark" %% "spark-streaming-kafka" % spark_version,
      "org.apache.spark" %% "spark-sql" % spark_version,
      "org.apache.spark" %% "spark-core" % spark_version % "provided" excludeAll(
        ExclusionRule(organization = "org.scala-lang"),
        ExclusionRule("jline", "jline"),
        ExclusionRule("org.slf4j", "slf4j-api"),
        ExclusionRule("org.apache.commons", "commons-lang3")
        ),
      "com.datastax.spark" %% "spark-cassandra-connector" % spark_cassandra_connector_version,
      "com.databricks" %% "spark-avro" % "2.0.1",
      "io.spray" %% "spray-can" % spray_verison,
      "io.spray" %% "spray-routing" % spray_verison,
      "io.spray" %% "spray-json" % spray_verison
    )
  )