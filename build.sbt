name := "lambda poc"
version := "1.0"
scalaVersion := "2.11.7"
val sprayV = "1.3.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "1.5.2",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.5.2",
  "org.apache.spark" %% "spark-core" % "1.5.2" % "provided" excludeAll(
                                                                    ExclusionRule(organization = "org.scala-lang"),
                                                                    ExclusionRule("jline", "jline"),
                                                                    ExclusionRule("org.slf4j", "slf4j-api"),
                                                                    ExclusionRule("org.apache.commons", "commons-lang3")
                                                                    ),
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-RC1",
  "com.databricks" %% "spark-avro" % "2.0.1",
  "io.spray"            %%  "spray-can"     % sprayV,
  "io.spray"            %%  "spray-routing" % sprayV,
  "io.spray"            %%  "spray-json" % sprayV,
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0-rc1"
)