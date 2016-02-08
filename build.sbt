
lazy val commonSettings = Seq(
  organization := "org.mdipaola",
  version := "0.1.0",
  scalaVersion := "2.11.7"
)

val avro_version = "1.8.0"
val cassandra_driver_core_version = "3.0.0-rc1"
val spark_version = "1.6.0"
val spark_avro_version = "2.0.1"
val spark_cassandra_connector_version = "1.5.0-RC1"
val spray_verison = "1.3.1"
val slf4j_version = "1.7.14"

val avroExclusion = ExclusionRule(organization = "org.apache.avro")
val jlineExclusion = ExclusionRule("jline", "jline")
val scalaExclusion = ExclusionRule(organization = "org.scala-lang")
val slf4jExclusion = ExclusionRule("org.slf4j", "slf4j-api")
val sparkExclusion = ExclusionRule(organization = "org.apache.spark")

lazy val provided_dependencies = Seq(
  "org.apache.spark" %% "spark-streaming" % spark_version excludeAll (scalaExclusion),
  "org.apache.spark" %% "spark-sql" % spark_version excludeAll (scalaExclusion),
  "org.apache.spark" %% "spark-core" % spark_version excludeAll (scalaExclusion)
)

val dependencies = Seq(
  "org.apache.spark" %% "spark-streaming-kafka" % spark_version,

  "com.datastax.spark" %% "spark-cassandra-connector" % spark_cassandra_connector_version excludeAll (sparkExclusion),
  "com.databricks" %% "spark-avro" % spark_avro_version,

  "org.apache.avro" % "avro" % avro_version,
  "io.spray" %% "spray-can" % spray_verison,
  "io.spray" %% "spray-routing" % spray_verison,
  "io.spray" %% "spray-json" % spray_verison
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(name := "lambda_poc").
  settings(
    libraryDependencies ++= (provided_dependencies.map(_ % "provided") ++ dependencies)
  )

lazy val mainRunner = (project in file("mainRunner")).
  dependsOn(RootProject(file("."))).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= (provided_dependencies.map(_ % "compile"))
  )


/****************************************************************************/
/**************************** ASSEMBLY PLUGIN *******************************/
/****************************************************************************/

assemblyJarName in assembly := "lambda_poc-uber.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => {
    (xs map { _.toLowerCase }) match {
            case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
            case _ => MergeStrategy.discard
    }
  }
  case _ => MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)