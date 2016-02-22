# lambda_poc
Poc for a lambda architecture using Kafka, Flume, Spark, Cassandra, HDFS

#Prerequisites:
* Java jdk 1.8.0_40
* Scala 2.11
* Sbt 0.13
* Hadoop/HDFS 2.7.1
* Spark 1.6.0 (locally built to make it compatible with scala 2.11)
* Cassandra 3.2.1
* Kafka 0.9.0.0
* Flume 1.6.0

#Flume Config
Create a conf file "lambdapoc_flume-conf.properties" and save it under the `conf` directory in the flume home

    tier1.sources  = source1
    tier1.channels = channel1
    tier1.sinks = sink1
    
    tier1.sources.source1.type = org.apache.flume.source.kafka.KafkaSource
    tier1.sources.source1.zookeeperConnect = localhost:2181
    tier1.sources.source1.topic = events_topic
    tier1.sources.source1.groupId = flume
    tier1.sources.source1.channels = channel1
    tier1.sources.source1.interceptors = i1
    tier1.sources.source1.interceptors.i1.type = timestamp
    tier1.sources.source1.kafka.consumer.timeout.ms = 100
    
    tier1.channels.channel1.type = memory
    tier1.channels.channel1.capacity = 10000
    tier1.channels.channel1.transactionCapacity = 1000
    
    tier1.sinks.sink1.type = hdfs
    tier1.sinks.sink1.hdfs.path = hdfs://localhost:9000/new_data/kafka/%{topic}/%y-%m-%d
    tier1.sinks.sink1.hdfs.rollInterval = 5
    tier1.sinks.sink1.hdfs.rollSize = 0
    tier1.sinks.sink1.hdfs.rollCount = 0
    tier1.sinks.sink1.hdfs.fileType = DataStream
    tier1.sinks.sink1.channel = channel1

#Run the example
Start HDFS:
    
    cd $HADOOP_HOME
    sbin/start-all.sh


Kafka comes with an embedded Zookeeper for testing purpose, you can start it with:

    cd $KAFKA_HOME
    bin/zookeeper-server-start.sh config/zookeeper.properties

Start Kafka broker:

    cd $KAFKA_HOME
    bin/kafka-server-start.sh config/server.properties

Create the topic 
    
    cd $KAFKA_HOME
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic events_topic

Start Flume agent:

    cd $FLUME_HOME
    bin/flume-ng agent -n tier1 -c conf -f conf/lambdapoc_flume-conf.properties

You can send messages to Kafka via shell with:
    
    cd $KAFKA_HOME
    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic events_topic

    {"event":"AAA", "timestamp":"2015-06-10 12:54:43"}
    {"event":"AAA", "timestamp":"2015-06-10 12:54:43"}
    {"event":"AAA", "timestamp":"2015-06-10 14:54:43"} 
    {"event":"ZZZ", "timestamp":"2015-06-25 12:54:43"}
    {"event":"ZZZ", "timestamp":"2015-06-25 12:54:53"}
    ...
    
Start Cassandra:

    cd $CASSANDRA_HOME
    bin/cassandra -f
    
Start the speed layer / Spark Streaming by launching the `speed_layer.StreamingEventCounter` class


Start the batch layer by launching the `batch_layer.BatchPipeline` class
You can also launch the `batch_layer.DataPreProcessing` and `batch_layer.DataPreProcessor` separately if you want


If you want to produce some test messages you can use the class `test.DataProducer`

You can launch the `serving_layer.RestEndpoint` class to launch a webservice that allows you to perform some queries.
Under the `test.frontend` folder you can find some simple html pages that call the RestEndpoint and show the results.