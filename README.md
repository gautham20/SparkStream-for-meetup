# SparkStream-for-meetup
Spark Streaming of RSVPs from meetup.com API using Kafka
[meetup.com](https://www.meetup.com/) provides a streaming data of RSVPs in JSON Format. The stream is accesible through, 
[http://stream.meetup.com/2/rsvps](http://stream.meetup.com/2/rsvps)

Getting this streaming data into Apache Spark-Streaming is the first step to perform various analytics, recommendations or visualizations on the data.

## Technologies Used

* Kafka 0.9.0.1
* Spark 2.0.1

[Kafka Python API](https://github.com/dpkp/kafka-python) is used to interact with kafka cluster. PySpark is used to write the spark streaming jobs.

## Execute the Application

Assuming Kafka and Spark of appropriate version is installed, the following commands are used to run the application.

> Spark Streaming integeration with kafka 0.10.0.0 and above, is still in experimental status, Hence using Kafka 0.9 (http://spark.apache.org/docs/latest/streaming-kafka-integration.html)

1. Run Zookeeper to maintain Kafka, command to be run from Kafka root dir
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

2. Start Kafka server, aditional servers can be added as per requirement.
```
bin/kafka-server-start.sh config/server.properties
```

3. Start Producer.py to start reading data from the meetup stream and store it in '''meetup''' kafka topic.

4. Start Consumer.py to consume the stream from the '''meetup''' topic

5. Submit the spark job spark_meetup.py, to read the data into Spark Streaming from Kafka.
> Spark depends on a external package for kafka integeration [link](https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-8_2.11/2.0.1)
```
bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1 spark_meetup.py localhost:2181 meetup
```

An analysis of number of RSVPs from various cities in "US" region is performed on the RSVPs Stream.

//TODO
* Visualize the data using Bokeh
* Explore ElasticSearch and Kibana to Visualize




