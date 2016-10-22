from __future__ import print_function

import json
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils




f_count = 0;




def country_count_update(newValue, existingCount):
    if existingCount is None:
        existingCount = 0

    return sum(newValue)+existingCount


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: spark_meetup.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext("local[2]",appName="PythonStreamingMeetup")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("checkpoint")

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "1", {topic: 1})
    rsvps = kvs.map(lambda x: x[1])
    rsvps_json = rsvps.map(lambda x: json.loads(x.encode('ascii','ignore')))
    rsvps_us = rsvps_json.filter(lambda x: x['group']['group_country']=='us')
    rsvps_city_pair = rsvps_us.map(lambda x: (x['group']['group_city'],1))
    rsvps_city_statefulCount = rsvps_city_pair.updateStateByKey(country_count_update)
    

                                
                            


    rsvps_city_statefulCount.pprint()

    ssc.start()
ssc.awaitTermination()