Roughly Following:
https://docs.confluent.io/current/streams/developer-guide/interactive-queries.html#streams-developer-guide-interactive-queries-discovery

Code example: 
https://github.com/confluentinc/kafka-streams-examples/tree/4.0.x/src/main/java/io/confluent/examples/streams/interactivequeries

# Purpose
You can use this in combination with kafka-key-value-producer to locally test some availability & scalability scenarios.  Keys 'a' through 'j' are printed when you query the state store.  Spin up more than 1 instance, send it some data, and see where the keys go.  Your input topic ('input-topic') must have > 1 partition.  Once you sent some data, kill one or the other instance and see what happens.  It's a valuable exercise to understand how kafka streams stores data with partitioning.


# To create input & output topics: 
see bin/create-topics.sh # default num topics is 2, can specify differently
   
# To run console producer:
kafka-console-producer.sh --broker-list localhost:9092 --topic input-topic

# To run console consumer to see output:
## For WordCount: (when testing word counts - see Main.scala to select active stream processor)
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic output-topic \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer    

## For KVStreamProcessor: (when testing generic String-String store, see Main.scala)
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic output-topic \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer    

# To create jar file:
sbt assembly
