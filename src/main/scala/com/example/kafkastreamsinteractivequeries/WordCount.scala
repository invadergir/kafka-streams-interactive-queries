package com.example.kafkastreamsinteractivequeries

//import org.json4s._
//import org.json4s.JsonDSL._
//import org.json4s.jackson.JsonMethods._
//import org.json4s.jackson.Serialization.{read, write}
import java.io.File

import com.typesafe.scalalogging.Logger
import java.util.Properties
import java.time._
import java.util.Collections
import java.util.Properties
import java.util.concurrent.CountDownLatch

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore

import scala.util.{Failure, Success}

//import scala.collection.JavaConversions._
import scala.collection.JavaConverters.asJavaIterableConverter

import com.example.kafkastreamsinteractivequeries.util._
import com.example.kafkastreamsinteractivequeries.util.PropertiesUtil._

import java.lang.Long
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KeyValueMapper, KStream, KStreamBuilder, KTable, Materialized, Produced, ValueMapper}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import java.nio.file.Files

object WordCount {

  val appName = "WordCount-App"

  // logging
  LoggerUtil.setRootLogLevel(ch.qos.logback.classic.Level.INFO)
  val log = Logger(appName)

  // Formatter for json4s
  implicit val jsonFormats = org.json4s.DefaultFormats

  val bootstrapServers = "localhost:9092"

  // add shutdown hook to close the stream on exit
  def closeOnExit(stream: KafkaStreams) = {
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println(">>>>>> Closing stream...")
      stream.close(10, TimeUnit.SECONDS)
      println(">>>>>> Done closing stream.")
    }))
  }

  // runs a stream and safely shuts down
  val latch: CountDownLatch = new CountDownLatch(1)
  def runStreamAndShutdown(stream: KafkaStreams) = {

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println(">>>>>> Closing stream...")
      stream.close()
      println(">>>>>> Done closing stream.")
      latch.countDown()
    }))

    try {
      stream.start()
      latch.await()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.exit(1)
    }
    System.exit(0)
  }

  val inputTopic = "input-topic"
  val outputTopic = "output-topic"
  val countsStoreName = "counts-store"
  val port = 8080

  /** Process the stream
    */
  def processStream(
    builder: StreamsBuilder,
    inputTopic: String,
    outputTopic: String
  ): Topology = {

    val source: KStream[String, String] = builder.stream(inputTopic)

    // split into words
    val words: KStream[String, String] = source.flatMapValues { s =>
      s.toLowerCase.split("\\W+").toIterable.asJava
    }

    // Key the stream on the value string, i.e. the lower cased word by using
    // groupBy.
    // Store in 'counts-store'.  Can be queried.
    val counts: KTable[String, Long] = words
      .groupBy((key, value) => value)
      .count(Materialized.as(countsStoreName).asInstanceOf[Materialized[String, Long, KeyValueStore[Bytes, Array[Byte]]]])
    //todo later try this: .count("counts-store")

    // Output counts to output topic.
    counts.toStream().to(outputTopic, Produced.`with`(Serdes.String(), Serdes.Long()))
    //counts.toStream.to // to(stringSerde, longSerde, "output-topic-2")

    // Print out the topology (sources, sinks, and global state stores)
    val topology: Topology = builder.build()
    println(">>>>  topology = " + topology.describe)
    // or easier, like this:
    //println(">>>>  topology = "+builder.build.describe)

    topology
  }

  /** Main method
    * You can add an HTTP server (start on separate thread of course) to query
    * local state.  In this app we just use a command line interface.
    */
  def run() {

    val config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, appName)
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)

      // For embedded HTTP server:
      // Provide the details of our embedded http service that we'll use to connect to this streams
      // instance and discover locations of stores.
      // (TODO not sure about this - is it needed only for HTTP (and why?))
      //      p.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + port)

      // This configures the persistence of local streaming data.
      // If set to a temp dir, it will change every time.
      // By default it uses /tmp/kafka-streams which is fine for testing,
      // but for real apps, this will need to be set to something persistent.
      //val streamsDataDir = Files.createTempDirectory(new File("/tmp").toPath, appName).toFile
      //p.put(StreamsConfig.STATE_DIR_CONFIG, streamsDataDir.getPath)

      // attempt to speed it up
      p.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, new Integer(0))
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, new Integer(2 * 1000))
      p
    }

    // create a topology builder
    val builder: StreamsBuilder = new StreamsBuilder()

    val topology = processStream(builder, inputTopic, outputTopic)

    // Create the streams object from the topology
    val stream: KafkaStreams = new KafkaStreams(topology, config)

    import scala.concurrent.ExecutionContext.Implicits.global
    // Run this in a separate thread so we can handle input in main thread.
    val commandLoop = Future {
      acceptRuntimeCommands(stream, countsStoreName)
    }
    commandLoop.onComplete { result =>
      result match {
        case Success(_) => println("Commandloop success.")
        case Failure(_) => println("Commandloop failure.")
      }
    }

    runStreamAndShutdown(stream)
  }

  /** accept some commands to query the current state store(s)
    *
    */
  def acceptRuntimeCommands(stream: KafkaStreams, countsStoreName: String): Unit = {

    while (latch.getCount > 0) {
      val command = scala.io.StdIn.readLine("Enter command: (q)uery, e(x)it: ")
      command.trim.toLowerCase() match {
        case "q" => {
          println("Current State:")
          val interestedKeys = List("h", "t", "n")

          try{

            lazy val countsStore: ReadOnlyKeyValueStore[String, Long] =
              Option(stream.store(countsStoreName, QueryableStoreTypes.keyValueStore[String, Long])).getOrElse(throw new RuntimeException("couldn't get store for "+countsStoreName))

            interestedKeys.map( k => (k, countsStore.get(k)) ).foreach{ tup =>
              println(s"${tup._1} -> ${tup._2}")
            }
          } catch { case e: Throwable => println("Caught exception getting data out of data store: "); e.printStackTrace() }
        }
        case "x" => println("Exiting..."); latch.countDown()
        case other => println("Unknown command: "+other)
      }
    }
  }


}
