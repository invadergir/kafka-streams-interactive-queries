package com.example.kafkastreamsinteractivequeries

import java.util.concurrent.CountDownLatch

import com.typesafe.scalalogging.Logger
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.Produced

import scala.util.{Failure, Success}

//import scala.collection.JavaConversions._
import java.lang.Long
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.example.kafkastreamsinteractivequeries.util._
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, Materialized}
import org.apache.kafka.streams.state.{KeyValueStore, QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.apache.kafka.streams.{StreamsConfig, _}

import scala.concurrent.Future

object KVStreamProcessor {

  val appName = "KVStreamConsumer-App"

  val inputTopic = "input-topic"
  val outputTopic = "output-topic"
  val storeName = "kv-store"

  // logging
  LoggerUtil.setRootLogLevel(ch.qos.logback.classic.Level.INFO)
  val log = Logger(appName)

  //////////////////////////////////////////
  // config options
  val printTotalsOnly = false
  //////////////////////////////////////////

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

  /** Process the stream
    */
  def processStream(
    builder: StreamsBuilder,
    inputTopic: String,
    outputTopic: String
  ): Topology = {

    val source: KStream[String, String] = builder.stream(inputTopic)

    // This looks like an error in the IDE but it's ok (java conversion issue probably):
    source
      .groupBy( (k,v) => k )
      .reduce (
        (a, b) => b,
        Materialized.as(storeName).asInstanceOf[Materialized[String, String, KeyValueStore[Bytes, Array[Byte]]]]
      )
      // output to topic
      .toStream().to(outputTopic, Produced.`with`(Serdes.String(), Serdes.String()))

    builder.build()
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

    // Print out the topology (sources, sinks, and global state stores)
    val topology: Topology = processStream(builder, inputTopic, outputTopic)
    println(">>>>  topology = " + topology.describe)
    // or easier, like this: 
    //println(">>>>  topology = "+builder.build.describe)

    // Create the streams object from the topology
    val stream: KafkaStreams = new KafkaStreams(topology, config)

    import scala.concurrent.ExecutionContext.Implicits.global
    // Run this in a separate thread so we can handle input in main thread.
    val commandLoop = Future {
      acceptRuntimeCommands(stream, storeName)
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
  def acceptRuntimeCommands(stream: KafkaStreams, storeName: String): Unit = {

    while (latch.getCount > 0) {
      val command = scala.io.StdIn.readLine("Enter command: (q)uery, e(x)it: ")
      command.trim.toLowerCase() match {
        case "q" => {

          try{

            lazy val kvStore: ReadOnlyKeyValueStore[String, String] =
              Option(stream.store(storeName, QueryableStoreTypes.keyValueStore[String, String])).getOrElse(throw new RuntimeException("couldn't get store for "+storeName))

            if (printTotalsOnly) {
              var count = 0L
              val iter = kvStore.all
              while( iter.hasNext ) {
                iter.next
                count += 1
              }
              println(s"Current keys in store $storeName: $count")
            }
            else { // print state of 'interested' keys
              println("Current State:")
              val interestedKeys = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
              interestedKeys.map( k => (k, kvStore.get(k)) ).foreach{ tup =>
                println(s"${tup._1} -> ${tup._2}")
              }
            }
          } catch { case e: Throwable => println("Caught exception getting data out of data store: "); e.printStackTrace() }
        }
        case "x" => println("Exiting..."); latch.countDown()
        case other => println("Unknown command: "+other)
      }
    }
  }


}
