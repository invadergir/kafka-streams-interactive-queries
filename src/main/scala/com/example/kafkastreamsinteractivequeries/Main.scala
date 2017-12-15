package com.example.kafkastreamsinteractivequeries

//import org.json4s._
//import org.json4s.JsonDSL._
//import org.json4s.jackson.JsonMethods._
//import org.json4s.jackson.Serialization.{read, write}
import com.typesafe.scalalogging.Logger
import java.util.Properties
import java.time._
import java.util.Collections
import java.util.Properties

import org.apache.kafka.common.utils.Bytes

//import scala.collection.JavaConversions._
import scala.collection.JavaConverters.asJavaIterableConverter

import com.example.kafkastreamsinteractivequeries.util._
import com.example.kafkastreamsinteractivequeries.util.PropertiesUtil._

import java.lang.Long
import java.util.Properties
import java.util.concurrent.TimeUnit
 
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KeyValueMapper, KStream, KStreamBuilder, KTable, Materialized, Produced, ValueMapper}
import org.apache.kafka.streams.state.KeyValueStore

object Main {

  // logging
  LoggerUtil.setRootLogLevel(ch.qos.logback.classic.Level.INFO)
  val log = Logger("Main")

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

  /** Main method
    * Calls code written by following https://kafka.apache.org/10/documentation/streams/tutorial
    */
  def main(args: Array[String]) = {

    log.info("Starting...")

    //WordCount.run()
    KVStreamProcessor.run()
   
    log.info("Done.")
  }

}
