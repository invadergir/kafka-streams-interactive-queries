package com.example.kafkastreamsinteractivequeries

import java.lang

import org.scalatest.junit.JUnitRunner
import com.madewithtea.mockedstreams.MockedStreams
import org.apache.kafka.common.serialization.Serdes

import scala.collection.immutable

class WordCountSpec extends UnitSpec {

  // before all tests have run
  override def beforeAll() = {
    super.beforeAll()
  }

  // before each test has run
  override def beforeEach() = {
    super.beforeEach()
  }

  // after each test has run
  override def afterEach() = {
    //myAfterEach()
    super.afterEach()
  }

  // after all tests have run
  override def afterAll() = {
    super.afterAll()
  }

  import TestConstants._

  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  // Tests start
  //////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  describe("WordCount") 
  {
    it("should properly count words, outputing a table") {

      val inputSeq = Seq("A", "B", "C D E", "C", "D", "B", "F")
        .map( e => ("key", e) ) // make k-v tuples out of em.

      val expected = Seq()

      val result = MockedStreams().topology { builder =>
        WordCount.processStream(builder, inputTopic, outputTopic)
      }
        .config(getStreamConf)  // returns MockedStreams.Builder
        .input(inputTopic, stringSerde, stringSerde, inputSeq) // returns MockedStreams.Builder
        .output(outputTopic, stringSerde, Serdes.Long(), inputSeq.size + 2)

      println("result = "+result)
      // note the 'record update' format.  The output is a KStream:
      result should be (Seq(("a",1), ("b",1), ("c",1), ("d",1), ("e",1), ("c",2), ("d",2), ("b",2), ("f", 1)))
    }
  }
}

  
