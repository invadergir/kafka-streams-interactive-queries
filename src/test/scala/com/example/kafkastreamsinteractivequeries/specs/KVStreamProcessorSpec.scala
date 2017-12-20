package com.example.kafkastreamsinteractivequeries

import java.lang

import org.scalatest.junit.JUnitRunner
import com.madewithtea.mockedstreams.MockedStreams
import org.apache.kafka.common.serialization.Serdes

import scala.collection.immutable

class KVStreamProcessorSpec extends UnitSpec {

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

  describe("KVStreamProcessor") 
  {
    it("should properly store k-v items") {

      // This stream processor doesn't really do much
      // except store and output the input (its a pipe).
      // So there's not much to test.

      val inputSeq = Seq("A"->"1", "B"->"2", "CDE"->"3")

      val expected = Seq()

      val result = MockedStreams().topology { builder =>
        KVStreamProcessor.processStream(builder, inputTopic, outputTopic)
      }
        .config(getStreamConf)  // returns MockedStreams.Builder
        .input(inputTopic, stringSerde, stringSerde, inputSeq) // returns MockedStreams.Builder
        .output(outputTopic, stringSerde, stringSerde, inputSeq.size)

      println("result = "+result)
      result should be (Seq(("A", "1"), ("B", "2"), ("CDE", "3")))
    }
  }
}

  

