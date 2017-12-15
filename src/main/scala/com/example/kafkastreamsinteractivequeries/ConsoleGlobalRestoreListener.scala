package com.example.kafkastreamsinteractivequeries

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.processor.StateRestoreListener

class ConsoleGlobalRestoreListener extends StateRestoreListener {

  override def onRestoreStart(
    topicPartition: TopicPartition,
    storeName: String,
    startingOffset: Long,
    endingOffset: Long
  ): Unit = {

    System.out.print("Started restoration of " + storeName + " partition " + topicPartition.partition)
    System.out.println(" total records to be restored " + (endingOffset - startingOffset))
  }

  override def onBatchRestored(
    topicPartition: TopicPartition,
    storeName: String,
    batchEndOffset: Long,
    numRestored: Long
  ): Unit = {

    System.out.println("Restored batch " + numRestored + " for " + storeName + " partition " + topicPartition.partition)
  }

  override def onRestoreEnd(
    topicPartition: TopicPartition,
    storeName: String,
    totalRestored: Long
  ): Unit = {

    System.out.println("Restoration complete for " + storeName + " partition " + topicPartition.partition)
  }
}