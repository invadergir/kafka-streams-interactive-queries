package com.example.kafkastreamsinteractivequeries.util

import ch.qos.logback.classic.Logger
import org.slf4j.LoggerFactory    
import ch.qos.logback.classic.Level

/** Helper functions for logging
  * 
  */
object LoggerUtil {

  /** Set the root log level.
    */
  def setRootLogLevel(level: Level) {
    val rootLogger: Logger = 
      LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]

    rootLogger.setLevel(level)
  }
}
