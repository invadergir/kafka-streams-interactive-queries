package com.example.kafkastreamsinteractivequeries.util

import java.util.Properties


/** Easily add any value (integers, floats, doubles, and longs) to properties.
  * 
  */
object PropertiesUtil {

  implicit class PropertiesAdditions(val props: Properties) {

    def putv(key: String, v: AnyVal) = props.put(key, v.toString)
  }
}
