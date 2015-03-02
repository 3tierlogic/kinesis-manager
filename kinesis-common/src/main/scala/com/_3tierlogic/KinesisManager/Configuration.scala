package com._3tierlogic.KinesisManager

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._

/**
 * @author Eric Kolotyluk
 */
object Configuration extends Logging with Environment {
  
  // This forces instantiation of the Environment object, and as a side effect,
  // it logs the current environment variables
  environment

  /** Typesafe config Object
    *
    * It is important that com.typesafe.config.ConfigFactory only be called once.
    * If it is called more than once, important configuration information can be
    * lost, or there can be other inconsistencies that are hard to troubleshoot.
    */
  val configuration = ConfigFactory.load()
  
  val configurationLog = configuration
    .entrySet()
    .map(entry => (entry.getKey, entry.getValue.render))
    .toList
    .sortBy(_._1)
    .map(tuple => tuple._1 + " = " + tuple._2)
    .mkString("\n<configuration>\n    ", "\n    ", "\n</configuration>")
      
  logger.info(configurationLog)  
}

trait Configuration {
  
  def config = Configuration.configuration
  
}
