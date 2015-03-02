package com._3tierlogic.KinesisManager

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._


/** Typesafe Configuration API
  * 
  * Provides a property getter for the singleton configuration.
  * 
  * It is important that com.typesafe.config.ConfigFactory only be called once.
  * If it is called more than once, important configuration information can be
  * lost, or there can be other inconsistencies that are hard to troubleshoot.
  * 
  * @author Eric Kolotyluk
  */
trait Configuration {
  
  def config = Configuration.configuration
  
}

/** Typesafe Configuration object
  *  
  * @author Eric Kolotyluk
  */
object Configuration extends LogbackLogging with Environment {
  
  // This forces instantiation of the Environment object, and as a side effect,
  // it logs the current environment variables. It is best to log the environment
  // before logging the configuration because the configuration may depend on the
  // environment.
  environment

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
