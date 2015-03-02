package com._3tierlogic.KinesisManager

import scala.collection.JavaConversions._


object Environment extends LogbackLogging {
  
  val environment = System.getenv
  
  val environmentLog = environment.toList
    .sortBy(_._1)
    .map(tuple => tuple._1 + " = " + tuple._2)
    .mkString("\n<environment>\n    ", "\n    ", "\n</environment>")

  logger.info(environmentLog)
  
}

trait Environment {
  
  def environment = Environment.environment

}