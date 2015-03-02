package com._3tierlogic.KinesisManager

import org.slf4j.Logger
import org.slf4j.LoggerFactory

trait LogbackLogging {
  
  val logger = LoggerFactory.getLogger(getClass);

}