package com._3tierlogic.KinesisManager

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import ch.qos.logback.core.joran.spi.JoranException
import ch.qos.logback.core.util.StatusPrinter

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/** '''Provide Logging Handle to Root Class'''
 *
 * @author Eric Kolotyluk
 */
trait LogbackLogging {
  
  LogbackLogging.loggerContext

  val logger = LoggerFactory.getLogger(getClass);

}

/** '''Singleton Logging Configuration'''
  *  
  * Log the current logging environment as if we were in debug mode. This is especially useful
  * when troubleshooting, or reverse engineering code, and trying to understand the logging
  * environment.
  * 
  * @author Eric Kolotyluk
  * 
  * @see [[http://logback.qos.ch/manual/configuration.html LogBack Configuration]]
  * 
  */
object LogbackLogging {
  // assume SLF4J is bound to logback in the current environment
  val loggerContext = LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]
  // print logback's internal status
  StatusPrinter.print(loggerContext)
}