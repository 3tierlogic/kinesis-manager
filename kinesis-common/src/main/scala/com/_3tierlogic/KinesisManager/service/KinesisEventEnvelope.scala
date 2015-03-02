package platform3.gnip2kinesis

import com.codahale.jerkson.Json._

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import org.joda.time.DateTime

import java.text.SimpleDateFormat
import java.util.Date
import java.util.UUID

//import platform3.util.logging.Platform3Logging

import scala.collection.mutable.MutableList

/** A wrapper for all events streamed into Kinesis: GIGO
  * 
  * Check that the parameters in the map are kosher, replacing them if necessary with valid ones.
  * If there is something wrong with the parameters, or they are missing, we still want to construct
  * something useful.
  * 
  * @author Eric Kolotyluk
  * 
  * @param data the raw data for the event. 
  * @param type a useful indicator of what type of garbage event this is. If this is missing,
  *        "unknown" will be used.
  * @param uuid UUID or GUID used to uniquely identify this event. If this is missing, or does not
  *         conform to [[java.util.UUID]], a random UUID will be generated.
  * @param time GMT timestamp for the event. If this is missing, or does not conform to ISO 8601,
  *         the current GMT TOD will be used instead.
  * @param nano nanoseconds discriminator used to order timestamps finder than one milisecond.
  *         If this is missing, or does not parse to a Long, System.nanoTime() will be used instead.
  *
  * @see [[https://docs.oracle.com/javase/7/docs/api/java/lang/System.html#nanoTime() System.nanoTime]]
  * @see [[https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html java.util.UUID]]
  */
@JsonIgnoreProperties(Array("warnings", "logger" /* from Platform3Logging*/))
case class KinesisEventEnvelope(data: String, var `type`: String, var uuid: String, var time: String, var nano: Long, var warnings: List[String]) /* extends Platform3Logging */ {
  
  def this(data: String) = this(data, null, null, null, 0, null)
  
  if (warnings == null) warnings = List[String]()
  
  if (`type` == null) `type` = "unknown"
  
  if (uuid == null) uuid = UUID.randomUUID.toString
  else try {
    uuid = UUID.fromString(uuid).toString()
  } catch {
    case exception: IllegalArgumentException =>
      uuid = UUID.randomUUID.toString
      val message = exception.getMessage + s", using $uuid instead"
      //logger.warn(message)
      warnings = message :: warnings
      uuid
  }

  if (time == null) time = KinesisEventEnvelope.simpleDateFormat.format(new Date)
  else try {
    time = KinesisEventEnvelope.simpleDateFormat.parse(time).toString()
  } catch {
    case exception: java.text.ParseException =>
    time = KinesisEventEnvelope.simpleDateFormat.format(new Date)
    val message = exception.getMessage + s", using $time instead"
    //logger.warn(message)
    warnings = message :: warnings
    time
  }
  
  if (nano < 1) nano = System.nanoTime
  
  /** List of Warnings
    * 
    * Constructing a KinesisEventEnvelope will always succeed, but if there are problems with the parameters,
    * you can access them from here.
    */
  def getWarnings = warnings
  
  /** JSON String for this Object
   * 
   */
  def getJson = generate(this)
}
  
/** Companion Object
  * 
  */
object KinesisEventEnvelope /* extends Platform3Logging */ {
  
  /** ISO 8601 Time Format
    * @see [[http://en.wikipedia.org/wiki/ISO_8601 ISO 8601 Time Format]]
    */
  val simpleDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'")
  simpleDateFormat.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))

  // TODO - @ThreadSafe
  /** Construct KinesisEventEnvelope from a Parameter Map
    * 
    * Check that the parameters in the map are kosher, replacing them if necessary with valid ones.
    * This constructor is designed to deal with URL parameters from an HTTP POST request. However,
    * if there is something wrong with the parameters, or they are missing, we still want to accept
    * the post command and carry on for the sake of resilience.
    * 
    * @param "time" GMT timestamp for the event. If this is missing, or does not conform to ISO 8601,
    *         the current GMT TOD will be used instead.
    * @param "nano" nanoseconds discriminator used to order timestamps finder than one milisecond.
    *         If this is missing, or does not parse to a Long, System.nanoTime() will be used instead.
    * @param "uuid" UUID or GUID used to uniquely identify this event. If this is missing, or does not
    *         conform to [[java.util.UUID]], a random UUID will be generated.
    * @param "type" a useful indicator of what type of garbage event this is. If this is missing,
    *        "unknown" will be used.
    * @param data the raw data for the event. 
    *
    */
  def apply(data: String, parameters: Map[String,String]) = {
    // Create a new warnings object in case we get called on different threads
    // as we are in a singleton object, and need to stay threadsafe.
    var warnings = List[String]()
    def getType = parameters.getOrElse("type", null)
    def getUuid = parameters.getOrElse("uuid", null)
    def getTime = parameters.getOrElse("time", null)
    def getNano = {
    if (parameters.isDefinedAt("nano")) try {
      parameters("nano").toLong
    } catch {
        case exception: NumberFormatException => 
          val nano = System.nanoTime
          val message = "NumberFormatException: " + exception.getMessage + s", using $nano instead"
          //logger.warn(message)
          warnings = message :: warnings
          nano
      } else 0L
    }
    
    new KinesisEventEnvelope(data, getType, getUuid, getTime, getNano, warnings)
  }
}
