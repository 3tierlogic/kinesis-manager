package com._3tierlogic.KinesisManager.service

import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import akka.actor.Actor
import akka.actor.Props

import com._3tierlogic.KinesisManager.Configuration
import com._3tierlogic.KinesisManager.protocol._
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.{CreateStreamRequest, DescribeStreamRequest, PutRecordsRequest, PutRecordsRequestEntry, ResourceNotFoundException}

import scala.collection.immutable.StringOps
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

object StreamManager {
  
  lazy val actorRef = Service.actorSystem.actorOf(Props[StreamManager])

}

class StreamManager extends Actor with ActorLogging with Configuration {
  /* 
   * Internal Message Protocol.
   * 
   * See also Service.scala for global prototol.
   */
  case object StreamActiveCheck
  case object StreamDeletedCheck

  lazy val amazonKinesisClient = new AmazonKinesisClient
  lazy val streamName = config.getString("amazon.web.service.kinesis.stream.name")
  lazy val describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName)
  lazy val kinesisEndpoint = config.getString("amazon.web.service.client.endpoint")

  val senderQueue = new scala.collection.mutable.Queue[ActorRef]
    
  var startSender: ActorRef = null
  
  var streamActivationTime: Long = 0
  var streamActivationTimeLimit: Long = 0
  
  def receive = {
  
    case Start =>
      
      startSender = sender
      
      log.info("Start: signalling %s we have Started".format(startSender.path.name))
      startSender ! Started
            
    case OpenStream =>
      
      senderQueue.enqueue(sender)
      streamActiveCheck
      
    case OpenOrCreateStream =>
      
      senderQueue.enqueue(sender)
      openOrCreateStream
    
    case StreamDeletedCheck => streamDeletedCheck
     
    case StreamActiveCheck => streamActiveCheck
  
    case StreamActive => streamActive
    
  }
  
  def openOrCreateStream = {
    try {
      log.info(s"kinesisEndpoint = $kinesisEndpoint")        
      amazonKinesisClient.setEndpoint(kinesisEndpoint)
        
      if (! streamExists) createStream
      else {
        val streamStatus = getStreamStatus
        if (streamStatus == "ACTIVE")
          self ! StreamActive
        else if (streamStatus == "CREATING")
          self ! StreamActiveCheck
        else if (streamStatus == "DELETING") {
          context.system.scheduler.scheduleOnce(20 seconds, self, StreamDeletedCheck)
        } else {
          log.error(s"unknown streamStatus = $streamStatus")
        }
      } 
    } catch {
      case exception: com.amazonaws.AmazonClientException =>
        if (exception.getMessage.contains("credentials")) {
          log.error(exception.getMessage)
          startSender ! StartFailed(exception.getMessage)
          // TODO good place to log a URL that better explains the problem. EK
        }
  
      case exception: Exception =>
        log.error(exception, "Your message")
        log.error("-------------" + exception.getMessage)
        log.error("-------------" + exception.getCause)
        log.error("-------------" + exception.getStackTrace.mkString("\n"))
    }
  }
  
  def streamExists = {
    val streamNameList = amazonKinesisClient.listStreams.getStreamNames.toList
    val streamNameLog  = streamNameList.mkString("\n<stream-names>\n  ", "\n  ", "\n</stream-names>")
    log.info(streamNameLog)
    val streamExists = streamNameList.contains(streamName)
    if (streamExists) {
      if (streamActivationTime == 0) {
          streamActivationTime = System.currentTimeMillis()
          streamActivationTimeLimit = streamActivationTime + Duration(10, MINUTES).toMillis
      }
    }
    streamExists
  }
  
  def getStreamStatus = {
    val describeStreamResponse = amazonKinesisClient.describeStream(describeStreamRequest)
    val streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus()
    log.info(s"streamStatus = $streamStatus")
    streamStatus
  }
  
  def createStream = {
    if (streamExists) log.warning(s"stream $streamName already exists, no point in trying to create it")
    else {
      log.info(s"creating stream $streamName")

      val createStreamRequest = new CreateStreamRequest()
        .withStreamName(streamName)
        .withShardCount(config.getInt("amazon.web.service.kinesis.shard.count"))
  
      amazonKinesisClient.createStream(createStreamRequest)
  
      val streamStatus = getStreamStatus
  
      if (streamStatus == "CREATING") {
        context.system.scheduler.scheduleOnce(20 seconds, self, StreamActiveCheck)
        log.info("Waiting for stream to go ACTIVE")
      } else
        log.error(s"stream $streamName: expecting status CREATING, but got $streamStatus")
    }
  }
  
  def streamDeletedCheck = {
      if (streamExists) {
        val streamStatus = getStreamStatus
        if (streamStatus == "DELETING") {
          log.info(s"waiting for stream $streamName to be deleted")
          context.system.scheduler.scheduleOnce(20 seconds, self, StreamDeletedCheck)
        }
      } else createStream
  }
  
  def streamActiveCheck = {
    try {
      val streamStatus = getStreamStatus
      if (streamStatus == "ACTIVE") {
        // Wait for things to settle a little, based on AWS example code. EK
        context.system.scheduler.scheduleOnce(10 seconds, self, StreamActive)
      } else if (System.currentTimeMillis > streamActivationTimeLimit) {
        log.error(s"stream $streamName never went active")
        senderQueue.foreach { _ ! StreamActiveTimeout }
      } else if (streamStatus == "DELETING") {
          context.system.scheduler.scheduleOnce(20 seconds, self, StreamDeletedCheck)
      } else if (streamStatus == "CREATING") {
        context.system.scheduler.scheduleOnce(20 seconds, self, StreamActiveCheck)
        log.info("Still waiting for stream to go ACTIVE")
      } else {
        log.error(s"stream $streamName: unknown status $streamStatus")
      }

    } catch {
      case resourceNotFoundException: ResourceNotFoundException =>
        log.error(resourceNotFoundException.getMessage)
    }
  }
  
  def streamActive = {
    log.info(s"stream $streamName is ACTIVE")
    senderQueue.dequeueAll { actorRef => true }.foreach { _ ! StreamActive }
  }
}