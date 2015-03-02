package com._3tierlogic.KinesisManager.consumer

import java.util.UUID

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import com._3tierlogic.KinesisManager.Configuration
import com._3tierlogic.KinesisManager.MessageEnvelope
import com._3tierlogic.KinesisManager.MessagePart
import com._3tierlogic.KinesisManager.protocol.Start
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.DescribeStreamRequest
import com.amazonaws.services.kinesis.model.DescribeStreamResult
import com.amazonaws.services.kinesis.model.GetRecordsRequest
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef

/** Actor for Managing Kinesis Consumer Activity
  * 
  * This actor attempts to make it easier to cons
  * @author Eric Kolotyluk
  * 
  * @see [[http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html Kinesis Introduction]]
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html AWS SDK for Java API Reference]]
  */
class KinesisConsumer extends Actor with ActorLogging with Configuration {
  
  val streamName = config.getString("kinesis-manager.stream.name")

  case object Pulse
  case class Wait(time: Long)

  val amazonKinesisClient = new AmazonKinesisClient

  
  var startSender: ActorRef = null
  
  var describeStreamResponse: DescribeStreamResult = null
  
  var lastSequenceNumber: String = null
  
  def receive = {
    
    case Start =>
      
      startSender = sender
      
      amazonKinesisClient.setEndpoint("kinesis.us-west-2.amazonaws.com", "kinesis", "us-west-2")
      log.info("Start: AWS endpoint = kinesis.us-west-2.amazonaws.com, kinesis, us-west-2")

      context.system.scheduler.scheduleOnce(40 seconds, self, Wait(System.currentTimeMillis))
      
    case Wait(time) =>
      
      val listStreamResult = amazonKinesisClient.listStreams()
      val streamNameList = listStreamResult.getStreamNames
      
      val i = streamNameList.iterator
      while (i.hasNext) {
        val name = i.next
        log.info(s"Start: name = $name")
      }
      
      if (streamNameList.contains(streamName)) {
        log.info(s"Start: using $streamName")
          
        val describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
  
        describeStreamResponse = amazonKinesisClient.describeStream(describeStreamRequest)
        val streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus()
        log.info(s"Start: streamStatus = $streamStatus")
        
        // context.system.scheduler.schedule(1 seconds, 1 seconds, self, Pulse) 
        context.system.scheduler.scheduleOnce(1 seconds, self, Pulse)

      } else {
        log.error(s"Start: cannot find stream $streamName")
      }

    case Pulse =>
      
      log.debug("Pulse: checking stream for messages")
      
      val shards = describeStreamResponse.getStreamDescription.getShards
      val shard = shards.get(0)
          
      val getShardIteratorRequest = new GetShardIteratorRequest();
      getShardIteratorRequest.setStreamName(streamName);
      getShardIteratorRequest.setShardId(shard.getShardId());
      
      if (lastSequenceNumber == null)
        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON")
      else {
        getShardIteratorRequest.setStartingSequenceNumber(lastSequenceNumber)
        getShardIteratorRequest.setShardIteratorType("AFTER_SEQUENCE_NUMBER")
      }
  
      val getShardIteratorResult = amazonKinesisClient.getShardIterator(getShardIteratorRequest);
      val shardIterator = getShardIteratorResult.getShardIterator();
        
      val getRecordsRequest = new GetRecordsRequest();
      getRecordsRequest.setShardIterator(shardIterator);
      getRecordsRequest.setLimit(25);
        
      val getRecordsResult = amazonKinesisClient.getRecords(getRecordsRequest);
      val records = getRecordsResult.getRecords();
        
      log.info("Start: got " + records.size() + " records")
      
      val partMap = scala.collection.mutable.Map[UUID,scala.collection.mutable.Map[Long,MessagePart]]()
  
      val i = records.iterator()
      while (i.hasNext()) {
        val record = i.next()
        lastSequenceNumber = record.getSequenceNumber
        log.info(s"Start: record = $record")
        val data = record.getData
        val f = new java.io.ByteArrayInputStream(data.array())
        val buffer = new java.io.ObjectInputStream(f)
        val eventPart = buffer.readObject().asInstanceOf[MessagePart]
        log.info(s"Start: uuid      = " + eventPart.uuid)
        log.info(s"Start: part      = " + eventPart.part)
        log.info(s"Start: of        = " + eventPart.of)
        log.info(s"Start: data.size = " + eventPart.data.size)
        log.info(s"Start: data      = " + eventPart.data)
          
        ///////////////////////////////////
          
        var partList = partMap.getOrElse(eventPart.uuid, scala.collection.mutable.Map[Long,MessagePart]())
          
        if (partList.size == 0) partMap(eventPart.uuid) = partList
          
        partList(eventPart.part) = eventPart
          
        log.info("Start: partList.size = " + partList.size)
          
        if (partList.size == eventPart.of) {
          val arrayBuffer = new scala.collection.mutable.ArrayBuffer[Byte]
          for (i <- 1L to eventPart.of) {
            val data = partList(i).data
            log.info("Start: eventPart.data.length = " + data.length)
            arrayBuffer.appendAll(data)
          }
            
          log.info("Start: deserializing " + eventPart.uuid)
            
          val byteArrayInputStream = new java.io.ByteArrayInputStream(arrayBuffer.toArray)
          val objectInputStream = new java.io.ObjectInputStream(byteArrayInputStream)
            
          try {
            while (true) {
              val eventEnvelope = objectInputStream.readObject().asInstanceOf[MessageEnvelope]
//                log.info("Start: data = " + new String(eventEnvelope.data))
//                log.info("Start: type = " + eventEnvelope.`type`)
//                log.info("Start: uuid = " + eventEnvelope.uuid)
//                log.info("Start: time = " + eventEnvelope.time)
//                log.info("Start: nano = " + eventEnvelope.nano)
            }
 
          } catch {
            case exception: Exception => log.info("Start: finished reading object")
          }
       }
          
      }
      
      context.system.scheduler.scheduleOnce(1 seconds, self, Pulse)
       
    case message: Any =>
      
      log.error("received unknown message = " + message)

  }
  
}