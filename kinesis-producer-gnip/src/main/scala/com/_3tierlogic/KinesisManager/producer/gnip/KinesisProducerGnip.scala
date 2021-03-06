package com._3tierlogic.KinesisManager.producer.gnip

import java.nio.ByteBuffer
import java.util.{ArrayList, UUID}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConversions._


import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.actorRef2Scala

import com._3tierlogic.KinesisManager.{Configuration, MessageEnvelope, BlockSegment}
import com._3tierlogic.KinesisManager.protocol.{Put, Start, StartFailed, Started}
import com._3tierlogic.KinesisManager.service.MessageEnvelopeQueue
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.{CreateStreamRequest, DescribeStreamRequest, PutRecordsRequest, PutRecordsRequestEntry, ResourceNotFoundException}

import scala.collection.immutable.StringOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/** Actor for Managing Kinesis Producer Activity
  * 
  * This actor attempts to make it easier to produce messages for Kinesis.
  * It is recommended that you use the KinesisConsumer actor for consuming
  * messages. These actors do not need to reside in the same process to work.
  * In particular, while you can easily shutdown and restart the consumer for
  * hours without data loss, you should not shut down the producer without
  * considering what kind of data loss you may experiences.
  * 
  * The Producer can also scale out by running more than one producer at a time,
  * and can be scaled out dynamically depending on load.
  * 
  * =Limits=
  * 
  * Kinesis has some important limits:
  * 
  * 1. Message blobs cannot be larger than 51200 bytes
  * 
  * 2. Messages cannot be sent at more than 1,000 messages per second per shard.
  * 
  * 3. Shards are not automatically added or deleted.
  * 
  * =Features=
  * 
  * This actor adds some of the following features:
  * 
  * 1. Logical messages can be any length, i.e., much larger than 51200 bytes.
  * 
  * 2. Logical messages are blocked, so throughput can exceed 1,000 messages per second per shard.
  *    Messages less than 51200 bytes will have a higher throughput rate, while messages more than
  *    51200 bytes will have a lower throughput rate.
  * 
  * 3. Blocks are segmented into physical blobs of 52100 bytes or less for optimal use of the bandwidth.
  * 
  * 4. Eventually shards will be automatically added or subtracted depending on load.
  * 
  * @author Eric Kolotyluk
  * 
  * @see [[http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html Kinesis Introduction]]
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html AWS SDK for Java API Reference]]
  */
class KinesisProducer extends Actor with ActorLogging with Configuration {

  /* 
   * Internal Message Protocol.
   * 
   * See also Service.scala for global prototol.
   */
  case object Active
  case object PulseEnvelopes
  case object PulseBlocks
  case class Wait(time: Long)
  case class PutRecords(records: scala.collection.mutable.ArrayBuffer[Future[ByteBuffer]])
  
  val streamName = config.getString("amazon.web.service.kinesis.stream.name")

  val describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName)

  val client = new AmazonKinesisClient
  
  def getClient = client
  
  var startSender: ActorRef = null
  
  var startTime: Long = 0
  var endTime: Long = 0

  /**
   * http://www.ibm.com/developerworks/library/j-jtp04186/
   */
  //val messageEnvelopeQueue = new ConcurrentLinkedQueue[MessageEnvelope]
  val putRecordsRequestEntryQueue = new ConcurrentLinkedQueue[PutRecordsRequestEntry]
  
  var messageEnvelopeBufferFuture = Future {}
  
  var testStartTime = 0L

  /** Generate a 4 character key
    *
    * These are likely used to hash into which shard to use when a Kinesis stream has
    * more than one shard. The Amazon documentation is not terribly clear on this, so
    * this is mostly intuition. Since we are not likely to have more that 9999 shards,
    * this magic number is safe to use.
    * 
    * @see [[http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-add-data-to-stream.html Kinesis Documentation]]
    */
  def getKinesisPartitionKey = {
    "%04d".format(Math.round(Math.random * 9999))
  }

  def receive = {
    
    case Start =>
      
      startSender = sender
      
      log.info("Active: signalling %s we have Started".format(startSender.path.name))
      startSender ! Started
      
      readyStream
      
    case Wait(time) =>
      
      try {
        val describeStreamResponse = client.describeStream( describeStreamRequest )
        val streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus()
        log.info(s"Wait: streamStatus = $streamStatus")
        if (streamStatus.equals("ACTIVE")) {
          // Wait for things to settle a little, based on AWS example code. EK
          context.system.scheduler.scheduleOnce(1 seconds, self, Active)
        } else if (time > endTime) {
          log.error("Stream never went active")
        } else {
          context.system.scheduler.scheduleOnce(20 seconds, self, Wait(System.currentTimeMillis))
          log.info("Still waiting for stream to go ACTIVE")
        }

      } catch {
        case resourceNotFoundException: ResourceNotFoundException =>
          log.error(resourceNotFoundException.getMessage)
      }

    case Active =>
      
      context.system.scheduler.schedule(1 seconds, 1 seconds, self, PulseEnvelopes)
      context.system.scheduler.schedule(1 seconds, 1 seconds, self, PulseBlocks)
      
      testStartTime = System.currentTimeMillis
      
      val recordCount = 100000

      for (i <- 0 to recordCount - 1) {
        val future = Future {
          val string = "The quick brown fox jumped over the lazy dog"
          val bytes = new StringOps(string).getBytes
          val messageEnvelope = new MessageEnvelope(bytes, "unknown", "text", UUID.randomUUID, System.currentTimeMillis, System.nanoTime)
          MessageEnvelopeQueue.add(messageEnvelope)
        }
        
      }
      
      val ingestionTime = System.currentTimeMillis - testStartTime
      log.info(s"Active:  $recordCount logical records consumed in $ingestionTime ms")
      
    case Put(messageEnvelope) =>
      
      MessageEnvelopeQueue.add(messageEnvelope)
    
    case PulseEnvelopes => drainMessageEnvelopeQueue

    case PulseBlocks =>

      val putRecordsRequestEntryList  = new ArrayList[PutRecordsRequestEntry]

      def putRecords = {

        val putRecordsRequest = new PutRecordsRequest()
          .withStreamName(streamName)
          .withRecords(putRecordsRequestEntryList)

        val putTime = System.currentTimeMillis
        val putRecordsResult  = client.putRecords(putRecordsRequest)
        log.info("PulseBlocks: put " + putRecordsResult.getRecords.size() + " blocks in " + (System.currentTimeMillis - putTime) + " ms")
        if (putRecordsResult.getFailedRecordCount > 0) log.error(putRecordsResult.getFailedRecordCount + " records failed")
        //log.info(s"PulseBlocks: putRecordsResult = $putRecordsResult")
        putRecordsRequestEntryList.clear
      }
      
      var count = 0
      var size = 0
      
      if (MessageEnvelopeQueue.isEmpty && putRecordsRequestEntryQueue.isEmpty && testStartTime > 0) {
        val blockQueueEmpty = System.currentTimeMillis - testStartTime
        log.info(s"PulseBlocks: block queue empty in $blockQueueEmpty ms")
        testStartTime = 0
      }
      
      // Drain the queue
      while (! putRecordsRequestEntryQueue.isEmpty()) {
        putRecordsRequestEntryList.add(putRecordsRequestEntryQueue.poll)
        count += 1
        size  += 51200
        
        // If we are at the limits of a putRecordsRequest, flush it
        if (count == 500 || size > 4500000) {
           log.info(s"PulseBlocks: count = $count, size = $size")
           putRecords
           count = 0
           size = 0
        }
      }
    
      if (! putRecordsRequestEntryList.isEmpty) putRecords

    
    case message: Any =>
      
       log.error("received unknown message = " + message)
  }
  
  def readyStream = {
    try {
      val kinesisEndpoint = config.getString("amazon.web.service.client.endpoint")
      log.info(s"kinesisEndpoint = $kinesisEndpoint")        
      client.setEndpoint(kinesisEndpoint)
        
      val streamNameList = client.listStreams.getStreamNames.toList
      val streamNameLog  = streamNameList.mkString("\n<stream-names>\n  ", "\n  ", "\n</stream-names>")
      
      log.info(streamNameLog)
      
      if (streamNameList.contains(streamName)) {
        log.info(s"using $streamName")
      } else { 
        log.info(s"creating stream $streamName")
            
        val createStreamRequest = new CreateStreamRequest()
          .withStreamName(streamName)
          .withShardCount(config.getInt("amazon.web.service.kinesis.shard.count"))

        client.createStream(createStreamRequest)
      }

      val describeStreamResponse = client.describeStream(describeStreamRequest)
      val streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus()
      log.info(s"streamStatus = $streamStatus")
      
      if (streamStatus.equals("ACTIVE")) self ! Active
      else if (streamStatus.equals("CREATING")) {
        startTime = System.currentTimeMillis();
        endTime = startTime + ( 10 * 60 * 1000 );
        context.system.scheduler.scheduleOnce(20 seconds, self, Wait(System.currentTimeMillis))
        log.info("Waiting for stream to go ACTIVE")
      }
      else {
        log.error(s"Start: unknown streamStatus = $streamStatus")
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
  
  class BetterByteArrayOutputStream extends java.io.ByteArrayOutputStream {
    def getCount = count
  }
  
  val byteArrayOutputStream = new BetterByteArrayOutputStream()
  val objectOutputStream = new java.io.ObjectOutputStream(byteArrayOutputStream)

  def drainMessageEnvelopeQueue = {
    
    val maximumBlockSize = Integer.MAX_VALUE - 1024
    
    // We skip this pulse if the previous one is not finished, or the queue is empty
    if (messageEnvelopeBufferFuture.isCompleted && !MessageEnvelopeQueue.isEmpty()) {
      messageEnvelopeBufferFuture = Future {
        val timestamp = System.currentTimeMillis
        var count = 0
        
        while (! MessageEnvelopeQueue.isEmpty) {
          if (byteArrayOutputStream.getCount > maximumBlockSize)
            flushBlock
          else {
            objectOutputStream.writeObject(MessageEnvelopeQueue.poll)
            count += 1
          }
        }
        flushBlock
      }
    }
  }
  
  def flushBlock = {
    
    // Now that we have a block, we segment it, and do a bulk put to Kinesis

    //val drainTime = System.currentTimeMillis - testStartTime
    //log.info(s"PulseEnvelopes: $count envelopes drained in $drainTime ms")
    
    objectOutputStream.flush
    byteArrayOutputStream.flush

    val block = byteArrayOutputStream.toByteArray
    
    byteArrayOutputStream.reset
    objectOutputStream.reset
    
    val blockFuture = Future {
      
      var blockCount = 0
      var index = 0
      var partCount = 1L
      val of: Long = block.length / 50000L + 1
      log.info(s"PulseEnvelopes: of = $of")
      
      val uuid = UUID.randomUUID
      val blobBuffer = new scala.collection.mutable.ArrayBuffer[Future[ByteBuffer]]
  
      while (index < block.length) {
        val remainder = block.length - index
        val chunk = Math.min(remainder, 50000)
        val to = index + chunk
        //log.info(s"PulseEnvelopes: index = $index, remainder = $remainder, chunk = chunck, to = $to")
        val data = java.util.Arrays.copyOfRange(block, index, to)
        val part = partCount
        
        val future = Future {
          val byteArrayOutputStream = new java.io.ByteArrayOutputStream()
          val objectOutputStream = new java.io.ObjectOutputStream(byteArrayOutputStream)
          objectOutputStream.writeObject(new BlockSegment(uuid, part, of, data))
          objectOutputStream.flush
          byteArrayOutputStream.flush
          val blob = java.nio.ByteBuffer.wrap(byteArrayOutputStream.toByteArray)
          val putRecordsRequestEntry  = new PutRecordsRequestEntry
          putRecordsRequestEntry.setData(blob)
          putRecordsRequestEntry.setPartitionKey(getKinesisPartitionKey)
          putRecordsRequestEntryQueue.add(putRecordsRequestEntry)
        }
        //log.info("PulseEnvelopes: data.length = " + data.length)
        index += chunk
        partCount += 1
      }
    }
  }
}