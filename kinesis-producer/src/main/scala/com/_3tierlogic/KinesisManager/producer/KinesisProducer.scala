package com._3tierlogic.KinesisManager.producer

import java.io.ObjectOutputStream
import java.nio.ByteBuffer
import java.util.{ArrayList, UUID}
import java.util.concurrent.ConcurrentLinkedQueue


import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import akka.actor.Actor
import akka.actor.Props
import akka.io.IO

import com._3tierlogic.KinesisManager.{Configuration, MessageEnvelope, BlockSegment}
import com._3tierlogic.KinesisManager.protocol._
import com._3tierlogic.KinesisManager.service.MessageEnvelopeQueue
import com._3tierlogic.KinesisManager.service.StreamManager
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.{CreateStreamRequest, DescribeStreamRequest, PutRecordsRequest, PutRecordsRequestEntry, ResourceNotFoundException}

import scala.collection.immutable.StringOps
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import spray.can.Http

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
  case object PulseEnvelopes
  case object PulseRecords
  
  case class PutRecords(records: scala.collection.mutable.ArrayBuffer[Future[ByteBuffer]])
  
  lazy val streamName = config.getString("amazon.web.service.kinesis.stream.name")
  
  lazy val maximumBlockSize =
    if (config.hasPath("kinesis-manager.producer.block.size.maximum")) config.getInt("kinesis-manager.producer.block.size.maximum")
    else Integer.MAX_VALUE - 1024

  val describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName)
        
  val restEndpointPort = config.getInt("kinesis-manager.producer.endpoint.port")

  val restEndpointRef = context.system.actorOf(Props[RestEndpoint], "kinesis-manager-endpoint")

  val amazonKinesisClient = new AmazonKinesisClient
  
  //def getClient = client
  
  var startSender: ActorRef = null
  
  var streamCreationTime: Long = 0
  var streamCreationTimeLimit: Long = 0

  /**
   * http://www.ibm.com/developerworks/library/j-jtp04186/
   */
  val putRecordsRequestEntryQueue = new ConcurrentLinkedQueue[PutRecordsRequestEntry]
  
  var pulseEnvelopesFuture = Future {}
  var pulseRecordsFuture = Future {}
  
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
      
      log.info("Start: signalling %s we have Started".format(startSender.path.name))
      startSender ! Started
      
      StreamManager.actorRef ! OpenOrCreateStream

    case StreamActive(kinesisEndpoint) =>
      amazonKinesisClient.setEndpoint(kinesisEndpoint)
      streamActive
      
      log.info("Starting " + restEndpointRef.path.name)
      IO(Http)(context.system) ! Http.Bind(restEndpointRef, "0.0.0.0", port = restEndpointPort)
      
    case StreamActiveTimeout =>
      log.error("StreamActiveTimeout")
      
    case ioResponse: Http.Bound =>

      log.info(restEndpointRef.path.name + " Started")
      log.info("%s bound to port %s".format(restEndpointRef.path.name, restEndpointPort))
      
    case ioResponse: Http.CommandFailed => 
      
      log.error("%s failed to bind to port 8061: ".format(restEndpointRef.path.name) + ioResponse.cmd.failureMessage)

    case Put(messageEnvelope) => MessageEnvelopeQueue.add(messageEnvelope)
    
    case PulseEnvelopes =>
    
      // We skip this pulse if the previous one is not finished, or the queue is empty
      if (pulseEnvelopesFuture.isCompleted && ! MessageEnvelopeQueue.isEmpty()) {
       pulseEnvelopesFuture = Future {pulseEnvelopes}
      }

    case PulseRecords => 
    
      // We skip this pulse if the previous one is not finished, or the queue is empty
      if (pulseRecordsFuture.isCompleted && ! putRecordsRequestEntryQueue.isEmpty()) {
       pulseRecordsFuture = Future {pulseRecords}
      }

    case message: Any =>
       // Uncaught messages get propagated so we need to catch any strays here.

       log.error("received unknown message = " + message)
  }
  
  
  def streamActive = {
    context.system.scheduler.schedule(1 seconds, 1 seconds, self, PulseEnvelopes)
    context.system.scheduler.schedule(1 seconds, 1 seconds, self, PulseRecords)
    
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
  }
  
  class BetterByteArrayOutputStream extends java.io.ByteArrayOutputStream {
    def getCount = count
  }
  
  /* In response to a pulse, drain the MessageEvenlopeQueue
   * 
   * This runs in the background via a Future so our actor can
   * return to processing in-box messages again.
   */
  def pulseEnvelopes = {
        
    val byteArrayOutputStream = new BetterByteArrayOutputStream()
    val objectOutputStream = new java.io.ObjectOutputStream(byteArrayOutputStream)

    val timestamp = System.currentTimeMillis
    var count = 0
    
    while (! MessageEnvelopeQueue.isEmpty) {
      // TODO - we could handle buffer overflows better - EK
      if (byteArrayOutputStream.getCount > maximumBlockSize) {
        flushBlock(byteArrayOutputStream, objectOutputStream)
        byteArrayOutputStream.reset
        objectOutputStream.reset
      } else {
        objectOutputStream.writeObject(MessageEnvelopeQueue.poll)
        count += 1
      }
    }
    flushBlock(byteArrayOutputStream, objectOutputStream)
  }
  
  /* Flush the currently message block to our segment stream.
   * 
   */
  def flushBlock(byteArrayOutputStream: BetterByteArrayOutputStream, objectOutputStream: ObjectOutputStream) = {
    
    // Now that we have a block, we segment it, and do a bulk put to Kinesis

    //val drainTime = System.currentTimeMillis - testStartTime
    //log.info(s"PulseEnvelopes: $count envelopes drained in $drainTime ms")
    
    objectOutputStream.flush
    byteArrayOutputStream.flush

    // This needs to be done before creating our future, as it captures the
    // ByteArray to be segmented into physical Kinesis messages. This needs
    // to be a val so the future can capture the current value
    val block = byteArrayOutputStream.toByteArray
    
    val blockFuture = Future {
      
      // var blockCount = 0
      var index = 0
      var partCount = 1L
      val of: Long = block.length / 50000L + 1
      log.info(s"PulseEnvelopes: of = $of")
      
      val uuid = UUID.randomUUID
      //val blobBuffer = new scala.collection.mutable.ArrayBuffer[Future[ByteBuffer]]
  
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
  
  /* Push Block Segments to Kinesis as Kinesis Records
   * 
   */
  def pulseRecords = {
    
    val putRecordsRequestEntryList  = new ArrayList[PutRecordsRequestEntry]

    def putRecords = {
      try {
        log.debug("***************************************")
        val putRecordsRequest = new PutRecordsRequest()
          .withStreamName(streamName)
          .withRecords(putRecordsRequestEntryList)
  
        val putTime = System.currentTimeMillis
        val putRecordsResult  = amazonKinesisClient.putRecords(putRecordsRequest)
        log.info("putTime = " + (System.currentTimeMillis - putTime))
        log.info("pulseRecords: put " + putRecordsResult.getRecords.size() + " blocks in " + (System.currentTimeMillis - putTime) + " ms")
        if (putRecordsResult.getFailedRecordCount > 0) log.error(putRecordsResult.getFailedRecordCount + " records failed")
        //log.info(s"pulseRecords: putRecordsResult = $putRecordsResult")
        putRecordsRequestEntryList.clear
        log.debug("########################################")
      } catch {
        case exception: Exception =>
          log.error(exception, exception.getMessage)
      }
    }

    var count = 0
    var size = 0
//    
//    if (MessageEnvelopeQueue.isEmpty && putRecordsRequestEntryQueue.isEmpty && testStartTime > 0) {
//      val blockQueueEmpty = System.currentTimeMillis - testStartTime
//      log.info(s"PulseBlocks: block queue empty in $blockQueueEmpty ms")
//      testStartTime = 0
//    }
  
    // Drain the queue
    while (! putRecordsRequestEntryQueue.isEmpty()) {
      val putRecordsRequestEntry = putRecordsRequestEntryQueue.poll
      putRecordsRequestEntryList.add(putRecordsRequestEntry)
      count += 1
      size  += putRecordsRequestEntry.getData.array.length
      
      // If we are at the limits of a putRecordsRequest, flush it
      if (count == 500 || size > 4500000) {
         log.info(s"putRecords: count = $count, size = $size")
         putRecords
         count = 0
         size = 0
      }
    }
  
    if (! putRecordsRequestEntryList.isEmpty) putRecords
      
  }
}