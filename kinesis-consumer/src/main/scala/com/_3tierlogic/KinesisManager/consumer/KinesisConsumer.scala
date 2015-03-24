package com._3tierlogic.KinesisManager.consumer

import java.io.StringBufferInputStream
import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef}
import com._3tierlogic.KinesisManager.{Configuration, MessageEnvelope, BlockSegment}
import com._3tierlogic.KinesisManager.protocol.Start
import com.codahale.jerkson.Json._
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.{DescribeStreamRequest, DescribeStreamResult, GetRecordsRequest, GetShardIteratorRequest, Shard}
import com.amazonaws.services.s3.AmazonS3Client

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/** Actor for Managing Kinesis Consumer Activity
  * 
  * This actor attempts to make it easier to cons
  * @author Eric Kolotyluk
  * 
  * @see [[http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html Kinesis Introduction]]
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html AWS SDK for Java API Reference]]
  */
class KinesisConsumer extends Actor with ActorLogging with Configuration {
  
  val streamName = config.getString("amazon.web.service.kinesis.stream.name")

  case object Pulse
  case class Wait(time: Long)

  val amazonKinesisClient = new AmazonKinesisClient
  val amazonS3Client = new AmazonS3Client
  
  val bucketName   = "platform3.kinesis-manager"

  
  var startSender: ActorRef = null
  
  var describeStreamResponse: DescribeStreamResult = null
  
  var lastSequenceNumber: String = null
  
  def receive = {
    
    case Start =>
      
      startSender = sender
      
      amazonKinesisClient.setEndpoint("kinesis.us-west-2.amazonaws.com", "kinesis", "us-west-2")
      log.info("Start: AWS endpoint = kinesis.us-west-2.amazonaws.com, kinesis, us-west-2")

      context.system.scheduler.scheduleOnce(40 seconds, self, Wait(System.currentTimeMillis))
      
      val bucketList   = amazonS3Client.listBuckets.toList
      val bucketOption = bucketList.find {bucket => bucket.getName == bucketName}
      val bucket = bucketOption match {
        case Some(bucket) => bucket
        case None =>
          log.info("creating bucket: " + bucketName)
          amazonS3Client.createBucket(bucketName)
      }
      
      log.info("using S3 bucket: " + bucket.getName)
      
      //val bucketsLog = buckets.mkString("\n<buckets>\n  ", "\n  ", "\n</buckets")
      //log.info(bucketsLog)
      
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
          
        val describeStreamRequest = new DescribeStreamRequest()
        describeStreamRequest.setStreamName(streamName)
  
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
      
      val shards = describeStreamResponse.getStreamDescription.getShards.toList
      shards.foreach { getRecords }
      
      context.system.scheduler.scheduleOnce(10 seconds, self, Pulse)
       
    case message: Any =>
      
      log.error("received unknown message = " + message)

  }
  
  def getRecords(shard: Shard) = {
    log.info("using shardId: " + shard.getShardId)
    
    val stringBuilder = new StringBuilder()
    
    val getShardIteratorRequest = new GetShardIteratorRequest()
    
    getShardIteratorRequest.setStreamName(streamName)
    getShardIteratorRequest.setShardId(shard.getShardId())
    
    if (lastSequenceNumber == null)
      getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON")
    else {
      getShardIteratorRequest.setStartingSequenceNumber(lastSequenceNumber)
      getShardIteratorRequest.setShardIteratorType("AFTER_SEQUENCE_NUMBER")
    }

    val getShardIteratorResult = amazonKinesisClient.getShardIterator(getShardIteratorRequest)
    val shardIterator = getShardIteratorResult.getShardIterator()
    val getRecordsRequest = new GetRecordsRequest()
    getRecordsRequest.setShardIterator(shardIterator)
    getRecordsRequest.setLimit(25)
      
    val getRecordsResult = amazonKinesisClient.getRecords(getRecordsRequest)
    val records = getRecordsResult.getRecords()
      
    log.info("Start: got " + records.size() + " records")
    
    val partMap = scala.collection.mutable.Map[UUID,scala.collection.mutable.Map[Long,BlockSegment]]()
    
    case class KinesisConsumerRecord(sequenceNumber: String, partitionKey: String, data: BlockSegment)

    val i = records.iterator()
    while (i.hasNext()) {
      val record = i.next()
      val partitionKey = record.getPartitionKey
      lastSequenceNumber = record.getSequenceNumber
      log.info(s"Start: record = $record")
      val data = record.getData
      val f = new java.io.ByteArrayInputStream(data.array())
      val buffer = new java.io.ObjectInputStream(f)
      val blockSegment = buffer.readObject().asInstanceOf[BlockSegment]
      log.info(s"Start: uuid      = " + blockSegment.uuid)
      log.info(s"Start: part      = " + blockSegment.part)
      log.info(s"Start: of        = " + blockSegment.of)
      log.info(s"Start: data.size = " + blockSegment.data.size)
      log.info(s"Start: data      = " + blockSegment.data)
      
      val kinesisConsumerRecord = KinesisConsumerRecord(lastSequenceNumber, partitionKey, blockSegment)
      
      val json = generate(kinesisConsumerRecord)
      
      log.debug(json)
      
      stringBuilder.append(json).append('\n')
      if (stringBuilder.length > 640000) {
        val stream = new java.io.ByteArrayInputStream(stringBuilder.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        val metadata = new com.amazonaws.services.s3.model.ObjectMetadata
        val key = shard.getShardId + "/" + lastSequenceNumber
        amazonS3Client.putObject(bucketName, key, stream, metadata)
        log.info("writing S3 " + key)
        stringBuilder.clear
      }
        
      ///////////////////////////////////
        
      var partList: mutable.Map[Long, BlockSegment] = partMap.getOrElse(blockSegment.uuid, scala.collection.mutable.Map[Long,BlockSegment]())
        
      if (partList.size == 0) partMap(blockSegment.uuid) = partList
        
      partList(blockSegment.part) = blockSegment
        
      log.info("Start: partList.size = " + partList.size)
        
      if (partList.size == blockSegment.of) {
        val arrayBuffer = new scala.collection.mutable.ArrayBuffer[Byte]
        for (i <- 1L to blockSegment.of) {
          val data = partList(i).data
          log.info("Start: eventPart.data.length = " + data.length)
          arrayBuffer.appendAll(data)
        }
          
        log.info("Start: deserializing " + blockSegment.uuid)
          
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

  }
  
}