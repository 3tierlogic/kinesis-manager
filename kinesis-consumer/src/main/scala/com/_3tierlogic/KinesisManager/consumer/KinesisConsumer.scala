package com._3tierlogic.KinesisManager.consumer

import java.text.SimpleDateFormat;
import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef}
import com._3tierlogic.KinesisManager.Configuration
import com._3tierlogic.KinesisManager.MessageEnvelope
import com._3tierlogic.KinesisManager.BlockSegment
import com._3tierlogic.KinesisManager.protocol._
import com._3tierlogic.KinesisManager.service.StreamManager
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
  * 
  * @author Eric Kolotyluk
  * 
  * @see [[http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html Kinesis Introduction]]
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html AWS SDK for Java API Reference]]
  */
class KinesisConsumer extends Actor with ActorLogging with Configuration {
  
  val streamName = config.getString("amazon.web.service.kinesis.stream.name")
  
  val simpleYearMonthDay = new SimpleDateFormat("yyyy-MM-dd");

  case object Pulse
  case class Wait(time: Long)

  lazy val amazonKinesisClient = new AmazonKinesisClient
  lazy val amazonS3Client = new AmazonS3Client
  
  val bucketName   = "platform3.kinesis-manager"

  var startSender: ActorRef = null
  
  var describeStreamResponse: DescribeStreamResult = null
  
  val lastSequenceNumbers = scala.collection.mutable.Map[String,String]()
    
  def receive = {
    
    case Start =>
      
      startSender = sender
      
      sender ! Started
      
      StreamManager.actorRef ! OpenStream
      
      val bucketList   = amazonS3Client.listBuckets.toList
      val bucketOption = bucketList.find {bucket => bucket.getName == bucketName}
      val bucket = bucketOption match {
        case Some(bucket) => bucket
        case None =>
          log.info("creating bucket: " + bucketName)
          amazonS3Client.createBucket(bucketName)
      }
      
      log.info("using S3 bucket: " + bucket.getName)
      
    case StreamActive(kinesisEndpoint) =>
      amazonKinesisClient.setEndpoint(kinesisEndpoint)
        
      val describeStreamRequest = new DescribeStreamRequest()
      describeStreamRequest.setStreamName(streamName)

      describeStreamResponse = amazonKinesisClient.describeStream(describeStreamRequest)
      val streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus()
      log.info(s"Start: streamStatus = $streamStatus")

      // We try to recover the last sequence numbers of our shards from S3
      val shards = describeStreamResponse.getStreamDescription.getShards.toList
      shards.foreach { shard =>
        val shardId = shard.getShardId()
        lastSequenceNumber(shardId) match {
          case None =>
            log.info("No archived segments found in S3")
          case Some(lastSequenceNumber) =>
            log.info(s"Recovered last sequence number $lastSequenceNumber from S3")
            lastSequenceNumbers(shardId) = lastSequenceNumber
         }
      }

      context.system.scheduler.scheduleOnce(60 seconds, self, Pulse)
      
    case StreamActiveTimeout =>
      log.error("StreamActiveTimeout")

    case Pulse =>
      
      log.debug("Pulse: checking stream for messages")
      
      val shards = describeStreamResponse.getStreamDescription.getShards.toList
      shards.foreach { getRecords }
      
      context.system.scheduler.scheduleOnce(60 seconds, self, Pulse)
       
    case message: Any =>
      
      log.error("received unknown message = " + message)
  }
  
  def getRecords(shard: Shard) = {
    
    val shardId = shard.getShardId
    
    log.info("using shardId: " + shardId)
    
    val stringBuilder = new StringBuilder()
    
    val getShardIteratorRequest = new GetShardIteratorRequest()
    
    getShardIteratorRequest.setStreamName(streamName)
    getShardIteratorRequest.setShardId(shardId)
    
    if (lastSequenceNumbers.contains(shardId)) {
      val lastSequenceNumber = lastSequenceNumbers(shardId)
      getShardIteratorRequest.setStartingSequenceNumber(lastSequenceNumber)
      getShardIteratorRequest.setShardIteratorType("AFTER_SEQUENCE_NUMBER")
      log.info("shardIteratorType = " + getShardIteratorRequest.getShardIteratorType + " " + lastSequenceNumber)
    } else {
      getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON")
      log.info("shardIteratorType = " + getShardIteratorRequest.getShardIteratorType)
    }

    val getShardIteratorResult = amazonKinesisClient.getShardIterator(getShardIteratorRequest)
    val shardIterator = getShardIteratorResult.getShardIterator()
    val getRecordsRequest = new GetRecordsRequest()
    getRecordsRequest.setShardIterator(shardIterator)
    getRecordsRequest.setLimit(25)
      
    val getRecordsResult = amazonKinesisClient.getRecords(getRecordsRequest)
    val records = getRecordsResult.getRecords()
      
    log.info("getRecords: got " + records.size() + " records")
    
    val partMap = scala.collection.mutable.Map[UUID,scala.collection.mutable.Map[Long,BlockSegment]]()
    
    case class KinesisConsumerRecord(sequenceNumber: String, partitionKey: String, data: BlockSegment)

    val i = records.iterator()
    while (i.hasNext()) {
      val record = i.next()
      val partitionKey = record.getPartitionKey
      val lastSequenceNumber = record.getSequenceNumber
      log.info(s"getRecords: record = $record")
      val data = record.getData
      val f = new java.io.ByteArrayInputStream(data.array())
      val buffer = new java.io.ObjectInputStream(f)
      val blockSegment = buffer.readObject().asInstanceOf[BlockSegment]
      log.info(s"getRecords: uuid      = " + blockSegment.uuid)
      log.info(s"getRecords: part      = " + blockSegment.part)
      log.info(s"getRecords: of        = " + blockSegment.of)
      log.info(s"getRecords: data.size = " + blockSegment.data.size)
      //log.info(s"getRecords: data      = " + blockSegment.data)
      
      val kinesisConsumerRecord = KinesisConsumerRecord(lastSequenceNumber, partitionKey, blockSegment)
      
      val json = generate(kinesisConsumerRecord)
      
      // log.debug(json)
      
      stringBuilder.append(json).append('\n')
      if (stringBuilder.length > 1000000) {
        val data = stringBuilder.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val stream = new java.io.ByteArrayInputStream(data);
        val metadata = new com.amazonaws.services.s3.model.ObjectMetadata
        metadata.setContentLength(data.length)
        val date = simpleYearMonthDay.format(System.currentTimeMillis())
        val key = "segments/" + date + "/" + shard.getShardId + "/" + lastSequenceNumber
        amazonS3Client.putObject(bucketName, key, stream, metadata)
        lastSequenceNumbers(shardId) = lastSequenceNumber
        log.info("getRecords: writing S3 " + key)
        stringBuilder.clear
      }
        
      ///////////////////////////////////
        
      var partList: mutable.Map[Long, BlockSegment] = partMap.getOrElse(blockSegment.uuid, scala.collection.mutable.Map[Long,BlockSegment]())
        
      if (partList.size == 0) partMap(blockSegment.uuid) = partList
        
      partList(blockSegment.part) = blockSegment
        
      log.info("getRecords: partList.size = " + partList.size)
        
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
  
  /** '''Last Archived Sequence Number for a Shard'''
    *
    * Scan Amazon S3 for the archived messages, and get the sequence number of the
    * last messaged archived for this shard.
    */
  def lastSequenceNumber(shardId: String) = {
    val listObjectsRequest = new com.amazonaws.services.s3.model.ListObjectsRequest
    listObjectsRequest.setBucketName(bucketName)
    listObjectsRequest.setPrefix("segments/")
    listObjectsRequest.setDelimiter("/")
        
    var objectListing = amazonS3Client.listObjects(listObjectsRequest)
      
    var commonPrefixes = objectListing.getCommonPrefixes
      
    log.info("---------------------------------------------")
    commonPrefixes.foreach { prefix =>  
      log.info(prefix)
    }
    log.info("---------------------------------------------")
    
    if (commonPrefixes.isEmpty()) None
    else {
      val max = commonPrefixes.max
      
      log.info("max = " + max)
      
      //listObjectsRequest.setDelimiter(max + shardId + "/")
      val prefix = max + shardId + "/"
      listObjectsRequest.setPrefix(prefix)
      
      objectListing = amazonS3Client.listObjects(listObjectsRequest)
      val keys = objectListing.getObjectSummaries.toList.map {_.getKey }
      if (keys.isEmpty) None
      else {
        val lastSequenceNumber = keys.max.substring(prefix.length)

        log.info("---------------------------------------------")
        objectListing.getObjectSummaries.toList.foreach { objectSummary => log.info(objectSummary.getKey) }
        log.info("---------------------------------------------")
      
        Some(lastSequenceNumber)
      }
    }
  }
  
}