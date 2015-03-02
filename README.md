# kinesis-manager
High level management service of Kinesis producer, consumer, and other resources

Kinesis Manager is to Kinesis what File Systems are to disk drives. The basic idea is that Kineis Manager offers a simple way to ingest messages at high speed as a Kineis Producer, and a simple way to archive as a Kineis Consumer. You do not need to know about the low level Kinesis API limitations such as the maximum message length, the number of messages pers second per shard, or even the nature of shards.

## Kinesis

### Constraints

* each message durable for up to 24 hours
* Producer Throughput: 1,000 message per second per shard
* Consumer Throughput: 4 messages per sedond per shard
* Maximum Message Size: 51,200 bytes
* bulk putRecordsRequest
  * 500 messages
  * 4.5 MB total

### Costs

* $0.015 per shard per hour
* $0.028 per 1,000,000 records

### Features
* can dynamically create and delete streams
* can dynamically add and subtract shards
* can get messages from any point in the buffer
* based on shard-id/sequence-number

### Warts
* complex low level API
* using more than one shard adds complexity
* abysmal documentation

## Kinesis Manager

### Features

An attempt to 
* make Kinesis easier to use
* simplify developer/user interface
  * no limit on logical message size
  * ignore the details of shards
* more cost effective
  * pack more logical messages per Kinesis message
  * reduce need for extra shards
* more performance effective
  * increase throughput of shorter logical messages
* archival to AWS S3
* ingestion from GNIP Power Stream and ...

### Limits

* unlimited logical message size
  * well, a lot bigger than 51200 bytes
* faster logical message rate
  * where application message size < 51200 bytes
  * slower where size > 51200 bytes

### Mechanics

* blocking: multiple logical messages packed into blocks
* segmentation: blocks segmented into physical messages
* pulse: time over which block is collected and flushed
* futures: used extensively for concurrent processing
* compression, serialization

### REST Endpoint
* curl http://localhost:<port>/ping
* pong
  * basic stay-alive
* curl --data “Hello World” http://localhost:<port>
  * basic POST request
* curl --data “Hello World” http://localhost:<port>?type=text
  * if type is not specified, default is “unknown”
* POST parameters: type, uuid, time, nano
  * no errors, if parameter is flawed/missing
  * reasonable default is used
  * warning result is returned

### GNIP
* Tightly coupled with Akka Actor System
* PowerTrack: Twitter et al
* Enterprise Data Collector: Instagram et al

### Miscellaneous
* REST API so that anything can stream
  * Useful for low volume messages because of archival
  * Event, Scraped, . . . whatever data you want to capture
* Remote Actors?

### Scalable for Cloud Applications
* Horizontally scalable
* Add more producers as necessary

### Archival to AWS S3
* raw data saved immediately
  * object names contain checkpoint information
    * eliminates dynamo loophole
  * segmented, blocked
  * potentially shard based
* post-processed to create logical message archive
  * raw data optionally removed

### Real Time API
* TBD

# Operation
## Service
* java -jar kinesis-manager.jar --producer
* java -jar kinesis-manager.jar --consumer
* java -jar kinesis-manager.jar --producer --consumer
* java -jar kinesis-manager.jar --producer --port=12345
* java -jar kinesis-manager.jar
  * prints out some basic information

## Configuration
* Environment Variables
* application.conf
* logback.xml

# Metrics
* Throughput
  * 10,000 logical records ingested in 50 ms
* Latency
  * 2.3 seconds for PutRecordsRequest, 20 records, 50K each
  * Likely less time than 20 PutRecordRequests
  * Certainly less time than 10,000 messages

# Development Environment

* Maven
  * 
* Java
  * 1.8
* Typesafe
  * Scala 2.11
  * Akka
* Eclipse Luna
  * Scala IDE 4.0
  * m2e connector for scala-maven-plugin

