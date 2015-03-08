# kinesis-manager
High level management service of Kinesis producer, consumer, and other resources

Kinesis Manager is to Kinesis what File Systems are to disk drives. The basic idea is that Kineis Manager offers a simple way to ingest messages at high speed as a Kinesis Producer, and a simple way to archive as a Kineis Consumer. You do not need to know about the low level Kinesis API limitations such as the maximum message length, the number of messages per second per shard, or even the nature of shards.

See also the wiki

# Development Environment

* Maven
  * 3.2.3
  * The [maven-wrapper]((https://github.com/bdemers/maven-wrapper) is included
* Java
  * 1.8
* Scala
  * 2.11
* Typesafe
  * Akka
* Eclipse Luna
  * Scala IDE 4.0
  * m2e connector for scala-maven-plugin
