package platform3.gnip2kinesis

import org.joda.time.DateTime
//import platform3.gnip2kinesis.Event


object Sandbox {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  val partitionKey = Math.round(Math.random * 1024)
                                                  //> partitionKey  : Long = 182
  
  println("%04d".format(partitionKey))            //> 0182
  
  //val event = Event("Hello World", null, null, null, 0, null)
  
   // val event = Event("Hello World")
  
  
  new DateTime("2010-06-30T01:20+02:00")          //> res0: org.joda.time.DateTime = 2010-06-29T16:20:00.000-07:00
  
  DateTime.parse("2010-06-30T01:20+02:00")        //> res1: org.joda.time.DateTime = 2010-06-30T01:20:00.000+02:00
  
  new DateTime("2010-06-30T01:20")                //> res2: org.joda.time.DateTime = 2010-06-30T01:20:00.000-07:00
  
  DateTime.parse("2010-06-30T01:20")              //> res3: org.joda.time.DateTime = 2010-06-30T01:20:00.000-07:00
  
   new DateTime("2010-06-30T01:20Z")              //> res4: org.joda.time.DateTime = 2010-06-29T18:20:00.000-07:00
  
  DateTime.parse("2010-06-30T01:20Z")             //> res5: org.joda.time.DateTime = 2010-06-30T01:20:00.000Z
  
  
  new DateTime()                                  //> res6: org.joda.time.DateTime = 2015-02-18T15:39:50.744-08:00
  
  new DateTime("Foo Bar")                         //> java.lang.IllegalArgumentException: Invalid format: "Foo Bar"
                                                  //| 	at org.joda.time.format.DateTimeFormatter.parseMillis(DateTimeFormatter.
                                                  //| java:754)
                                                  //| 	at org.joda.time.convert.StringConverter.getInstantMillis(StringConverte
                                                  //| r.java:65)
                                                  //| 	at org.joda.time.base.BaseDateTime.<init>(BaseDateTime.java:171)
                                                  //| 	at org.joda.time.DateTime.<init>(DateTime.java:241)
                                                  //| 	at platform3.gnip2kinesis.Sandbox$$anonfun$main$1.apply$mcV$sp(platform3
                                                  //| .gnip2kinesis.Sandbox.scala:34)
                                                  //| 	at org.scalaide.worksheet.runtime.library.WorksheetSupport$$anonfun$$exe
                                                  //| cute$1.apply$mcV$sp(WorksheetSupport.scala:76)
                                                  //| 	at org.scalaide.worksheet.runtime.library.WorksheetSupport$.redirected(W
                                                  //| orksheetSupport.scala:65)
                                                  //| 	at org.scalaide.worksheet.runtime.library.WorksheetSupport$.$execute(Wor
                                                  //| ksheetSupport.scala:75)
                                                  //| 	at platform3.gnip2kinesis.Sandbox$.main(platform3.gnip2kinesis.Sandbox.s
                                                  //| cala:7)
                                                  //| 	at platform3.gnip2kinesis.Sandbox.main(platform3.gnip2kinesi
                                                  //| Output exceeds cutoff limit.
}