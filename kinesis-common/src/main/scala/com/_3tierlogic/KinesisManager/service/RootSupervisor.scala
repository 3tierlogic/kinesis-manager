package com._3tierlogic.KinesisManager.service

import java.lang.management.ManagementFactory
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.seqAsJavaList
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import com._3tierlogic.KinesisManager.Configuration
import com._3tierlogic.KinesisManager.protocol.Start
import com._3tierlogic.KinesisManager.protocol.StartFailed
import com._3tierlogic.KinesisManager.protocol.Started
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Escalate
import akka.actor.SupervisorStrategy.Restart
import akka.actor.SupervisorStrategy.Resume
import akka.actor.SupervisorStrategy.Stop
import akka.actor.actorRef2Scala
import com.typesafe.config.ConfigRenderOptions

/** '''Root of the Actor Supervisors'''
  * 
  * This actor is started by the service kernel and is responsible for all the other actors in the system.
  * Not all of our actors are necessarily on the classpath, in particular this actor system is designed to
  * handle actors that are optionally included in the deployment assembly.
  * 
  * @author Eric Kolotyluk
  * 
  * @see [[http://spray.io/documentation/1.1-M8/spray-can/http-server/ Spray Can Documentation]]
  */
class RootSupervisor extends Actor with ActorLogging with Configuration {
  
  case class Wait(time: Long)
  
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: ArithmeticException => Resume
      case _: NullPointerException => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception => Escalate
    }
  
  lazy val date = new java.util.Date()
    
  val processId = ManagementFactory.getRuntimeMXBean().getName()
  
  log.info(s"running with process ID $processId")
  
  def actorRefForName(className: String) = try {
    //val actorClass = Class.forName(className, false, this.getClass.getClassLoader)
    val actorClass = Class.forName(className)
    log.info(s"actorRefForName: class $className found. This actor will be used")
    Some(context.actorOf(Props(actorClass), actorClass.getSimpleName))
  } catch {
    case classNotFoundException: ClassNotFoundException =>
      log.warning(s"actorRefForName: class $className not found. This actor will not be used")
      None
  }
  
  lazy val kinesisProducer = actorRefForName("com._3tierlogic.KinesisManager.producer.KinesisProducer")
  lazy val kinesisConsumer = actorRefForName("com._3tierlogic.KinesisManager.consumer.KinesisConsumer")
  
  var actorsStarted = false

  /** Novice Tips
    * 
    * All receive(d) messages originate from the actor's mailbox, a FIFO queue of messages.
    * Each actor is only ever active on one thread at a time, because the actor state is not
    * thread-safe. Safe concurrency is handled by having multiple actors or futures, where their
    * internal state is only accessible by one thread at a time.
    */
  def receive = {

    case Start =>
      
      val actorList = config.getList("kinesis-manager.actors").toList
      
      if (actorList.isEmpty()) {
        log.warning("config: kinesis-manager.actors is missing or empty! Shutting down now")
        context.system.shutdown()

      }
      
      actorList.foreach { configValue =>
        // Fucking Typesafe Config - everything has to be more complicated than necessary - EK
        // TODO - parse this as JSON to get a proper list of strings - EK
        if (configValue.valueType == com.typesafe.config.ConfigValueType.STRING) {
          val actorName   = configValue.unwrapped.asInstanceOf[String]
          val actorOption = actorRefForName(actorName)
          actorOption match {
            case Some(actorRef) =>
              log.info("Starting " + actorRef.path.name)
              actorRef ! Start
            case None =>
            log.info("There is no actor to start for " + actorName)
          }
        } else {
          log.error("Unknown CongifValueType '" + configValue.valueType + "' found for values at 'kinesis-manager.actors'")
        }
      }
     
      context.system.scheduler.scheduleOnce(40 seconds, self, Wait(System.currentTimeMillis))
            
//      kinesisProducer match {
//        case Some(kinesisProducerRef) =>
//          log.info("Starting " + kinesisProducerRef.path.name)
//          kinesisProducerRef ! Start
//        case None =>
//          log.info("-------------------> There is no Kinesis Producer actor to start.")
//      }

    case Started =>
      
      actorsStarted = true
      
      log.info(sender.path.name + " Started")
      
//      // foreach is a little confusing when there is only Some or None, but
//      // basically we can only use our actorRef if there is one. EK
//      kinesisProducer.foreach { kinesisProducerRef =>
//        if (sender.equals(kinesisProducerRef)) {
//          log.info(kinesisProducerRef.path.name + " Started")
//          log.info("Starting " + restEndpointRef.path.name)
//          IO(Http)(context.system) ! Http.Bind(restEndpointRef, "0.0.0.0", port = 8061)
//        }
//      }
//      
//      kinesisConsumer.foreach { kinesisConsumerRef =>
//        if (sender.equals(kinesisConsumerRef)) {
//          log.info(kinesisConsumerRef.path.name + " Started")
//        }
//      }


    case StartFailed(message: String) =>
      
      log.error(sender.path.name + " failed to start.")

//      
//      kinesisProducer.foreach { kinesisProducerRef =>
//        log.error(kinesisProducerRef.path.name + " failed to start.")
//        // TODO - don't shut down the system if there is a consumer - EK
//        log.error("Shutting down the system")
//        context.system.shutdown()
//      }
//      
//      kinesisConsumer.foreach { kinesisConsumerRef =>
//        if (sender.equals(kinesisConsumerRef)) {
//           log.error(kinesisConsumerRef.path.name + " failed to start.")
//        }
//      }
//
      
//    case ioResponse: Http.CommandFailed => 
//      
//      log.error("%s failed to bind to port 8061: ".format(restEndpointRef.path.name) + ioResponse.cmd.failureMessage)
//      
//    case ioResponse: Http.Bound =>
//      
//      log.info(restEndpointRef.path.name + " Started")
//      log.info("%s bound to port 8061".format(restEndpointRef.path.name))
//      
//      
//      kinesisConsumer match {
//        case Some(kinesisConsumerRef) =>
//          log.info("Starting " + kinesisConsumerRef.path.name)
//          kinesisConsumerRef ! Start
//        case None =>
//          log.info("There is no Kinesis Consumer actor to start.")
//      }
      
    case Wait(time) =>
      if (actorsStarted) {
        log.info("Actor system stable at " + time)
      } else {
        log.warning("No actors started. Shutting down now.")
        context.system.shutdown()
      }

    case message: Any =>
      log.error("Unknown message received: " + message)
  }

}

