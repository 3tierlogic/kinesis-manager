package com._3tierlogic.KinesisManager.service

import java.lang.management.ManagementFactory

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
import akka.io.IO
import spray.can.Http

/** Root of the Actor Supervisors
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
    Some(context.actorOf(Props(actorClass), actorClass.getSimpleName))
  } catch {
    case classNotFoundException: ClassNotFoundException =>
      log.info(s"class $className not found. This actor will not be used")
      None
  }
  
  lazy val restEndpointRef = context.actorOf(Props[RestEndpoint], "RestEndpoint")
  lazy val kinesisProducer = actorRefForName("com._3tierlogic.KinesisManager.producer.KinesisProducer")
  lazy val kinesisConsumer = actorRefForName("com._3tierlogic.KinesisManager.consumer.KinesisConsumer")

  /** Novice Tips
    * 
    * All received messages originate from the actor's mailbox and always on the same thread. Actors are
    * not designed to be threadsafe. Concurrency is handled by having multiple actors, with protected state,
    * or futures, with no state.
    */
  def receive = {

    case Start =>
            
      kinesisProducer match {
        case Some(kinesisProducerRef) =>
          log.info("Starting " + kinesisProducerRef.path.name)
          kinesisProducerRef ! Start
        case None =>
          log.info("There is no Kinesis Producer actor to start.")
      }

    case Started =>
      
      // foreach is a little confusing when there is only Some or None, but
      // basically we can only use our actorRef if there is one. EK
      kinesisProducer.foreach { kinesisProducerRef =>
        if (sender.equals(kinesisProducerRef)) {
          log.info(kinesisProducerRef.path.name + " Started")
          log.info("Starting " + restEndpointRef.path.name)
          IO(Http)(context.system) ! Http.Bind(restEndpointRef, "0.0.0.0", port = 8061)
        }
      }
      
      kinesisConsumer.foreach { kinesisConsumerRef =>
        if (sender.equals(kinesisConsumerRef)) {
          log.info(kinesisConsumerRef.path.name + " Started")
        }
      }


    case StartFailed(message: String) =>
      
      kinesisProducer.foreach { kinesisProducerRef =>
        log.error(kinesisProducerRef.path.name + " failed to start.")
        // TODO - don't shut down the system if there is a consumer - EK
        log.error("Shutting down the system")
        context.system.shutdown()
      }
      
      kinesisConsumer.foreach { kinesisConsumerRef =>
        if (sender.equals(kinesisConsumerRef)) {
           log.error(kinesisConsumerRef.path.name + " failed to start.")
        }
      }

      
    case ioResponse: Http.CommandFailed => 
      
      log.error("%s failed to bind to port 8061: ".format(restEndpointRef.path.name) + ioResponse.cmd.failureMessage)
      
    case ioResponse: Http.Bound =>
      
      log.info(restEndpointRef.path.name + " Started")
      log.info("%s bound to port 8061".format(restEndpointRef.path.name))
      
      
      kinesisConsumer match {
        case Some(kinesisConsumerRef) =>
          log.info("Starting " + kinesisConsumerRef.path.name)
          kinesisConsumerRef ! Start
        case None =>
          log.info("There is no Kinesis Consumer actor to start.")
      }

  }

}

