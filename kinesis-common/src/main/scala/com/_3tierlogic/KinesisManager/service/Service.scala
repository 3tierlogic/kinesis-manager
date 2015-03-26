package com._3tierlogic.KinesisManager.service

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.kernel.Bootable

import com._3tierlogic.KinesisManager.Configuration
import com._3tierlogic.KinesisManager.LogbackLogging
import com._3tierlogic.KinesisManager.protocol._

/** Akka Service Kernel Bootable
  * 
  * @author Eric Kolotyluk
  * 
  * @see [[http://doc.akka.io/docs/akka/snapshot/general/actor-systems.html Actor Systems]]
  * @see [[http://doc.akka.io/docs/akka/snapshot/general/configuration.html Akka Configuration]]
  */
class Service extends Bootable with Configuration with LogbackLogging {

  def startup = {
    Service.rootSupervisor ! Start
  }

  def shutdown = {
    Service.actorSystem.shutdown()
  }
}

/**
 * boot object for command line runtime
 */
object Service extends App with Configuration with LogbackLogging {
  
  // ActorSystem is a heavy object: create only one per application
  // Don't let Akka handle configuration because we're doing it.
  // See the Configuration trait for more details. EK
  lazy val actorSystem = ActorSystem("KinesisManager", config)
  
  // Create the root actor of our system, that supervises all the others.
  lazy val rootSupervisor = actorSystem.actorOf(Props[RootSupervisor], "RootSupervisor")

  
  if (args.isEmpty) {
    println("No application arguments were specified!\nuse ? for a list of arguments.")
    System.exit(0)
  }
  
  // TODO - this is problematic - EK
  if (args.contains("?")) {
    explainArguments
    System.exit(0)
  }
  
  val argsLog = args.mkString("\n<arguments>\n  ", "\n  ", "\n</arguments>")
  
  logger.info(argsLog)
  
  val service = new Service
    
  rootSupervisor ! ApplicationArguments(args)

  service.startup
  
  def explainArguments = {
    println("?       - explains the possible arguments")
    println("install - installs start scripts and configuration files")
  }
    
}

