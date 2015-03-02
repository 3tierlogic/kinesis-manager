package com._3tierlogic.KinesisManager

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props


import com._3tierlogic.KinesisManager.protocol._

/** Akka Service Kernel Bootable
  * 
  * @author Eric Kolotyluk
  * 
  * @see [[http://doc.akka.io/docs/akka/snapshot/general/configuration.html Akka Configuration]]
  */
class Service extends akka.kernel.Bootable with Configuration {
  
  // ActorSystem is a heavy object: create only one per application
  // Don't let Akka handle configuration because we're doing it.
  // See the Configuration trait for more details. EK
  lazy val actorSystem = ActorSystem("KinesisManager", config)
  
  // Create the root actor of our system, that supervises all the others.
  lazy val rootSupervisor = actorSystem.actorOf(Props[RootSupervisor], "RootSupervisor")


  def startup = {
    rootSupervisor ! Start
  }

  def shutdown = {
    actorSystem.shutdown()
  }
}

/**
 * boot object for command line runtime
 */
object Service extends App {
  
  (new Service).startup()
    
}

