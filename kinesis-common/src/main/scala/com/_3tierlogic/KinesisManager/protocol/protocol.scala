package com._3tierlogic.KinesisManager

package object protocol {
  
  
case class ApplicationArguments(arguments: Array[String])
  
/** Start an Actor
 *  
 *  Generic message to tell an actor to start after being constructed.
 *  
 *  Constructing an actor should not consume a lot of resources, and construction should
 *  rely on lazy members because we may decide never to start an actor.
 */
case object Start
case object Started
case class StartFailed(message: String)

// StreamManager

case object OpenOrCreateStream
case object OpenStream
case object StreamActive
case object StreamActiveTimeout

// KinesisProducer

case class Put(messageEnvelope: MessageEnvelope)


}