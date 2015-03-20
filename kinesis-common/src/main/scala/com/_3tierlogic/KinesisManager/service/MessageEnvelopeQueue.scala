package com._3tierlogic.KinesisManager.service

import com._3tierlogic.KinesisManager.MessageEnvelope

import java.util.concurrent.ConcurrentLinkedQueue

/** Non-Blocking Queue for Ingesting Messages
  *  
  * @author Eric Kolotyluk
  * 
  * @see [[http://www.ibm.com/developerworks/library/j-jtp04186 Java theory and practice: Introduction to nonblocking algorithms]]
  */
object MessageEnvelopeQueue extends ConcurrentLinkedQueue[MessageEnvelope] {

}