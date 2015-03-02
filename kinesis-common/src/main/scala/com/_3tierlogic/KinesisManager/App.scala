package com._3tierlogic.KinesisManager

/**
 * Hello world!
 *
 */
object App extends App with Configuration with Environment {
  println( "Hello World!" )
  
  val configuration = config
}
