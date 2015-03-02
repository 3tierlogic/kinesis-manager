package com._3tierlogic.KinesisManager

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.event.Logging
import akka.pattern.ask

// import com.codahale.jerkson.Json._

import spray.routing.HttpService
import spray.routing.RequestContext
import spray.routing.PathMatchers.Segment
//import spray.can.server.Stats
//import spray.can.Http
import spray.httpx.marshalling.Marshaller
import spray.httpx.encoding.Gzip
import spray.util._
import spray.http._
import spray.http.StatusCodes._
import MediaTypes._

import java.time.Instant

import scala.collection.mutable.MutableList

/** REST End-Point Routing Actor
  * 
  * =Examples/Tests=
  * {{{
  *   curl http://localhost:<port>
  *   curl --data "hello world" http://localhost:<port>
  *   curl http://localhost:<port>/ping
  *   curl -v --data "hello world" "http://localhost:<port>?uuid=hello&nano=foo&time=bar"
  * }}}
  * 
  * @see [[http://spray.io/documentation/1.2.2/spray-routing Spray Routing]]
  * @see [[http://spray.io/documentation/1.1-SNAPSHOT/api/index.html#spray.routing.HttpService Spray HttpService]]
  */
class RestEndpoint extends Actor with ActorLogging with HttpService {
  
  val html =
    <html>
      <body>
        <h1>3TL Service Up and Running</h1>
        <p>Currently in Production mode</p>
        <ul>
          <li><a href="/ping">/ping</a></li>
        </ul>
      </body>
    </html>
  
  val route_index = {
    path("") {
      pathEnd {
        get {
          log.info("pathEnd")
          // XML is marshalled to `text/xml` by default, so we simply override here
          respondWithMediaType(`text/html`) { 
            complete(html)
          }
        } ~
        post {
          entity(as[String]) { body =>
            log.info(s"body = $body")
            parameterMap { parameters =>
              val ACCEPTED = 202
              complete(HttpResponse(ACCEPTED, parameters.mkString("\n", "\n", "\n\n")))
//              val event = KinesisEventEnvelope(body, parameters)
//              val warnings = event.getWarnings
//              val json = event.getJson
//              log.info(s"json = $json")
//              if (warnings.isEmpty) {
//                complete(HttpResponse(OK,"ok"))
//              } else {
//                warnings.foreach(log.warning)
//                val ACCEPTED = 202
//                complete(HttpResponse(ACCEPTED, warnings.mkString("\n", "\n", "\n\n")))
//              }
            }
          }
        }
      }
    }
  }
  
  /** Keep Alive Polling
    * 
    * Used by Monit and other monitoring services to check on the health of the service/end-point
    * 
    * @see [[http://mmonit.com Monit]]
    */
  val route_ping = {
    path("ping") {
      get {
        complete(HttpResponse(OK,"pong"))
      }
    }
  }
  
  /** actorRefFactory proxy
    * 
    * Just give HttpService our current context
    */
  override def actorRefFactory = context

  /** receive proxy
    *  
    * HttpService can proxy our actor's receive method
    * 
    * The ~ operator (or) is used to link together alternate routes to match.
    *  
    * @see [[http://spray.io/documentation/1.1-SNAPSHOT/api/index.html#spray.routing.HttpService runRoute]]
    */
  override def receive = runRoute(
    route_index ~
    route_ping
  )

}

