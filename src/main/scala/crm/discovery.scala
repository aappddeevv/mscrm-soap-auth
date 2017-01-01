package crm

import scala.language._
import scala.util.control.Exception._
import scopt._
import org.w3c.dom._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.async.Async._
import scala.util._
import org.log4s._
import com.lucidchart.open.xtract.{ XmlReader, _ }
import XmlReader._
import play.api.libs.functional.syntax._
import cats._
import cats.data._
import cats.implicits._
import better.files._
import java.io.{ File => JFile }
import fs2._
import scala.concurrent.ExecutionContext
import sdk.CrmAuth._
import sdk.httphelpers._
import sdk.streamhelpers._
import scala.util.matching._
import sdk.soapnamespaces.implicits._
import sdk.messages.soaprequestwriters._
import sdk.soapreaders._
import sdk.metadata.xmlreaders._
import sdk._
import sdk.discovery._

object Discovery {
/*
  private[this] implicit val logger = getLogger

  import scala.util.control.Exception._

  /**
   * Run discovery commands.
   */
  def apply(config: Config): Unit = {

    println("Discovery mode")
    config.discoveryAction match {

      case "findServicesUrl" =>
        println(s"Find org services url for region abbrev ${config.region} and web app url ${config.url}")
        val output = discoveryAuth(Http, config.username, config.password, config.region).
          flatMap { discoveryAuth =>
            discoveryAuth match {
              case Right(auth) => orgServicesUrl(Http, auth, config.url)
              case Left(err) => Future.successful(Left(s"Error: $err"))
            }
          }.andThen {
            case Success(result) => result match {
              case Right(url) => println(s"Organization services URL: $url")
              case Left(err) => println(s"Error: $err")
            }
            case Failure(ex) => println(s"Failed: $ex")
          }
        Await.ready(output, config.timeout seconds)

      case "saveDiscoveryWsdl" =>
        println(s"Retrieving discovery service wsdl from ${config.url}")
        val http = httphelpers.client(config) // in case there are redirects
        val qp = Map("wsdl" -> null)
        val req = url(endpoint(config.url)) <<? qp
        val fut = http(req OKWithBody as.xml.Elem).unwrapEx.
          recover {
            case x: ApiHttpError =>
              println(s"Exception during WSDL retrieval. Response code is: ${x.code}")
              if (x.response.getResponseBody.trim.isEmpty) println("Response body is empty")
              else println(s"Response body: ${x.response.getResponseBody}")
              logger.error(x)("Exception during WSDL retrieval processing.")
              "Unable to obtain WSDL"
            case scala.util.control.NonFatal(ex) =>
              println("Exception during WSDL retrieval")
              logger.error(ex)("Exception during WSDL retrieval processing.")
              "Unable to obtain WSDL"
          }.
          andThen { case _ => http.shutdown }.
          andThen {
            case Success(wsdl) =>
              // Side affect is writing to a file.
              println("Obtained WSDL.")
              println(s"WSDL written to file ${config.wsdlFilename}")
              config.wsdlFilename.toFile < wsdl.toString
              logger.debug("WSDL: " + wsdl.toString)
          }
        catchTimeout("wsdl") { Await.ready(fut, config.timeout seconds) }

      case "saveOrgSvcWsdl" =>
        println(s"Retrieving organization service wsdl from ${config.url}")
        // https://msdn.microsoft.com/en-us/library/gg309401.aspx
        val qp = config.sdkVersion.map(v => Map("singleWsdl" -> null, "sdkversion" -> v)) getOrElse Map("wsdl" -> "wsdl0")
        val req = url(endpoint(config.url)) <<? qp
        logger.debug("WSDL request: " + req.toRequest)
        val http = httphelpers.client(config) // in case there are redirects
        val fut = http(req OKWithBody as.xml.Elem).unwrapEx.
          recover {
            case x: ApiHttpError =>
              println(s"Exception during WSDL retrieval. Response code is: ${x.code}")
              if (x.response.getResponseBody.trim.isEmpty) println("Response body is empty")
              else println(s"Response body: ${x.response.getResponseBody}")
              logger.error(x)("Exception during WSDL retrieval processing.")
              "Unable to obtain WSDL"
            case scala.util.control.NonFatal(ex) =>
              println("Exception during WSDL retrieval")
              logger.error(ex)("Exception during WSDL retrieval processing.")
              "Unable to obtain WSDL"
          }.
          andThen { case _ => http.shutdown }.
          andThen {
            case Success(wsdl) =>
              // Side affect is writing to a file.
              println("Obtained WSDL.")
              println(s"WSDL written to file ${config.wsdlFilename}")
              config.wsdlFilename.toFile < wsdl.toString
              logger.debug("WSDL: " + wsdl.toString)
          }
        catchTimeout("wsdl") { Await.ready(fut, config.timeout seconds) }

      case "listEndpoints" =>
        println("List valid enpdoints for a given region and user.")
        println(s"User           : ${config.username}")
        println(s"Password       : (not displayed)")
        println(s"Discovey Region: ${config.region}")

        def output(org: OrganizationDetail) = {
          s"Org     : ${org.friendlyName} (${org.uniqueName}, ${org.guid})\n" +
            s"State   : ${org.state}\n" +
            s"URL Name: ${org.urlName}\n" +
            s"Version : ${org.version}\n" +
            "End points:\n" +
            org.endpoints.map { e =>
              f"\t${e.name}%-32s: ${e.url}\n"
            }.mkString
        }

        import discovery.soapreaders._

        // Endpoints come from the discovery URL locaton, not the actual org of course.
        val endpoints = discoveryAuth(Http, config.username, config.password, config.region).
          flatMap { discoveryAuth =>
            discoveryAuth match {
              case Right(auth) =>
                val orgs = discovery.soapwriters.requestEndpoints(Http, auth).map { presult =>
                  val sb = new StringBuilder()
                  presult match {
                    case Right(orgs) =>
                      sb.append(s"# of endpoints: ${orgs.length}\n")
                      orgs.foreach(org => sb.append(output(org) + "\n"))
                    case Left(error) =>
                      sb.append(s"Errors parsing results: $error\n")
                  }
                  sb.toString
                }
                orgs
              case Left(err) => Future.successful(err)
            }
          }
        val printableOutput = Await.result(endpoints, config.timeout seconds)
        println(printableOutput)

      case "listRegions" =>
        println("Known discovery endpoints")
        regionToDiscoveryUrl.foreach {
          case (k, v) => println(f"$k%-10s: $v")
        }
      case _ =>
    }
  }
  */
}