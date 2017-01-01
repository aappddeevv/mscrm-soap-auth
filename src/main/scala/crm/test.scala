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
import cats.instances._
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
import sdk.metadata.soapwriters._
import sdk.discovery.soapwriters._

object Test {

  import sdk.metadata._
  import sdk._

  private[this] implicit val logger = getLogger

  /**
   * Run some basic test.s
   */
  def apply(config: Config): Unit = {

    //    println("Find discovery URL given a region")
    //    val x = regionToDiscoveryUrl.get(config.region)
    //    x match {
    //      case Some(url) => println(s"Discovery URL for region ${config.region} is $url")
    //      case None => println(s"Unable to find discovery url for region abbrev ${config.region}")
    //    }
    //    val discoveryUrl = x.get

    import scala.concurrent.ExecutionContext.Implicits.global
    import discovery.soapreaders._
    import crm.sdk.client._

    nonFatalCatch withApply { t =>
      println("Error occurred most likely creating discovery client:\n" + t)
    } apply {
      val dclient = DiscoveryCrmClient.fromConfig(config)
      println("Get endpoints")
      val endpointsTask = requestEndpoints(dclient)
      endpointsTask.unsafeAttemptRun.fold(
        t => println("Error obtaining enpdoints:\n${t}"),
        endpoints => endpoints.foreach(ep => println(s"Org: $ep")))
      dclient.shutdownNow()
    }

    //    println("Get entity metadata, this could take awhile...")
    //    import sdk.metadata._
    //    import sdk.metadata.xmlreaders._
    //    val f4 = requestEntityMetadata(http, orgSvcAuth).andThen { result =>
    //      result match {
    //        case Success(xor) => xor match {
    //          case Right(result) => println(s"# entities: ${result.entities.length}")
    //          case Left(err) => println(s"Error: $err")
    //        }
    //        case Failure(ex) => println(s"Failed: $ex")
    //      }
    //    }
    //    Await.ready(f4, 10 * config.timeout seconds)

    //    println("Find org services URL from web app url and region")
    //    val f5 = orgServicesUrl(Http, discAuth, config.url).andThen { result =>
    //      result match {
    //        case Success(xor) => xor match {
    //          case Right(result) => println(result)
    //          case Left(err) => println(s"Error: $err")
    //        }
    //        case Failure(ex) => println(s"Failed $ex")
    //      }
    //    }
    //    Await.ready(f5, config.timeout seconds)
    /*
  println("Testing WhoAmI across all orgs using discovery tokens!...this could take awhile")
 
    val whos: Future[Either[String, String]] =
      discoveryAuth(http, config.username, config.password, config.region).flatMap { ei =>
        ei match {
          case Right(auth) =>
            requestEndpoints(http, auth).flatMap { eps =>
              eps match {
                case Right(sod) =>
                  val r = sod.map { od =>
                    od.endpoints.find(_.name == "OrganizationDataService") match {
                      case Some(datasvc) =>
                        val who = Await.result(Auth.CrmWhoAmI(auth, datasvc.url), 120 seconds)
                        s"Issuing WhoAmI for ${datasvc.url} => $who"
                      case None => s"No org data service found for ${od.friendlyName}"
                    }
                  }
                  Future.successful(Right(r.mkString("\n")))
                case x@Left(msg) => Future.successful(Left(msg))
              }
            }
          case x@Left(msg) => Future.successful(Left(msg))
        }
      }
    val whosoutput = Await.result(whos, config.timeout seconds).getOrElse("No test message returned for the last test.")
    println(whosoutput)
 */

  }

}
