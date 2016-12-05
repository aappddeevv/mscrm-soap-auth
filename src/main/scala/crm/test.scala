package crm

import scala.language._
import scala.util.control.Exception._
import scopt._
import org.w3c.dom._
import dispatch._
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

    import Defaults._

    val http = httphelpers.client(120)

    println("Get discovery auth and URL")
    val f1 = discoveryAuth(http, config.username, config.password, config.region).
      andThen { result =>
        result match {
          case Success(xor) => xor match {
            case Right(result) => println(s"Result: $result")
            case Left(err) => println(s"Error: $err")
          }
          case Failure(ex) => println(s"Failed: $ex")
        }
      }
    val discAuth = Await.result(f1, config.timeout seconds).getOrElse(CrmAuthenticationHeader())

    println("Find discovery URL given a region")
    val x = regionToDiscoveryUrl.get(config.region)
    x match {
      case Some(url) => println(s"Discovery URL for region ${config.region} is $url")
      case None => println(s"Unable to find discovery url for region abbrev ${config.region}")
    }
    val discoveryUrl = x.get

    println("Get org services auth and URL")
    val f2 = orgServicesAuth(http, discAuth, config.username, config.password, config.url, config.region).
      andThen { result =>
        result match {
          case Success(xor) => xor match {
            case Right(result) => println(s"Result: $result")
            case Left(err) => println(s"Error: $err")
          }
          case Failure(ex) => println(s"Failed: $ex")
        }
      }
    val orgSvcAuth = Await.result(f2, config.timeout seconds).getOrElse(CrmAuthenticationHeader())

    import discovery.soapreaders._

    println("Get endpoints")
    val f3 = requestEndpoints(http, discAuth).
      andThen { result =>
        result match {
          case Success(xor) => xor match {
            case Right(endpoints) => endpoints.foreach { ep =>
              println(s"Org: $ep")
            }
            case Left(err) => println(s"Error: $err")
          }
          case Failure(ex) => println(s"Failed: $ex")
        }
      }
    Await.ready(f3, config.timeout seconds)

    /*
    println("Get entity metadata, this could take awhile...")
    import sdk.metadata._
    import sdk.metadata.xmlreaders._
    val f4 = requestEntityMetadata(http, orgSvcAuth).andThen { result =>
      result match {
        case Success(xor) => xor match {
          case Right(result) => println(s"# entities: ${result.entities.length}")
          case Left(err) => println(s"Error: $err")
        }
        case Failure(ex) => println(s"Failed: $ex")
      }
    }
    Await.ready(f4, 10 * config.timeout seconds)
*/

    println("Find org services URL from web app url and region")
    val f5 = orgServicesUrl(Http, discAuth, config.url).andThen { result =>
      result match {
        case Success(xor) => xor match {
          case Right(result) => println(result)
          case Left(err) => println(s"Error: $err")
        }
        case Failure(ex) => println(s"Failed $ex")
      }
    }
    Await.ready(f5, config.timeout seconds)
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

    println("Testing dispatch client.")
    import sdk.driver._
    import sdk.messages._

    val client = DispatchClient.fromConfig(config)
    val req = WhoAmIRequest()
    val fut = client.execute(req) //.map { e =>
    //      e match {
    //        case Right(eresult) =>
    //          "done"
    //        case Left(err) =>
    //          s"Error: $toUserMessage(err)}"
    //      }
    //    }
    val who = fut.value.unsafeRun()
    who match { 
      case Right(r) =>
            println(s"""guid who: ${r.results.getAs[TypedServerValue]("UserId").map(_.text).getOrElse("no guid user id found")}""")
      case Left(err) =>
        println(s"Error: ${toUserMessage(err)}")
    }
    client.shutdownNow()
    
    http.shutdown
    Http.shutdown
  }

}
