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
import sdk.metadata.readers._
import sdk._

object Test {

  import sdk.metadata._
  import sdk._

  private[this] implicit val logger = getLogger

  /**
   * Run some basic test.s
   */
  def apply(config: Config): Unit = {

    import Defaults._

    println("Get discovery auth and URL")
    val f1 = discoveryAuth(Http, config.username, config.password, config.region).
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
    val x = locationsToDiscoveryURL.get(config.region)
    x match {
      case Some(url) => println(s"Discovery URL for region ${config.region} is $url")
      case None => println(s"Unable to find discovery url for region abbrev ${config.region}")
    }
    val discoveryUrl = x.get

    println("Get org services auth and URL")
    val f2 = orgServicesAuth(Http, discAuth, config.username, config.password, config.url, config.region).
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

    import soapreaders._

    println("Get endpoints")
    val f3 = requestEndpoints(Http, discAuth).
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

    println("Get entity metadata")
    import sdk.metadata._
    import sdk.metadata.readers._
    val f4 = requestEntityMetadata(Http, orgSvcAuth).andThen { result =>
      result match {
        case Success(xor) => xor match {
          case Right(result) => println(s"# entities: ${result.entities.length}")
          case Left(err) => println(s"Error: $err")
        }
        case Failure(ex) => println(s"Failed: $ex")
      }
    }
    Await.ready(f4, 10 * config.timeout seconds)

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
  }
}
