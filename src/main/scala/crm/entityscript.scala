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
import sdk.SoapHelpers._
import sdk.StreamHelpers._
import scala.util.matching._
import sdk.SoapNamespaces.implicits._
import sdk.crmwriters._
import sdk.responseReaders._
import sdk.metadata.readers._
import sdk._

/** 
 *  Run a "script" derived from JSOn objects that create or modify or
 *  delete objects in CRM. Entity scripts make it easier to run tests
 *  in an automated way or, depending on the source of the script,
 *  migrate data. The JSON format is greatly simplified compared
 *  to XML or even to the newer CRM REST APIs.
 */
object EntityScript {

  import sdk.metadata._
  import sdk._

  private[this] implicit val logger = getLogger

  def apply(config: Config): Unit = {
    config.entityCommand match {
      case "runCommands" =>
      // Read command file and parse into json values

      // Find a create command

      case _ =>
        println("Unknown entity command.")
    }
  }
}
