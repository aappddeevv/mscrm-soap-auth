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

object Metadata {

  private[this] implicit val logger = getLogger

  import scala.util.control.Exception._
  import scala.async.Async.{ async => sasync }
  import program._

  def apply(config: Config): Unit = {
    config.metadataAction match {
      case "dumpRawXml" =>
        import Defaults._
        println("Authenticating...")
        val header = GetHeaderOnline(config.username, config.password, config.url)
        println(s"Output metadata file: ${config.output}")
        println("Obtaining metadata...")
        val metadata = sasync {
          val h = await(header)
          val req = createPost(config) << retrieveAllEntitiesTemplate(h, config.objects).toString
          val m = await(Http(req OKWithBody as.xml.Elem).unwrapEx)
          config.output.toFile.printWriter(true).map(_.write(m.toString))
        } recover {
          case scala.util.control.NonFatal(ex) =>
            println("Exception obtaining metadata")
            println("Message: " + ex.getMessage)
            logger.error(ex)("Exception during processing")
        } andThen {
          case _ => Http.shutdown
        }
        catchTimeout("metadata") { Await.ready(metadata, config.timeout seconds) }

      case "generateDdl" =>
        val outputFile = "crm.ddl"
        println("Generating RDBMS schemas for CRM entities.")
        println(s"Output file: $outputFile")
        val filters =
          if (config.filterFilename.toFile.exists) {
            println(s"Using ${config.filterFilename} to restrict entities (logical name) selected for schema generation.")
            config.filterFilename.toFile.lines.map(_.trim).filterNot(_.isEmpty).map(new Regex(_))
          } else Seq()
        val ddl = outputFile.toFile
        ddl < "-- auto generated\n"

      case _ =>
        println("Unknown metadata action requested.")
    }
  }
}
