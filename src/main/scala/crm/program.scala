package crm

import scala.language._
import scala.util.control.Exception._

import scopt._
import org.w3c.dom._
import dispatch._, Defaults._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.async.Async._
import com.typesafe.scalalogging._
import scala.util._
import java.nio.file._

case class Config(
  help: Boolean = false,
  url: String = "",
  username: String = "",
  password: String = "",
  timeout: Int = 60,
  getMetadata: Boolean = false,
  filterFilename: String = "filters.txt",
  objects: String = "Entity Relationships Attributes",
  output: String = "metadata.xml")

object program extends CrmAuth with SoapHelpers with LazyLogging {

  val parser = new scopt.OptionParser[Config]("crmauth") {
    override def showUsageOnError = true
    head("crmauth", "0.1.0")
    opt[String]('r', "url").required().valueName("<url>").text("Organization service url.")
      .action((x, c) => c.copy(url = x))
    opt[String]('u', "userid").required().valueName("<username>").text("Userid")
      .action((x, c) => c.copy(username = x))
    opt[String]('p', "password").required().valueName("<password>").text("Password")
      .action((x, c) => c.copy(password = x))
    opt[Int]('t', "timeout").valueName("<number>").text("Timeout in seconds for each request.")
      .action((x, c) => c.copy(timeout = x))
    opt[Unit]('m', "metadata").text("Retrieve all published metadata (make take awhile).")
      .action((x, c) => c.copy(getMetadata = true))
    opt[String]('f', "filter").valueName("<filename>").text("Input regexp filter, one filter per line. No filters means accept everything.")
      .action((x, c) => c.copy(filterFilename = x))
    opt[String]('o', "objects").valueName("<value list>").text("What metadata to return. Space separated list.")
      .action((x, c) => c.copy(objects = x))
    opt[String]("output").valueName("<filename>").text("Output file for metadata retrieved using -m. The entire SOAP envelope is output.")
      .action((x, c) => c.copy(output = x))
    help("help").text("Show help")
    note("The url can be obtained from the developer resources web page within your CRM org.")
  }

  def main(args: Array[String]): Unit = {

    val config = parser.parse(args, Config()) match {
      case Some(c) => c
      case None => Http.shutdown; return
    }

    println("Authenticating...")
    val header = GetHeaderOnline(config.username, config.password, config.url)

    // CRM Online
    val nameFuture = async {
      val h = await(header)
      val who = await(CrmWhoAmI(h, config))
      println("User guid: " + who)
      await(CrmGetUserName(h, who, config))
    }

    nameFuture onComplete {
      case Success(name) => println("Name: " + name)
      case Failure(ex) =>
        println("Exception obtaining name")
        println("Message: " + ex.getMessage)
        logger.debug("Exception during processing", ex)
    }

    // End of world, wait for the response.
    println("Obtaining WhoAmI...")
    catchTimeout("name") apply { Await.ready(nameFuture, config.timeout seconds) }

    if (config.getMetadata) {
      import java.nio.file._
      import io.Source

      // open filter filename if present
      val p = Paths.get(config.filterFilename)
      val filters =
        if (Files.exists(p)) {
          Source.fromFile(config.filterFilename).getLines.toSeq
        } else Seq()
      println("# entity filters to use: " + filters.size)
      if (filters.size == 0) println("Accept all entities." )

      println("Obtaining metadata...")
      val metadataFuture = async {
        val h = await(header)
        val req = post(config) << RetrieveAllEntities(h, config.objects).toString
        await(Http(req OK as.xml.Elem))
      }

      metadataFuture onComplete {
        case Success(m) =>
          println("Obtained metadata for entities: ")

          nonFatalCatch withApply { t =>
            println(s"Unable to write metadata output to ${config.output}")
          } apply Files.write(Paths.get(config.output), m.toString.getBytes)

          (m.child \\ "EntityMetadata").map { em: xml.Node =>
            //(em \\ "DisplayName" \\ "Label").text
            (em \\ "SchemaName").text
          }.filter(_.length > 0).sorted.foreach(println)
        case Failure(ex) =>
          println("Exception obtaining metadata")
          println("Message: " + ex.getMessage)
          logger.debug("Exception during processing", ex)
      }
      catchTimeout("metadata") apply { Await.ready(metadataFuture, config.timeout seconds) }
      Thread.sleep(10000)
    }

    Http.shutdown
  }

  /** Create a POST SOAP request. */

  def post(c: Config) = dispatch.url(endpoint(c.url)).secure.POST.setContentType("application/soap+xml", "utf-8")
  /**
   * Issues a WhoAmI message versus just returning a SOAP request body.
   */
  def CrmWhoAmI(auth: CrmAuthenticationHeader, config: Config): Future[String] = {

    val xml3 = <s:Body>
                 <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services">
                   <request i:type="c:WhoAmIRequest" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:c="http://schemas.microsoft.com/crm/2011/Contracts">
                     <b:Parameters xmlns:d="http://schemas.datacontract.org/2004/07/System.Collections.Generic"/>
                     <b:RequestId i:nil="true"/>
                     <b:RequestName>WhoAmI</b:RequestName>
                   </request>
                 </Execute>
               </s:Body>

    val body = wrap(auth, xml3).toString
    val req = post(config) << body.toString
    Http(req OK as.xml.Elem) map { response =>
      val nodes = (response \\ "KeyValuePairOfstringanyType")
      nodes.collect {
        case <KeyValuePairOfstringanyType><key>UserId</key><value>{ id }</value></KeyValuePairOfstringanyType> => id.text.trim
      }.headOption.get
    }
  }

  /**
   * Get the user name given the user id GUID. Issues the call to the endpoint versus just returning a SOAP request body.
   */
  def CrmGetUserName(authHeader: CrmAuthenticationHeader, id: String, config: Config): Future[String] = {
    val xml = <s:Body>
                <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
                  <request i:type="a:RetrieveRequest" xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts">
                    <a:Parameters xmlns:b="http://schemas.datacontract.org/2004/07/System.Collections.Generic">
                      <a:KeyValuePairOfstringanyType>
                        <b:key>Target</b:key>
                        <b:value i:type="a:EntityReference">
                          <a:Id>{ id }</a:Id>
                          <a:LogicalName>systemuser</a:LogicalName>
                          <a:Name i:nil="true"/>
                        </b:value>
                      </a:KeyValuePairOfstringanyType>
                      <a:KeyValuePairOfstringanyType>
                        <b:key>ColumnSet</b:key>
                        <b:value i:type="a:ColumnSet">
                          <a:AllColumns>false</a:AllColumns>
                          <a:Columns xmlns:c="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
                            <c:string>firstname</c:string>
                            <c:string>lastname</c:string>
                          </a:Columns>
                        </b:value>
                      </a:KeyValuePairOfstringanyType>
                    </a:Parameters>
                    <a:RequestId i:nil="true"/>
                    <a:RequestName>Retrieve</a:RequestName>
                  </request>
                </Execute>
              </s:Body>

    val body = wrap(authHeader, xml).toString
    Http(post(config) << body OK as.xml.Elem) map { response =>
      val nodes = (response \\ "KeyValuePairOfstringanyType")
      nodes.collect {
        case <KeyValuePairOfstringanyType><key>firstname</key><value>{ fname }</value></KeyValuePairOfstringanyType> => (1, fname.text)
        case <KeyValuePairOfstringanyType><key>lastname</key><value>{ lname }</value></KeyValuePairOfstringanyType> => (2, lname.text)
      }.sortBy(_._1).map(_._2).mkString(" ")
    }
  }

}



