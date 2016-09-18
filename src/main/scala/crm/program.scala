package crm

import scala.language._
import scala.util.control.Exception._

import scopt._
import org.w3c.dom._
import dispatch._, Defaults._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.async.Async._
import scala.util._
import better.files._
import org.log4s._

case class Config(
  help: Boolean = false,
  url: String = "",
  username: String = "",
  password: String = "",
  timeout: Int = 3 * 60,
  mode: String = "auth",
  filterFilename: String = "filters.txt",
  objects: String = "Entity Relationships Attributes",
  output: String = "metadata.xml",
  discoveryAction: String = "listRegions",
  discoveryRegion: String = "NA")

object program extends CrmAuth with SoapHelpers {

  private[this] val logger = getLogger

  val defaultConfig = Config()
  val parser = new scopt.OptionParser[Config]("crmauth") {
    override def showUsageOnError = true
    head("crmauth", "0.1.0")
    opt[String]('u', "userid").optional().valueName("<username>").text("Userid")
      .action((x, c) => c.copy(username = x))
    opt[String]('p', "password").optional().valueName("<password>").text("Password")
      .action((x, c) => c.copy(password = x))
    opt[Int]('t', "timeout").valueName("<number>").text(s"Timeout in seconds for each request. Default is ${defaultConfig.timeout}")
      .action((x, c) => c.copy(timeout = x))

    help("help").text("Show help")
    note("")

    cmd("metadata").action((_, c) => c.copy(mode = "metadata")).
      text("Obtain metadata from an organization").
      children(
        opt[String]('r', "url").required().valueName("<url>").text("Organization service url.").
          action((x, c) => c.copy(url = x)),
        opt[String]('f', "filter").valueName("<filename>").text(s"Input regexp inclusion filter, one filter per line. No filters means accept everything. Default reads ${defaultConfig.filterFilename} if present.").
          action((x, c) => c.copy(filterFilename = x)),
        opt[String]('o', "objects").valueName("<Entity Relationship Attributes>").text("What metadata to return. Space separated list. Use quotes in shell. All metadata is returned by default.").
          action((x, c) => c.copy(objects = x)),
        opt[String]("output").valueName("<filename>").text("Output file for metadata retrieved using -m. The entire SOAP envelope is output.").
          action((x, c) => c.copy(output = x)))
    note("You need a username/password and -r URL to run the auth command.")
    note("The orgcanization service url can be obtained from the developer resources web page within your CRM org or using the discovery command.")
    note("")
    cmd("auth").action((_, c) => c.copy(mode = "auth")).
      text("Check that authentication works. This is the default command.")
    note("You need a username/password and -r URL to run the auth command.")
    note("")
    cmd("discovery").action((_, c) => c.copy(mode = "discovery")).
      text("Discover and work with endpoints").
      children(
        opt[Unit]("list-regions").optional().text("List known (to this program) regions (abbrev and URLs) to use when finding endpoints. This is the default discovery action."),
        opt[String]("list-endpoints").valueName("<region abbrev>").optional().text("List the specific orgs given a specific region and username/pasword. Default region is NA.")
          .action((x, c) => c.copy(discoveryRegion = x, discoveryAction = "listEndpoints")))

    note("You only need a username/password and discovery URL for the region to run a discovery command.")

    checkConfig(c =>
      if (c.mode == "discovery" && c.discoveryAction == "listEndpoints" && (c.username == "" || c.password == ""))
        failure("Listing endpoints requires an username and password.")
      else if (c.mode == "auth" && (c.username == "" || c.password == ""))
        failure("Auth check requires an username, password and url.")
      else success)

    note("")
    note("This program only works with MS CRM Online.")
  }

  def main(args: Array[String]): Unit = {
    val config = parser.parse(args, defaultConfig) match {
      case Some(c) => c
      case None => Http.shutdown; return
    }
    config.mode match {
      case "discovery" => discovery(config)
      case "auth" => auth(config)
      case "metadata" => metadata(config)
    }
    Http.shutdown
  }

  def discovery(config: Config): Unit = {
    println("Discovery mode")
    config.discoveryAction match {
      case "listEndpoints" =>
        val endpoint = locationsToDiscoveryURL.get(config.discoveryRegion)
        endpoint match {
          case Some(e) => println(s"Listing known organizations endpoints for user ${config.username} at discovery location $e.")
          case _ =>
            println(s"Unknown region: ${config.discoveryRegion}")
            return
        }
        endpoint.foreach { e =>
          // Issues discovery request and print results.            
          val orgs = GetHeaderOnline(config.username, config.password, e).flatMap { header =>
            val reqXml = CreateDiscoveryRequest(header, Nil, Seq(CreateOrganizationsRequest), e)
            val req = dispatch.url(e).secure.POST.setContentType("application/soap+xml", "utf-8") << reqXml.toString
            Http(req OK as.xml.Elem)
          }.map { orgs: scala.xml.Elem =>
            import com.lucidchart.open.xtract._
            import com.lucidchart.open.xtract._
            import play.api.libs.functional.syntax._
            val theorgs = XmlReader.of[Seq[OrganizationDetail]].read(orgs \ "Body")
            theorgs.foreach {
              _ foreach { org =>
                println(s"Org     : ${org.friendlyName} (${org.uniqueName}, ${org.guid})")
                println(s"State   : ${org.state}")
                println(s"URL Name: ${org.urlName}")
                println(s"Version : ${org.version}")
                println("End points:")
                org.endpoints.foreach { e =>
                  println(f"\t${e.name}%-32s: ${e.url}")
                }
                println()
              }
            }
          } recover {
            case util.control.NonFatal(ex) =>
              println("Exception obtaining available organization endpoints")
              println("Message: " + ex.getMessage)
              logger.error(ex)("Exception during organization list processing.")
              Nil
          }
          catchTimeout("endponts") {
            Await.ready(orgs, config.timeout seconds)
          }
        }
      case "listRegions" =>
        println("Known discovery endpoints")
        locationsToDiscoveryURL.foreach {
          case (k, v) => println(f"$k%-10s: $v")
        }
      case _ =>
    }
  }

  def auth(config: Config): Unit = {
    println("Checking authentication by performing a WhoAmI")
    println("Authenticating...")
    val who = async {
      val header = await(GetHeaderOnline(config.username, config.password, config.url))
      val guid = await(CrmWhoAmI(header, config))
      println("User guid: " + guid)
      val name = await(CrmGetUserName(header, guid, config))
      println("Name: " + name)
    } recover {
      case util.control.NonFatal(ex) =>
        println("Exception obtaining name")
        println("Message: " + ex.getMessage)
        logger.error(ex)("Exception during processing")
        "unable to obtain name"
    }
    // End of world, wait for the response.
    println("Obtaining WhoAmI...")
    catchTimeout("name") { Await.ready(who, config.timeout seconds) }
  }

  def metadata(config: Config): Unit = {
    println("Authenticating...")
    val header = GetHeaderOnline(config.username, config.password, config.url)

    // open filter filename if present
    val filters = nonFatalCatch withApply { _ => Seq() } apply config.filterFilename.toFile.lines
    println("# entity inclusion filters to use: " + filters.size)
    if (filters.size == 0) println("Accept all entities.")

    println("Obtaining metadata...")
    val metadata = async {
      val h = await(header)
      val req = createPost(config) << RetrieveAllEntities(h, config.objects).toString
      val m = await(Http(req OK as.xml.Elem))
      println("Obtained metadata for entities: ")
      nonFatalCatch withApply { t =>
        println(s"Unable to write metadata output to ${config.output}")
      } apply config.output.toFile.printWriter(true).map(_.write(m.toString))
      (m.child \\ "EntityMetadata").map { em: xml.Node =>
        (em \\ "SchemaName").text
      }.filter(_.length > 0).sorted.foreach(println)
    } recover {
      case util.control.NonFatal(ex) =>
        println("Exception obtaining metadata")
        println("Message: " + ex.getMessage)
        logger.error(ex)("Exception during processing")
    }
    catchTimeout("metadata") { Await.ready(metadata, config.timeout seconds) }
    Thread.sleep(10000)
  }

  /** Create a POST SOAP request. */
  def createPost(url: String): Req = dispatch.url(endpoint(url)).secure.POST.setContentType("application/soap+xml", "utf-8")

  /** Create a POST SOAP request. */
  def createPost(c: Config): Req = createPost(c.url)

  /**
   * Wrap a raw XML request (should be the full <body> element) with a header and issue the request.
   * @return Request body as XML or exception thrown if the response is not OK.
   */
  def createPost(requestBody: xml.Elem, auth: CrmAuthenticationHeader, config: Config, url: Option[String] = None): Future[xml.Elem] = {
    val body = wrap(auth, requestBody)
    val req = createPost(url getOrElse config.url) << body.toString
    Http(req OK as.xml.Elem)
  }

  /**
   * Create a request to obtain the list of organizations. Does not include the <body> tag.
   */
  val CreateOrganizationsRequest: xml.Elem =
    <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Discovery">
      <request i:type="RetrieveOrganizationsRequest" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
        <AccessType>Default</AccessType>
        <IsInternalCrossGeoServerRequest>false</IsInternalCrossGeoServerRequest>
        <Release>Current</Release>
      </request>
    </Execute>

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
    val req = createPost(config) << body.toString
    Http(req OK as.xml.Elem) map { response =>
      val nodes = (response \\ "KeyValuePairOfstringanyType")
      nodes.collect {
        case <KeyValuePairOfstringanyType><key>UserId</key><value>{ id@_* }</value></KeyValuePairOfstringanyType> => id.text.trim
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
    Http(createPost(config) << body OK as.xml.Elem) map { response =>
      val nodes = (response \\ "KeyValuePairOfstringanyType")
      nodes.collect {
        case <KeyValuePairOfstringanyType><key>firstname</key><value>{ fname }</value></KeyValuePairOfstringanyType> => (1, fname.text)
        case <KeyValuePairOfstringanyType><key>lastname</key><value>{ lname }</value></KeyValuePairOfstringanyType> => (2, lname.text)
      }.sortBy(_._1).map(_._2).mkString(" ")
    }
  }
}
