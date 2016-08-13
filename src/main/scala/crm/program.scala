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

case class Config(
  help: Boolean = false,
  url: String = "",
  username: String = "",
  password: String = "",
  timeout: Int = 60)

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
    help("help").text("Show help")
    note("The url can be obtained from the developer resources web page within your CRM org.")
  }

  def main(args: Array[String]): Unit = {
    val config = parser.parse(args, Config()) match {
      case Some(c) => c
      case None => Http.shutdown; return
    }

    // CRM Online
    val nameFuture = async {
      val header = GetHeaderOnline(config.username, config.password, config.url)
      val authHeader = await(header)
      val who = await(CrmWhoAmI(authHeader, config))
      println("user id guid: " + who)
      await(CrmGetUserName(authHeader, who, config))
    }

    nameFuture onComplete {
      case Success(name) => println("Name: " + name)
      case Failure(ex) =>
        println("Exception obtaining name")
        println("Messoge: " + ex.getMessage)
        logger.debug("Exception during processing", ex)
    }

    // End of world, wait for the response.
    catchTimeout apply { Await.ready(nameFuture, config.timeout seconds) }
  }

  /** Create a POST SOAP request. */
  def post(c: Config) = dispatch.url(endpoint(c.url)).secure.POST.setContentType("application/soap+xml", "UTF-8")

  /**
   * Start a WhoAmI message.
   */
  def CrmWhoAmI(authHeader: CrmAuthenticationHeader, config: Config): Future[String] = {
    val xml3 = <s:Body>
                 <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services">
                   <request i:type="c:WhoAmIRequest" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:c="http://schemas.microsoft.com/crm/2011/Contracts">
                     <b:Parameters xmlns:d="http://schemas.datacontract.org/2004/07/System.Collections.Generic"/>
                     <b:RequestId i:nil="true"/>
                     <b:RequestName>WhoAmI</b:RequestName>
                   </request>
                 </Execute>
               </s:Body>

    val body = wrap(authHeader, xml3).toString
    Http(post(config) << body OK as.xml.Elem) map { response =>
      val nodes = (response \\ "KeyValuePairOfstringanyType")
      nodes.collect {
        case <KeyValuePairOfstringanyType><key>UserId</key><value>{ id }</value></KeyValuePairOfstringanyType> => id.text.trim
      }.headOption.get
    }
  }

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
