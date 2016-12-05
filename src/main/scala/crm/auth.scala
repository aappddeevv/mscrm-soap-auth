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
import sdk.metadata.xmlreaders._
import sdk._

object Auth {

  private[this] implicit val logger = getLogger

  import scala.util.control.Exception._
  import program._
  import scala.async.Async.{ async => sasync }

  def apply(config: Config): Unit = {
    import Defaults._

    val who = scala.async.Async.async {
      println("Authenticating...")
      val header = await(GetHeaderOnline(config.username, config.password, config.url))
      val guid = await(CrmWhoAmI(header, config.url))
      println("User guid: " + guid)
      val name = await(CrmGetUserName(header, guid, config))
      println(s"Name: $name")
    } recover {
      case scala.util.control.NonFatal(ex) =>
        println("Exception obtaining name")
        println("Message: " + ex.getMessage)
        logger.error(ex)("Exception during processing")
    }
    // End of world, wait for the response.
    println("Obtaining WhoAmI...")
    catchTimeout("name") { Await.ready(who, config.timeout seconds) }
  }

  val whoAmIBodyTemplate =
    <request i:type="c:WhoAmIRequest" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:c="http://schemas.microsoft.com/crm/2011/Contracts">
      <b:Parameters xmlns:d="http://schemas.datacontract.org/2004/07/System.Collections.Generic"/>
      <b:RequestId i:nil="true"/>
      <b:RequestName>WhoAmI</b:RequestName>
    </request>

  /**
   * Issues a WhoAmI message versus just returning a SOAP request body.
   */
  def CrmWhoAmI(auth: CrmAuthenticationHeader, url: String): Future[String] = {
    import Defaults._

    //    val body = wrap(auth, whoAmIBodyTemplate).toString
    val body = executeTemplate(whoAmIBodyTemplate, Some(auth))

    val req = httphelpers.createPost(url) << body.toString
    Http(req OK as.xml.Elem) map { response =>
      val nodes = (response \\ "KeyValuePairOfstringanyType")
      nodes.collect {
        case <KeyValuePairOfstringanyType><key>UserId</key><value>{ id@_* }</value></KeyValuePairOfstringanyType> => id.text.trim
      }.headOption.get
    }
  }

  def retrieveRequestTemplate(entity: String, id: String) =
    <request i:type="a:RetrieveRequest" xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
      <a:Parameters xmlns:b="http://schemas.datacontract.org/2004/07/System.Collections.Generic">
        <a:KeyValuePairOfstringanyType>
          <b:key>Target</b:key>
          <b:value i:type="a:EntityReference">
            <a:Id>{ id }</a:Id>
            <a:LogicalName>{ entity }</a:LogicalName>
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

  /**
   * Get the user name given the user id GUID. Issues the call to the endpoint versus just returning a SOAP request body.
   */
  def CrmGetUserName(authHeader: CrmAuthenticationHeader, id: String, config: Config): Future[String] = {
    import Defaults._

    val body = executeTemplate(retrieveRequestTemplate("systemuser", id), Some(authHeader))
    Http(createPost(config) << body.toString OK as.xml.Elem) map { response =>
      val nodes = (response \\ "KeyValuePairOfstringanyType")
      nodes.collect {
        case <KeyValuePairOfstringanyType><key>firstname</key><value>{ fname }</value></KeyValuePairOfstringanyType> => (1, fname.text)
        case <KeyValuePairOfstringanyType><key>lastname</key><value>{ lname }</value></KeyValuePairOfstringanyType> => (2, lname.text)
      }.sortBy(_._1).map(_._2).mkString(" ")
    }
  }
}