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
      val guid = await(CrmWhoAmI(header, config))
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

  /**
   * Wrap a SOAP body in an envelope and add the authentication header. xmlns s is soap-envelope,
   * xmlns a is addressing and u is wssecurty utils.
   */
  def wrap(auth: CrmAuthenticationHeader, soapBody: scala.xml.Elem*) = {
    (<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing">
     </s:Envelope>).copy(child = Seq(auth.Header) ++ soapBody)
  }

  val whoAmIBodyTemplate = <s:Body>
                             <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services">
                               <request i:type="c:WhoAmIRequest" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:c="http://schemas.microsoft.com/crm/2011/Contracts">
                                 <b:Parameters xmlns:d="http://schemas.datacontract.org/2004/07/System.Collections.Generic"/>
                                 <b:RequestId i:nil="true"/>
                                 <b:RequestName>WhoAmI</b:RequestName>
                               </request>
                             </Execute>
                           </s:Body>

  /**
   * Issues a WhoAmI message versus just returning a SOAP request body.
   */
  def CrmWhoAmI(auth: CrmAuthenticationHeader, config: Config): Future[String] = {
    import Defaults._

    val body = wrap(auth, whoAmIBodyTemplate).toString
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
    import Defaults._

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