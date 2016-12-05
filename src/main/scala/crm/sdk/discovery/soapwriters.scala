package crm
package sdk
package discovery


import scala.language._
import java.io._
import java.net._
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util._

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers._
import javax.xml.xpath.XPathExpressionException;
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

import org.apache.commons.codec.binary.Base64;
import org.w3c.dom._
import org.xml.sax.SAXException;
import dispatch._, Defaults._
import org.log4s._
import com.lucidchart.open.xtract._
import com.lucidchart.open.xtract.{ XmlReader, __ }
import com.lucidchart.open.xtract.XmlReader._
import com.lucidchart.open.xtract._
import play.api.libs.functional.syntax._
import cats._
import cats.data._
import cats.implicits._
import cats.syntax._
import sdk.driver.CrmException
import fs2._
import dispatch.retry._
import metadata._
import sdk.messages._

import soapnamespaces._

object soapreaders {

  implicit val readEndpoint: XmlReader[Endpoint] = (
    (__ \ "key").read[String] and
    (__ \ "value").read[String])(Endpoint.apply _)

  implicit val readOrganizationDetail: XmlReader[OrganizationDetail] = (
    (__ \ "FriendlyName").read[String] and
    (__ \ "OrganizationId").read[String] and
    (__ \ "OrganizationVersion").read[String] and
    (__ \ "State").read[String] and
    (__ \ "UniqueName").read[String] and
    (__ \ "UrlName").read[String] and
    (__ \ "Endpoints" \ "KeyValuePairOfEndpointTypestringztYlk6OT").read(seq[Endpoint]).default(Seq()))(OrganizationDetail.apply _)

  implicit val readSeqOrganizationDetail: XmlReader[Seq[OrganizationDetail]] = (__ \\ "OrganizationDetail").read(seq[OrganizationDetail])

}

object soapwriters {

  import httphelpers._
  //import crm.sdk.CrmAuth._

  private[this] lazy val logger = getLogger

  /**
   * Create a discovery request.
   *
   *  @param auth Authentication header to the discovery service, not authentication to the organization service.
   *  @param headers Headers to place into the request. This should *not* have the <Header> tag.
   *  @param body Body of envelope excluding the <body> tag.
   *  @param discoveryUrl URL of the discovery service and *not* a specific org.
   */
  def discoveryRequestEnvelopeTemplate(body: xml.Elem) =
    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
      <s:Header>
        <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Discovery/IDiscoveryService/Execute</a:Action>
        { messageIdEl }
        <a:ReplyTo>
          <a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>
        </a:ReplyTo>
      </s:Header>
      <s:Body>
        { body }
      </s:Body>
    </s:Envelope>

  /** SDK client version. Requires NS 'a' to be defined. */
  val SDKClientVersionHeader = <a:SdkClientVersion xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts">7.0</a:SdkClientVersion>

  /**
   * Create a request to obtain the list of organizations. Does not include the <body> tag.
   * @return An execute request including the <Execute> tag.
   */
  val retrieveOrganizationsRequestTemplate: xml.Elem =
    <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Discovery">
      <request i:type="RetrieveOrganizationsRequest" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
        <AccessType>Default</AccessType>
        <IsInternalCrossGeoServerRequest>false</IsInternalCrossGeoServerRequest>
        <Release>Current</Release>
      </request>
    </Execute>

  /** CreateRequest -> SOAP Envelope */
  implicit val retrieveOrganizationsRequestWriter = CrmXmlWriter[RetrieveOrganizationsRequest.type] { req =>
    val r = retrieveOrganizationsRequestTemplate
    discoveryRequestEnvelopeTemplate(r)
  }

  /**
   * Return organization detail using app default error handling. If an error occurs,
   * the left side holds a user presentable error message. Detailed error information
   * is provided in the log.
   *
   * @param auth Auth for discovery service.
   */
  def requestEndpoints(http: HttpExecutor, auth: CrmAuthenticationHeader)(implicit ex: ExecutionContext, reader: XmlReader[Seq[OrganizationDetail]]): Future[Either[String, Seq[OrganizationDetail]]] = {
    val r = discoveryRequestEnvelopeTemplate(retrieveOrganizationsRequestTemplate)    
    val reqXml = messages.soaprequestwriters.addAuth(r, auth)
    val req = dispatch.url(auth.url).secure.POST.setContentType("application/soap+xml", "utf-8") << reqXml.toString
    logger.debug("getEndpoints:request: " + req.toRequest)
    logger.debug("getEndpoints:request body: " + reqXml)
    http(req OKThenParse (reader, logger))(ex).map { result =>
      result match {
        case Right(endpoints) => Right(endpoints)
        case Left(UnexpectedStatus(_, code, _)) => Left("Unexpected response from server.")
        case Left(UnknonwnResponseError(_, msg, _)) => Left(s"Unknown error: $msg")
        case Left(XmlParseError(_, _, _)) => Left("Unable to interpret response from server.")
        case Left(CrmError(_, _, _)) => Left("Server returned a fault.")
      }
    }
  }

}

