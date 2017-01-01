package crm
package sdk
package metadata

import sdk.messages._
import scala.language._
import java.io._
import java.net._
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util._

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers._
import javax.xml.xpath.XPathExpressionException;
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

import org.apache.commons.codec.binary.Base64;
import org.w3c.dom._
import org.xml.sax.SAXException;
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
import metadata._

import sdk.messages._
import sdk.soapnamespaces.implicits._
import httphelpers._

object soapwriters {

  
  private[this] lazy val logger = getLogger

  def retrieveEntityRequestTemplate(entity: String, entityFilter: Seq[String], retieveAsIfPublished: Boolean = true) =
    <request i:type="a:RetrieveEntityRequest" xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts">
      <a:Parameters xmlns:b="http://schemas.datacontract.org/2004/07/System.Collections.Generic">
        <a:KeyValuePairOfstringanyType>
          <b:key>EntityFilters</b:key>
          <b:value i:type="c:EntityFilters" xmlns:c="http://schemas.microsoft.com/xrm/2011/Metadata">{ entityFilter.mkString(" ") }</b:value>
        </a:KeyValuePairOfstringanyType>
        <a:KeyValuePairOfstringanyType>
          <b:key>MetadataId</b:key>
          <b:value i:type="c:guid\" xmlns:c="http://schemas.microsoft.com/2003/10/Serialization/">00000000-0000-0000-0000-000000000000</b:value>
        </a:KeyValuePairOfstringanyType>
        <a:KeyValuePairOfstringanyType>
          <b:key>RetrieveAsIfPublished</b:key>
          <b:value i:type="c:boolean" xmlns:c="http://www.w3.org/2001/XMLSchema">{ retieveAsIfPublished }</b:value>
        </a:KeyValuePairOfstringanyType>
        <a:KeyValuePairOfstringanyType>
          <b:key>LogicalName</b:key>
          <b:value i:type="c:string" xmlns:c="http://www.w3.org/2001/XMLSchema">{ entity }</b:value>
        </a:KeyValuePairOfstringanyType>
      </a:Parameters>
      <a:RequestId i:nil="true"/>
      <a:RequestName>RetrieveEntity</a:RequestName>
    </request>

  def retrieveAllEntitiesRequestTemplate(entityFilter: Seq[String], retrieveAsIfPublished: Boolean = true) =
    <request i:type="b:RetrieveAllEntitiesRequest" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
      <b:Parameters xmlns:c="http://schemas.datacontract.org/2004/07/System.Collections.Generic">
        <b:KeyValuePairOfstringanyType>
          <c:key>EntityFilters</c:key><c:value i:type="d:EntityFilters" xmlns:d="http://schemas.microsoft.com/xrm/2011/Metadata">{ entityFilter }</c:value>
        </b:KeyValuePairOfstringanyType>
        <b:KeyValuePairOfstringanyType>
          <c:key>RetrieveAsIfPublished</c:key><c:value i:type="d:boolean" xmlns:d="http://www.w3.org/2001/XMLSchema">{ retrieveAsIfPublished.toString }</c:value>
        </b:KeyValuePairOfstringanyType>
      </b:Parameters><b:RequestId i:nil="true"/><b:RequestName>RetrieveAllEntities</b:RequestName>
    </request>

  /** RetrieveAllEntities -> SOAP Envelope */
  implicit val retrieveAllEntitiesRequestWriter = CrmXmlWriter[RetrieveAllEntitiesRequest] { req =>
    val r = retrieveAllEntitiesRequestTemplate(req.entityFilter, req.asIfPublished)
    messages.soaprequestwriters.executeTemplate(r)
  }

  /*
  /**
   *  Issue http request to obtain entity metadata.
   */
  def fetchEntityMetadata(http: HttpExecutor, auth: CrmAuthenticationHeader, entityFilter: Seq[String] = Seq("Entity", "Attributes", "Relationships"))(implicit ec: ExecutionContext) = {
    val c = RetrieveAllEntitiesRequest(entityFilter, true)
    val r = CrmXmlWriter.of[RetrieveAllEntitiesRequest].write(c).asInstanceOf[xml.Elem]
    val env = messages.soaprequestwriters.addAuth(r, auth)

    val req = httphelpers.createPost(auth.url) << env.toString
    logger.debug("requestEntityMetadata:request:" + req.toRequest)
    logger.debug("requestEntityMetadata:request body: " + env.toString)
    http(req)(ec)

  }

  /**
   * Return entity metadata or a user printable error message. By default, all metadata is returned
   * and the metadata package can be quite large. Automatically sets the cache
   * with the returned results if token is a Some. This function does additional
   * post processing to convert errors to be more user friendly.
   *
   * @param auth Org data services auth.
   * @param token String to hash to write persistent cache of metadata to. The entire envelope is written.
   * None implies no caching. Use the org web app url instead of the services one, generally.
   */
  def requestEntityMetadata(http: HttpExecutor, auth: CrmAuthenticationHeader, token: Option[String] = None)(implicit ec: ExecutionContext, reader: XmlReader[CRMSchema]): Future[Either[String, CRMSchema]] = {
    fetchEntityMetadata(http, auth)(ec) map { r =>
      token.foreach { t => setCache(t, r.getResponseBody) }
      r
    } map { r =>
      responseToXml(r).flatMap(processXml(_)(reader, logger))
    } map { result =>
      result match {
        case Right(schema) => Right(schema)
        case Left(UnexpectedStatus(_, code, _)) => Left("Unexpected response from server.")
        case Left(UnknonwnResponseError(_, msg, _)) => Left(s"Unknown error: $msg")
        case Left(XmlParseError(_, _, _)) => Left("Unable to interpret response from server.")
        case Left(CrmError(_, _, _)) => Left("Server returned a fault.")
      }
    }
  }

  /** Future fails if there is a parse error or no cache is found. Hence, it is never a Xor.Left. */
  def entityMetadataFromCache(token: String)(implicit reader: XmlReader[CRMSchema], ec: ExecutionContext): Future[Either[String, CRMSchema]] =
    Future {
      getCache(token).map { content =>
        reader.read(xml.XML.loadString(content)) match {
          case ParseSuccess(v) => Right(v)
          case PartialParseSuccess(v, issue) => Right(v)
          case _ => throw new RuntimeException(s"Unable to parse cache loaded for token $token")
        }
      }.getOrElse(throw new RuntimeException(s"No cache found for token $token."))
    }(ec)

  /**
   * Obtain an org's entity metadata either from the cache or by issuing a
   * request. The web app URL is used to to hash into the cache.
   *
   * @return A Future with an (error msg Either CrmSchema).
   */
  def entityMetadata(http: HttpExecutor, orgAuth: CrmAuthenticationHeader, webAppUrl: String)(implicit reader: XmlReader[CRMSchema], ec: ExecutionContext) =
    entityMetadataFromCache(webAppUrl) recoverWith
      { case _ => requestEntityMetadata(Http, orgAuth, Some(webAppUrl))(ec, reader) }

*/
}