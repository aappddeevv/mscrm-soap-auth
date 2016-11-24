package crm
package sdk

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

import soapnamespaces._

trait CrmAuth {

  import httphelpers._

  private[this] lazy val logger = getLogger

  /**
   * Generate a NodeSeq with a timestamp. Uses "u" NS.
   */
  def genTimestamp() = {
    val ts = timestamp()
    <u:Timestamp u:Id="_0">
      <u:Created>{ ts._1 }</u:Created>
      <u:Expires>{ ts._2 }</u:Expires>
    </u:Timestamp>
  }

  /**
   * Issue a CRM Online SOAP authentication request. Since this is so fundamental,
   * an exception is thrown if an error occurs. Retrys are attempted. This method
   * issues the request directly.
   *
   * @return CrmAuthenticationHeader An object containing the SOAP header and
   *         expiration date/time of the header.
   * @param username
   *            Username of a valid CRM user.
   * @param password
   *            Password of a valid CRM user.
   * @param url
   *            The Url of the CRM Online organization
   *            (https://org.crm.dynamics.com).). Is this right?
   * @param leaseTime Lease time for auth
   * @param numRetrys Number of times to retry if the auth fails.
   * @param pauseInSeconds Pause between retrys, in seconds.
   */
  def GetHeaderOnline(username: String, password: String, url: String, http: HttpExecutor = Http,
    leaseTime: Int = 120, numRetrys: Int = 5, pauseInSeconds: Int = 15)(implicit ec: ExecutionContext): Future[CrmAuthenticationHeader] = {

    val urnAddress = GetUrnOnline(url)
    val ts = timestamp(leaseTime)

    val xml =
      <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
        <s:Header>
          <a:Action s:mustUnderstand="1">http://schemas.xmlsoap.org/ws/2005/02/trust/RST/Issue</a:Action>
          { messageIdEl }
          <a:ReplyTo><a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address></a:ReplyTo>
          <a:To s:mustUnderstand="1">https://login.microsoftonline.com/RST2.srf</a:To>
          <o:Security s:mustUnderstand="1" xmlns:o="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
            <u:Timestamp u:Id="_0">
              <u:Created>{ ts._1 }</u:Created>
              <u:Expires>{ ts._2 }</u:Expires>
            </u:Timestamp>
            <o:UsernameToken u:Id={ "uuid-" + java.util.UUID.randomUUID() + "-1" }>
              <o:Username>{ username }</o:Username>
              <o:Password>{ password }</o:Password>
            </o:UsernameToken>
          </o:Security>
        </s:Header>
        <s:Body>
          <trust:RequestSecurityToken xmlns:trust="http://schemas.xmlsoap.org/ws/2005/02/trust">
            <wsp:AppliesTo xmlns:wsp="http://schemas.xmlsoap.org/ws/2004/09/policy">
              <a:EndpointReference>
                <a:Address>urn:{ urnAddress }</a:Address>
              </a:EndpointReference>
            </wsp:AppliesTo>
            <trust:RequestType>http://schemas.xmlsoap.org/ws/2005/02/trust/Issue</trust:RequestType>
          </trust:RequestSecurityToken>
        </s:Body>
      </s:Envelope>

    // could use url("https://...")
    val svchost =
      (host("login.microsoftonline.com").secure / "RST2.srf")
        .POST
        .setContentType("application/soap+xml", "UTF-8") << xml.toString

    logger.debug("GetHeaderOnline:request: " + xml)
    logger.debug("GetHeaderOnline:request: " + svchost.toRequest)

    def makeAuthRequest() = {
      logger.debug("GetHeaderOnline:request: " + xml)
      logger.debug("GetHeaderOnline:request: " + svchost.toRequest)

      http(svchost OK as.xml.Elem).map { xml =>
        logger.debug("GetHeaderOnline:response: " + xml)

        try {
          val cipherElements = xml \\ "CipherValue"
          val token1 = cipherElements(0).text
          val token2 = cipherElements(1).text

          //val keyIdentiferElements = xml \\ "wsse:KeyIdentifier"
          val keyIdentiferElements = xml \\ "KeyIdentifier"
          val keyIdentifer = keyIdentiferElements(0).text

          //val tokenExpiresElements = xml \\ "wsu:Expires"
          val tokenExpiresElements = xml \\ "Expires"
          val tokenExpires = tokenExpiresElements(0).text

          val c = DatatypeConverter.parseDateTime(tokenExpires)
          CrmAuthenticationHeader(CreateSoapHeader(url, keyIdentifer, token1, token2), keyIdentifer, token1, token2, c.getTime(), url)
        } catch {
          case scala.util.control.NonFatal(e) =>
            logger.error(e)("Unable to obtain auth, error during parsing XML")
            throw e
        }
      }.either
    }

    retry.Pause(numRetrys, pauseInSeconds.seconds)(() => makeAuthRequest()).map {
      _ match {
        case Right(auth) => auth
        case Left(err) =>
          logger.error(err)("Error obtaining auth")
          throw err
      }
    }

  }

  /**
   * Create a CRM Online SOAP `<header>` element including the security element.
   *
   * @return String The XML SOAP header to be used in future requests.
   * @param url
   *            The Url of the CRM Online organization
   *            (https://org.crm.dynamics.com) w/o the .svc postfix.
   * @param keyIdentifer
   *            The KeyIdentifier from the initial request.
   * @param token1
   *            The first token from the initial request.
   * @param token2
   *            The second token from the initial request..
   */
  def CreateSoapHeader(toUrl: String, keyIdentifier: String, token1: String, token2: String) = {
    <s:Header>
      { CreateExecuteHeaders(toUrl) }
      { soapSecurityHeaderTemplate(keyIdentifier, token1, token2) }
    </s:Header>
  }

  /**
   *  Create standard IOrganization service execute headers. Does *not* include security header.
   *  Requires 'a' NS to be http://www.w3.org/2005/08/addressing?
   */
  def CreateExecuteHeaders(toUrl: String): xml.NodeSeq = {
    <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute</a:Action>
    <a:ReplyTo><a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address></a:ReplyTo>
    <a:To s:mustUnderstand="1">{ endpoint(toUrl) }</a:To> ++ { messageIdEl() }
  }

  /**
   * Create a MS CRM SOAP security header.
   */
  def soapSecurityHeaderTemplate(auth: CrmAuthenticationHeader): xml.Elem =
    soapSecurityHeaderTemplate(auth.key, auth.token1, auth.token2)

  /**
   * Create the CRM security SOAP element.
   */
  def soapSecurityHeaderTemplate(keyIdentifier: String, token1: String, token2: String): xml.Elem =
    <o:Security s:mustUnderstand="1" xmlns:o="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd" xmlns="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
      <EncryptedData Id="Assertion0" Type="http://www.w3.org/2001/04/xmlenc#Element" xmlns="http://www.w3.org/2001/04/xmlenc#">
        <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#tripledes-cbc"/>
        <ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
          <EncryptedKey>
            <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#rsa-oaep-mgf1p"></EncryptionMethod>
            <ds:KeyInfo Id="keyinfo">
              <wsse:SecurityTokenReference xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                <wsse:KeyIdentifier EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary" ValueType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509SubjectKeyIdentifier">{ keyIdentifier }</wsse:KeyIdentifier>
              </wsse:SecurityTokenReference>
            </ds:KeyInfo>
            <CipherData>
              <CipherValue>{ token1 }</CipherValue>
            </CipherData>
          </EncryptedKey>
        </ds:KeyInfo>
        <CipherData>
          <CipherValue>{ token2 }</CipherValue>
        </CipherData>
      </EncryptedData>
    </o:Security>

  /**
   * Create a generic SOAP message. Body should not have a body element wrapper. Caller should provide Action, ReplyTo, or To is provided.
   *  Namespaces 'a' and 's' namespaces are set. Security header is automatically provided.
   *  @param headers Headers. Should *not* have <Header> tag.l
   */
  /*  def CreateGenericRequest(auth: CrmAuthenticationHeader, headers: Seq[scala.xml.NodeSeq] = Nil, body: Seq[scala.xml.NodeSeq] = Nil) =
    <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope" xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts">
      <s:Header>
        { SDKClientVersionHeader }
        { soapSecurityHeaderTemplate(auth.key, auth.token1, auth.token2) }
        { headers }
      </s:Header>
      <s:Body>
        { body }
      </s:Body>
    </s:Envelope>
*/

  /**
   * Create a discovery request, whose "Action" is different than a request to the data organization service.
   *
   *  @param auth Authentication header to the discovery service, not authentication to the organization service.
   *  @param headers Headers to place into the request. This should *not* have the <Header> tag.
   *  @param body Body of envelope excluding the <body> tag.
   *  @param discoveryUrl URL of the discovery service and *not* a specific org.
   */
  def discoveryRequestEnvelopeTemplate(auth: CrmAuthenticationHeader, headers: Seq[scala.xml.Elem] = Nil, body: Seq[scala.xml.Elem] = Nil, discoveryUrl: String) =
    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
      <s:Header>
        <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Discovery/IDiscoveryService/Execute</a:Action>
        { messageIdEl }
        <a:To s:mustUnderstand="1">{ discoveryUrl }</a:To>
        <a:ReplyTo>
          <a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>
        </a:ReplyTo>
        { soapSecurityHeaderTemplate(auth.key, auth.token1, auth.token2) }
        { headers }
      </s:Header>
      <s:Body>
        { body }
      </s:Body>
    </s:Envelope>

  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers._
  import net.ceedubs.ficus.Ficus.toFicusConfig
  import com.typesafe.config.{ Config, ConfigFactory }
  import scala.collection._
  val _config: Config = ConfigFactory.load()

  private[this] val urnMap = mapValueReader[String].read(_config, "auth.urlToUrn")
  private[this] val defaultUrn = _config.as[String]("auth.defaultUrn")

  /** Region abbrevs mapped to their discovery URLs. */
  val locationsToDiscoveryURL = mapValueReader[String].read(_config, "auth.discoveryUrls")

  /**
   * Gets the correct URN Address based on the Online region.
   *
   * @return String URN Address.
   * @param url
   *            The Url of the CRM Online organization
   *            (https://org.crm.dynamics.com).
   */
  def GetUrnOnline(url: String): String = {
    val urlx = new java.net.URL(url.toUpperCase)
    urnMap.get(urlx.getHost) getOrElse defaultUrn
  }

  /** HTTP headers for most SOAP calls. */
  val StandardHttpHeaders = collection.Map(
    "Accept" -> "application/xml, text/xml, */*",
    "Content-Type" -> "text/xml; charset=utf-8",
    "SOAPAction" -> "http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute")

  /** SDK client version. Requires NS 'a' to be defined. */
  val SDKClientVersionHeader = <a:SdkClientVersion xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts">7.0</a:SdkClientVersion>

  val DefaultAspect = 1
  val EntityAspect = 1
  val AttributesAspect = 2
  val PrivilegesAspect = 4
  val RelationshipsAspect = 8
  val AllAspects = 15

  /**
   * Retrieve metadata based on the entity filter provided.
   */
  def retrieveAllEntitiesTemplate(auth: CrmAuthenticationHeader, entityFilter: String = "Entity", retrieveAsIfPublished: Boolean = false) =
    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
      <s:Header>
        <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute</a:Action>
        <SdkClientVersion xmlns="http://schemas.microsoft.com/xrm/2011/Contracts">7.0.0.3030</SdkClientVersion>
        { messageIdEl }
        <a:ReplyTo>
          <a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>
        </a:ReplyTo>
        <a:To s:mustUnderstand="1">{ endpoint(auth.url) }</a:To>
        { soapSecurityHeaderTemplate(auth.key, auth.token1, auth.token2) }
      </s:Header>
      <s:Body>
        <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services">
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
        </Execute>
      </s:Body>
    </s:Envelope>

  /**
   * Create a request to obtain the list of organizations. Does not include the <body> tag.
   * @return An execute request including the <Execute> tag.
   */
  val createOrganizationsRequestTemplate: xml.Elem =
    <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Discovery">
      <request i:type="RetrieveOrganizationsRequest" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
        <AccessType>Default</AccessType>
        <IsInternalCrossGeoServerRequest>false</IsInternalCrossGeoServerRequest>
        <Release>Current</Release>
      </request>
    </Execute>

  
  /**
   * Retrieve multiple objects request. This does not use the executeTemplate so the
   * request XML is a little different.
   *
   *  @param multipleRequest Element containing the multiple request. The content must be derived from a a Query.
   *  @param auth Org data services auth.
   */
  def retrieveMultiple(auth: CrmAuthenticationHeader, multipleRequest: xml.Elem) = //ename: String, page: Int, cookie: Option[String], enameId: Option[String] = None) =
    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
      <s:Header>
        <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/RetrieveMultiple</a:Action>
        { messageIdEl() }
        <a:ReplyTo>
          <a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>
        </a:ReplyTo>
        <a:To s:mustUnderstand="1">{ endpoint(auth.url) }</a:To>
        { soapSecurityHeaderTemplate(auth) }
      </s:Header>
      <s:Body>
        <RetrieveMultiple xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services">
          { multipleRequest }
        </RetrieveMultiple>
      </s:Body>
    </s:Envelope>

  /**
   * Get a page of entity results wrapped in a Task. Returns
   * `(Envelope, Option[PagingInfo], moreRecords)`. The
   * result type can be interpreted as the "result" and additional information to
   * perform another get, if needed, to obtain the next page of results in the multiple request.
   * Returned PagingInfo is suitable for a subsequent calls to this function as the
   * page number and cookie have been updated. The paging information and
   * "morerecords" values are promoted into the return value so its is easier
   * to determine if another call to this function is needed.
   *
   * The final XML request envelope is debug logged. An exception is thrown
   * if a Fault or error is encountered. Errors and SOAP faults are also logged.
   *
   * This function is usually called inside a monad that can provide a HttpExecutor
   * and CrmAuthenticationHeader. This function only return a single Envelope
   * but the request may require many Envelopes and the auth needs to be valid
   * for each call into this function.
   *
   * @param T Type of Envelope expected.
   * @param http Http executor to execute the request.
   * @param xmlBody The HTTP request body. The XML must be derived from a Query and typically integrates in the returned PagingInfo.
   * @param orgAuth Auth header
   * @param pageInfo Paging information that is updated based on the results of this call. The page number is incremented. (SHOULD THIS BE OUTSIDE THIS FUNCTION?)
   * @param retrys Number of automatica HTTP retries.
   * @param pauseInSeconsd Pause time between retries.
   *
   * @param ec ExecutionContext
   * @param reader Translator between response XML body and T
   * @param strategy fs2 Strategy
   *
   * @return Task of either a Throwable or (Envelope, updated PagingInfo, flag indicating more records are available)
   */
  def getMultipleRequestPage[T <: Envelope](http: HttpExecutor, xmlBody: scala.xml.Elem, orgAuth: CrmAuthenticationHeader, pageInfo: Option[PagingInfo],
    retrys: Int = 5, pauseInSeconds: Int = 30)(
      implicit ec: ExecutionContext, strategy: Strategy, reader: XmlReader[T]): Task[Either[Throwable, (Envelope, Option[PagingInfo], Boolean)]] = Task.fromFuture {
    import soapreaders._

    def makeRequest = {
      val qxml = retrieveMultiple(orgAuth, xmlBody)
      logger.debug("Query XML: " + qxml.toString)
      val headers = Map("SOAPAction" -> "http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute")
      val req = createPost(orgAuth.url) <:< headers << qxml.toString
      logger.debug("Query entity request: " + req.toRequest)
      http(req).either.map {
        _ match {
          case Right(response) =>
            logger.debug(s"Response: ${show(response)}")
            try {
              val body = response.getResponseBody
              val crmResponse = responseToXml(response).flatMap(processXml(_)(reader, logger))
              crmResponse match {
                case Right(Envelope(_, Fault(n, msg))) =>
                  logger.error(s"SOAP fault: $msg ($n)")
                  Left(new RuntimeException(s"Server returned an error: $msg ($n)"))
                case Left(err) =>
                  logger.error(s"Error occurred: $err")
                  logger.error(s"User friendly message: ${toUserMessage(err)}")
                  Left(new RuntimeException(toUserMessage(err)))
                case Right(e@Envelope(_, ec@EntityCollectionResult(name, entities, total, limit, more, cookieOpt))) =>
                  logger.trace(ec.toString)
                  logger.info(s"Iteration: ename=$name, totalRecordCount=$total, limitExceeded=>$limit, moreRecords=$more, cookie=$cookieOpt")
                  val pinfo = cookieOpt.flatMap(c => pageInfo.map(pi => pi.copy(page = pi.page + 1, cookie = cookieOpt)))
                  Right((e, pinfo, more))
                case Right(e@Envelope(_, _)) =>
                  Right((e, None, false))
              }
            } catch {
              case scala.util.control.NonFatal(e) =>
                logger.error(e)("Error possibly during string=>xml or xml=>object reading.")
                logger.error(s"Request content: ${qxml.toString}")
                logger.error(s"Response body: ${response.getResponseBody}")
                Left(e)
            }

          case Left(e) =>
            logger.error(e)("Request error")
            Left(e)
        }
      }
    }
    import retry.Success._
    retry.Pause(retrys, pauseInSeconds.seconds)(() => makeRequest)
  }(strategy, ec)

  /**
   * Return organization detail using app default error handling. If an error occurs,
   * the left side holds a user presentable error message. Detailed error information
   * is provided in the log.
   *
   * @param auth Auth for discovery service.
   */
  def requestEndpoints(http: HttpExecutor, auth: CrmAuthenticationHeader)(implicit ex: ExecutionContext, reader: XmlReader[Seq[OrganizationDetail]]): Future[Either[String, Seq[OrganizationDetail]]] = {
    val reqXml = discoveryRequestEnvelopeTemplate(auth, Nil, Seq(createOrganizationsRequestTemplate), auth.url)
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

  /**
   *  Issue http request to obtain entity metadata.
   */
  def fetchEntityMetadata(http: HttpExecutor, auth: CrmAuthenticationHeader)(implicit ec: ExecutionContext) = {
    val body = retrieveAllEntitiesTemplate(auth, "Entity Attributes Relationships").toString
    val req = createPost(auth.url) << body
    logger.debug("requestEntityMetadata:request:" + req.toRequest)
    logger.debug("requestEntityMetadata:request body: " + body)
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
   * @return A Future with an (error msg Xor CrmSchema).
   */
  def entityMetadata(http: HttpExecutor, orgAuth: CrmAuthenticationHeader, webAppUrl: String)(implicit reader: XmlReader[CRMSchema], ec: ExecutionContext) =
    entityMetadataFromCache(webAppUrl) recoverWith
      { case _ => requestEntityMetadata(Http, orgAuth, Some(webAppUrl))(ec, reader) }

  /** Create a POST SOAP request. */
  def createPost(url: String): Req = dispatch.url(endpoint(url)).secure.POST.setContentType("application/soap+xml", "utf-8")

  /**
   *  Return a discovery auth or an error message wrapped in a future.
   *
   *  The discovery URL is obtained from the region, webAppUrl and user information.
   *  Then the org services URL is obtained from that lookup. Most people know their
   *  CRM web app URL and not their org services URL.
   *
   *  @param webAppUrl The URL to your CRM instance.
   *  @param username Username
   *  @param password Password
   *  @param regionAbbrev Abbreviation for the region.
   *  @param leaseTimee Lease time on auth in minutes.
   *  @return Client to use in calls. Includes authentication results.
   */
  def discoveryAuth(http: HttpExecutor, username: String,
    password: String, regionAbbrev: String = "NA", leaseTime: Int = 120)(implicit ec: ExecutionContext): Future[Either[String, CrmAuthenticationHeader]] = {
    locationsToDiscoveryURL.get(regionAbbrev) match {
      case Some(discoveryUrl) =>
        GetHeaderOnline(username, password, discoveryUrl, http, leaseTime).map(Right(_))
      case _ =>
        Future.successful(Left(s"Unrecognized region abbrevation $regionAbbrev"))
    }
  }

  /**
   * Return an org services auth and org services url given user information and
   * a web app URL and region abbreviation.
   * @param auth Discovery auth.
   * @param leaseTime Lease time of auth header in minutes.
   */
  def orgServicesAuth(http: HttpExecutor, auth: CrmAuthenticationHeader, username: String, password: String, webAppUrl: String, regionAbbrev: String = "NA", leaseTime: Int = 120) = {
    locationsToDiscoveryURL.get(regionAbbrev) match {
      case Some(discoveryUrl) =>
        orgServicesUrl(http, auth, webAppUrl) flatMap {
          _ match {
            case Left(err) => Future.successful(Left(err))
            case Right(url) => GetHeaderOnline(username, password, url, http, leaseTime).map(Right(_))
          }
        }
      case _ =>
        Future.successful(Left(s"Unrecognized region abbreviation $regionAbbrev"))
    }
  }

  import soapreaders._

  /**
   * Given a discovery auth and a web app URL, return the services URL.
   * The webapp URL is typically https://yourorg.crm.dynamics.com. Otherwise
   * return an user presentable error string.
   *
   * @param auth Discovery auth.
   * @param webAppUrl Web application URL.
   */
  def orgServicesUrl(http: HttpExecutor, auth: CrmAuthenticationHeader, webAppUrl: String): Future[Either[String, String]] = {
    requestEndpoints(http, auth).map { result =>
      result match {
        case Right(details) =>
          val matched = details.find { _.endpoints.find(endp => endp.url.contains(webAppUrl)).isDefined }
          val servicesUrl = matched.flatMap { detail =>
            detail.endpoints.find(_.name == "OrganizationService").map(_.url)
          }
          Either.fromOption(servicesUrl, s"Unable to find org services URL match for $webAppUrl in the advertised endpoints.")
        case Left(msg) => Left(msg)
      }
    }
  }

  /**
   * Produce an org services auth suitable for SOAP requests.
   *
   * The discovery auth is thrown away so this is a very expensive call to make.
   *
   * @param leaseTime Lease time of auth header in minutes.
   */
  def orgServicesAuthF(http: HttpExecutor, username: String, password: String, webAppUrl: String, region: String, leaseTime: Int = 120)(implicit ec: ExecutionContext): Future[CrmAuthenticationHeader] = {
    discoveryAuth(http, username, password, region).flatMap { discovery =>
      discovery match {
        case Right(discoveryAuth) => orgServicesAuth(http, discoveryAuth, username, password, webAppUrl, region, leaseTime).flatMap {
          _ fold (err => throw new RuntimeException(err), auth => Future.successful(auth))
        }
        case Left(err) => throw new RuntimeException(err)
      }
    }
  }

}

object CrmAuth extends CrmAuth


