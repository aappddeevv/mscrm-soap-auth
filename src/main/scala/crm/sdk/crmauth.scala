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
import scala.concurrent._

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
import crm.sdk.driver._
import org.asynchttpclient._

import soapnamespaces._

/** This move to a sub-package. */
trait CrmAuth {

  import httphelpers._
  import sdk.discovery._

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
   * SOAP Envelope template for obtaining a security header.
   *
   * @param username
   * @param password
   * @param urn Not the web app URL, but a URN for the country.
   * @param leaseTimeInMin Lease time for the security header.
   * @param target The target for the message (the <To> element).
   */
  private def securityHeaderOnlineEnvelopeTemplate(username: String, password: String,
    urn: String, leaseTimeInMin: Int = 120, target: String = "https://login.microsoftonline.com/RST2.srf") = {
    val ts = timestamp(leaseTimeInMin)
    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
      <s:Header>
        <a:Action s:mustUnderstand="1">http://schemas.xmlsoap.org/ws/2005/02/trust/RST/Issue</a:Action>
        { messageIdEl }
        <a:ReplyTo><a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address></a:ReplyTo>
        <a:To s:mustUnderstand="1">{ target }</a:To>
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
              <a:Address>urn:{ urn }</a:Address>
            </a:EndpointReference>
          </wsp:AppliesTo>
          <trust:RequestType>http://schemas.xmlsoap.org/ws/2005/02/trust/Issue</trust:RequestType>
        </trust:RequestSecurityToken>
      </s:Body>
    </s:Envelope>
  }

  /*
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
   *            The Url of the CRM Online organization as seen by
   *            the web app URL. This is used to lookup up the URN
   *            (e.g. https://org.crm.dynamics.com or https://org.crm5.dynamics.com).
   * @param leaseTime Lease time for auth
   * @param numRetrys Number of times to retry if the auth fails.
   * @param pauseInSeconds Pause between retrys, in seconds.
   */
  def GetHeaderOnline(username: String, password: String, url: String, http: HttpExecutor = Http,
    leaseTime: Int = 120, numRetrys: Int = 5, pauseInSeconds: Int = 15)(implicit ec: ExecutionContext): Future[CrmAuthenticationHeader] = {

    val urnAddress = urlToUrn(url)
    val xml = securityHeaderOnlineEnvelopeTemplate(username, password, urnAddress, leaseTime)

    val svchost =
      (host("login.microsoftonline.com").secure / "RST2.srf")
        .POST
        .setContentType("application/soap+xml", "UTF-8") << xml.toString

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
          CrmAuthenticationHeader(
            c.getTime(),
            crm.sdk.messages.soaprequestwriters.soapSecurityHeaderTemplate(url, keyIdentifer, token1, token2),
            keyIdentifer, token1, token2, c.getTime(), url)
            
        } catch {
          case scala.util.control.NonFatal(e) =>
            logger.error(e)("Unable to obtain auth, error during parsing XML")
            throw e
        }
      }.either
    }

    //    retry.Pause(numRetrys, pauseInSeconds.seconds)(() => makeAuthRequest()).map {
    //      _ match {
    //        case Right(auth) => auth
    //        case Left(err) =>
    //          logger.error(err)("Error obtaining auth")
    //          throw err
    //      }
    //    }
    makeAuthRequest()
  }
  */

  private def onPremAuthTemplate(username: String, password: String, urn: String, leaseTimeInMin: Int = 60, target: String) = {
    val now = new Date();
    val gmtTZ = TimeZone.getTimeZone("GMT")
    val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
    formatter.setTimeZone(gmtTZ)

    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing">
      <s:Header>
        <a:Action s:mustUnderstand="1">http://docs.oasis-open.org/ws-sx/ws-trust/200512/RST/Issue</a:Action>
        { messageIdEl }
        <a:ReplyTo>
          <a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>
        </a:ReplyTo>
        <Security s:mustUnderstand="1" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd" xmlns="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
          <u:Timestamp u:Id={ java.util.UUID.randomUUID().toString }>
            <u:Created>{ formatter.format(now) + "Z" }</u:Created>
            <u:Expires>{ formatter.format(AddMinutes(leaseTimeInMin, now)) + "Z" }</u:Expires>
          </u:Timestamp>
          <UsernameToken u:Id={ java.util.UUID.randomUUID().toString }>
            <Username>{ username }</Username>
            <Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{ password }</Password>
          </UsernameToken>
        </Security>
        <a:To s:mustUnderstand="1">{ target }</a:To>
      </s:Header>
      <s:Body>
        <trust:RequestSecurityToken xmlns:trust="http://docs.oasis-open.org/ws-sx/ws-trust/200512">
          <wsp:AppliesTo xmlns:wsp="http://schemas.xmlsoap.org/ws/2004/09/policy">
            <a:EndpointReference>
              <a:Address>{ urn }</a:Address>
            </a:EndpointReference>
          </wsp:AppliesTo>
          <trust:RequestType>http://docs.oasis-open.org/ws-sx/ws-trust/200512/Issue</trust:RequestType>
        </trust:RequestSecurityToken>
      </s:Body>
    </s:Envelope>
  }

  /**
   * Obtain a CRM On Premise SOAP header & expiration for a specific URL endpoint.
   *
   * @param username username (typically userid@someorg.yourcrmdomain)
   * @param password password
   * @param http A Service that can convert Requests into XML response elements.
   * @param leaseTime Lease time in minutes requested for the returned token.
   * @param url A services URL e.g. discovery service or organization data service. See CRM's "Developer Resources" for the specific URLs.
   */
  def GetHeaderOnPremise(username: String,
    password: String, url: String, http: Service[Request, scala.xml.Elem],
    leaseTimeInMin: Int = 120)(implicit ec: ExecutionContext): Task[CrmOnPremAuthenticationHeader] = {

    val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
    //    val modurl =
    //      if (!url.endsWith("/")) url + "/";
    //      else url
    //
    val urnAddress = url //modurl + "XRMServices/2011/Organization.svc";
    //String usernamemixed = adfsUrl + "/trust/13/usernamemixed";

    def makeAuthRequest() = {
      getAdfs(url, http).flatMap { adfsUrl =>
        val usernamemixed = adfsUrl + "/13/usernamemixed";
        logger.debug("get auth IFD");
        logger.debug("urnAddress   : " + urnAddress);
        logger.debug("usernamemixed: " + usernamemixed);

        val xml = onPremAuthTemplate(username, password, urnAddress, leaseTimeInMin = leaseTimeInMin, usernamemixed)
        // this did not work!
        //val xml = securityHeaderOnlineEnvelopeTemplate(username, password, urnAddress, leaseTimeInMin = leaseTimeInMin, usernamemixed)

        val req = new RequestBuilder("POST")
          .setUrl(usernamemixed)
          .setBody(xml.toString)
          .addHeader("Content-Type", "application/soap+xml; charset=UTF-8")
          .build()

        //        logger.debug("GetHeaderOnPremise:request: " + req.toString)

        http(req).map { xml =>
          //          logger.debug("GetHeaderOnline:response: " + xml)

          val faultNode = xml \\ "Fault" // not already detected,see svc
          if (!faultNode.isEmpty) {
            // Get the message and throw an exception.
            val msg = (faultNode \\ "Reason" \\ "Text").text
            throw new HandlerError(NonEmptyList.of(msg))
          }

          CrmOnPremAuthenticationHeader(
            null,
            crm.sdk.messages.hack.GetHeaderOnPremise(xml.toString, "blah"),
            url, null, null, null,
            null, null,
            null, null, null, null)
        }
      }
    }
    makeAuthRequest()
  }

  def test1() = {
    import cats._, data._, implicits._
    import fs2.interop.cats._
    import scala.concurrent.ExecutionContext.Implicits.global
    import crm.sdk.client._

    val config = Config(
      //url = "https://crmdemo.demo.local:444/XRMServices/2011/Organization.svc",
      url = "https://dev.demo.local:444/XRMServices/2011/Discovery.svc",
      auth = crm.sdk.client.Federation,
      username = "crmuser@demo.local",
      password = "crmuser",
      httpRetrys = 1, pauseBetweenRetriesInSeconds = 1)
    val client = DiscoveryCrmClient.fromConfig(config)
    //val client = OrganizationCrmClient.fromConfig(config)

    //val svc = conn.toHttpService(toXml.run)

    // couple of tests
    //val x = getAdfs(config.url, svc)
    // get auth
    //val x = GetHeaderOnPremise("crmuser@demo.local", "crmuser", config.url, svc, config.leaseTimeInMin)
    //val x = null

    //    import crm.sdk._
    //    val client = OrganizationCrmClient.fromConfig(config)
    //    val r = client.request.withBody(crm.sdk.messages.soaprequestwriters.whoAmIRequestTemplate)
    //    import crm.sdk.driver._, fs2._, fs2.interop.cats._, crm.sdk.messages.soaprequestwriters._
    //    val result = client.fetch(r)(x => Task.now(x)).unsafeRun
    //    println("whoamiresult: " + result)    

    client
  }

  /**
   * Obtain the ADFS server from the discovery WSDL.
   *
   * Org and Dis services have a different WSDL access query parameter.
   * wsdl0 works for Org to get the ADFS server, but wsdl1 works for Dis.
   * Hence, the only save bet is singleWdl
   *
   * @param url Discovery or Org data services URL e.g. has a XrmServices path segment in it.
   * @param http Http service.
   */
  def getAdfs(url: String, http: Service[Request, scala.xml.Elem])(implicit ec: ExecutionContext): Task[String] = {
    //    val WsdlURL =
    //      if (url.toLowerCase.contains("xrmservices")) url
    //      else url + "/XrmServices/2011/Discovery.svc?wsdl=wsdl1" // was ?wsdl=wsdl0, (or 1) was Organization.svc
    //val WsdlURL = url + "?wsdl=wsdl0"
    val WsdlURL = url + "?singleWsdl"
    logger.debug("getAdfs:wsdlurl: " + WsdlURL)
    val req = new RequestBuilder()
      .setUrl(WsdlURL)
      .setBody("")
      .build

    def mkRequest() = {
      http(req).map { xml =>
        logger.debug("getADFS response: " + xml)
        val url = (xml \\ "Identifier").text.replace("http://", "https://")
        if (url.length == 0)
          throw new HandlerError(NonEmptyList.of("The response did not include the ADFS identifier."))
        logger.debug("adfs location: " + url)
        url
      }
    }
    mkRequest()
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
  /*
  def CreateSoapHeader(toUrl: String, keyIdentifier: String, token1: String, token2: String) = {
    <s:Header>
      { CreateExecuteHeaders(toUrl) }
      { soapSecurityHeaderTemplate(keyIdentifier, token1, token2) }
    </s:Header>
  }
  */

  //  /**
  //   *  Create standard IOrganization service execute headers. Does *not* include security header.
  //   *  Requires 'a' NS to be http://www.w3.org/2005/08/addressing?
  //   */
  //  def CreateExecuteHeaders(toUrl: String): xml.NodeSeq = {
  //    <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute</a:Action>
  //    <a:ReplyTo><a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address></a:ReplyTo>
  //    <a:To s:mustUnderstand="1">{ endpoint(toUrl) }</a:To> ++ { messageIdEl() }
  //  }

  /** HTTP headers for most SOAP calls. */
  //  val StandardHttpHeaders = collection.Map(
  //    "Accept" -> "application/xml, text/xml, */*",
  //    "Content-Type" -> "text/xml; charset=utf-8",
  //    "SOAPAction" -> "http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute")

  val DefaultAspect = 1
  val EntityAspect = 1
  val AttributesAspect = 2
  val PrivilegesAspect = 4
  val RelationshipsAspect = 8
  val AllAspects = 15

  /**
   * Retrieve metadata based on the entity filter provided.
   */
  /*
  def retrieveAllEntitiesTemplate(auth: CrmAuthenticationHeader, entityFilter: String = "Entity", retrieveAsIfPublished: Boolean = false) =
    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
      <s:Header>
        <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute</a:Action>
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
*/

  //                <SdkClientVersion xmlns="http://schemas.microsoft.com/xrm/2011/Contracts">7.0.0.3030</SdkClientVersion>

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
        <a:To s:mustUnderstand="1">{ endpoint(auth.uri) }</a:To>
        { crm.sdk.messages.soaprequestwriters.soapSecurityHeaderTemplate(auth) }
      </s:Header>
      <s:Body>
        <RetrieveMultiple xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services">
          { multipleRequest }
        </RetrieveMultiple>
      </s:Body>
    </s:Envelope>

  /*
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
      val headers = scala.collection.Map("SOAPAction" -> "http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute")
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

*/

  /*
  /**
   * Return an org services auth and org services url given user information and
   * a web app URL and region abbreviation.
   * @param auth Discovery auth.
   * @param leaseTime Lease time of auth header in minutes.
   */
  def orgServicesAuth(http: HttpExecutor, auth: CrmAuthenticationHeader, username: String, password: String, webAppUrl: String, regionAbbrev: String = "NA", leaseTime: Int = 120) = {
    regionToDiscoveryUrl.get(regionAbbrev) match {
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
    regionToDiscoveryUrl.get(regionAbbrev) match {
      case Some(discoveryUrl) =>
        GetHeaderOnline(username, password, discoveryUrl, http, leaseTime).map(Right(_))
      case _ =>
        Future.successful(Left(s"Unrecognized region abbrevation $regionAbbrev"))
    }
  }

  /** A service provided by CRM, organization or discovery. */
  trait CrmService
  abstract class DiscoveryService extends CrmService {
    def authenticate(username: String, password: String, regionAbbrev: String = "NA"): Unit
  }
  abstract class OrganizationService extends CrmService

  import soapreaders._

  /**
   * Given a discovery auth and a web app URL, return the services URL.
   * The webapp URL is typically https://yourorg.crm.dynamics.com. Otherwise
   * return an user presentable error string. The endpoints are accessed
   * through the discovery service and the webapp found in the endpoints.
   *
   * @param auth Discovery auth.
   * @param webAppUrl Web application URL.
   */
  def orgServicesUrl(http: HttpExecutor, auth: CrmAuthenticationHeader, webAppUrl: String): Future[Either[String, String]] = {
    discovery.soapwriters.requestEndpoints(http, auth).map { result =>
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

  /**
   * Obtain a data services auth given a data services URL directly.
   */
  def orgServicesAuthF(http: HttpExecutor, username: String, password: String, dataServicesUrl: String, leaseTime: Int)(implicit ec: ExecutionContext): Future[CrmAuthenticationHeader] = {
    GetHeaderOnline(username, password, dataServicesUrl, http, leaseTime)
  }
*/
}

object CrmAuth extends CrmAuth
