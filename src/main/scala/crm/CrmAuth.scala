package crm

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

/**
 * Authentication information for creating security headers.
 * @param url Small url e.g. https://org.crm.dynamics.com.
 */
case class CrmAuthenticationHeader(Header: scala.xml.Elem, key: String, token1: String, token2: String, Expires: Date, url: String)

trait CrmAuth extends SoapHelpers {

  /**
   * Issue a CRM Online SOAP authentication request.
   *
   * @return CrmAuthenticationHeader An object containing the SOAP header and
   *         expiration date/time of the header.
   * @param username
   *            Username of a valid CRM user.
   * @param password
   *            Password of a valid CRM user.
   * @param url
   *            The Url of the CRM Online organization
   *            (https://org.crm.dynamics.com).).
   */
  def GetHeaderOnline(username: String, password: String, url: String): Future[CrmAuthenticationHeader] = {

    val urnAddress = GetUrnOnline(url)
    val ts = timestamp

    val xml = <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
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

    val r = Http(svchost OK as.xml.Elem)

    r.map { xml =>
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
    }
  }

  /**
   * Create a CRM Online SOAP header including the security element.
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
      <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute</a:Action>
      { messageIdEl }
      <a:ReplyTo>
        <a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>
      </a:ReplyTo>
      <a:To s:mustUnderstand="1">{ endpoint(toUrl) }</a:To>
      { CreateSOAPSecurityHeader(keyIdentifier, token1, token2) }
    </s:Header>
  }

  //        <a:To s:mustUnderstand="1">{ endpoint(toUrl) }</a:To>

  /**
   * Create the CRM security SOAP element. Requires namespace u or wss security utils to be defined.
   */
  def CreateSOAPSecurityHeader(keyIdentifier: String, token1: String, token2: String) =
    <o:Security xmlns:o="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd" xmlns="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
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

  /** Create a generic SOAP message. Body should not have a body element wrapper. */
  def CreateGenericRequest(auth: CrmAuthenticationHeader, headers: Seq[scala.xml.Elem] = Nil, body: Seq[scala.xml.Elem] = Nil) =
    <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope" xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts">
      <s:Header>
        { SDKClientVersionHeader }
        { CreateSOAPSecurityHeader(auth.key, auth.token1, auth.token2) }
        { headers }
      </s:Header>
      <s:Body>
        { body }
      </s:Body>
    </s:Envelope>

  /**
   * Create a discovery request, which is different than a request to a a specific organization.
   *  @param auth Authentication header to the discovery service, not authentication to the organization service.
   *  @param headers Headers to place into the request. This should *not* have the <Header> tag.
   *  @param body Body including the <body> tag.
   *  @param url URL of the discovery service not a specific org.
   */
  def CreateDiscoveryRequest(auth: CrmAuthenticationHeader, headers: Seq[scala.xml.Elem] = Nil, body: Seq[scala.xml.Elem] = Nil, discoveryUrl: String) =
    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
      <s:Header>
        <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Discovery/IDiscoveryService/Execute</a:Action>
        { messageIdEl }
        <a:To s:mustUnderstand="1">{ discoveryUrl }</a:To>
        <a:ReplyTo>
          <a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>
        </a:ReplyTo>
        { CreateSOAPSecurityHeader(auth.key, auth.token1, auth.token2) }
        { headers }
      </s:Header>
      <s:Body>
        { body }
      </s:Body>
    </s:Envelope>

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
    val regionmap = collection.Map(
      "CRM2.DYNAMICS.COM" -> "crmsam:dynamics.com",
      "CRM4.DYNAMICS.COM" -> "crmemea:dynamics.com",
      "CRM5.DYNAMICS.COM" -> "crmapac:dynamics.com",
      "CRM6.DYNAMICS.COM" -> "crmoce:dynamics.com",
      "CRM7.DYNAMICS.COM" -> "crmjpn:dynamics.com",
      "CRM8.DYNAMICS.COM" -> "crmgcc:dynamics.com")
    val default = "crmna:dynamics.com"

    regionmap.get(urlx.getHost) getOrElse default
  }

  /**
   * Online O365 discovery URLs only.
   */
  val locationsToDiscoveryURL = collection.Map(
    "NA" -> "https://disco.crm.dynamics.com/XRMServices/2011/Discovery.svc",
    "NA2" -> "https://disco.crm9.dynamics.com/XRMServices/2011/Discovery.svc",
    "EMEA" -> "https://disco.crm4.dynamics.com/XRMServices/2011/Discovery.svc",
    "APAC" -> "https://disco.crm5.dynamics.com/XRMServices/2011/Discovery.svc",
    "Oceania" -> "https://disco.crm6.dynamics.com/XRMServices/2011/Discovery.svc",
    "JPN" -> "https://disco.crm7.dynamics.com/XRMServices/2011/Discovery.svc",
    "SA" -> "https://disco.crm2.dynamics.com/XRMServices/2011/Discovery.svc",
    "IND" -> "https://disco.crm8.dynamics.com/XRMServices/2011/Discovery.svc",
    "CN" -> "https://disco.crm3.dynamics.com/XRMServices/2011/Discovery.svc")

  val NSEnvelope = "http://schemas.xmlsoap.org/soap/envelope/"
  val NSContracts = "http://schemas.microsoft.com/xrm/2011/Contracts"
  val NSSchema = "http://www.w3.org/2001/XMLSchema-instance"
  val NSCollectionsGeneric = "http://schemas.datacontract.org/2004/07/System.Collections.Generic"
  val NSMetadata = "http://schemas.microsoft.com/xrm/2011/Metadata"

  /** Namespace lookup using default NS abbrevs. */
  val DefaultNS = collection.Map(
    "s" -> NSEnvelope,
    "a" -> NSContracts,
    "i" -> NSSchema,
    "b" -> NSCollectionsGeneric,
    "c" -> NSMetadata)

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
  def RetrieveAllEntities(auth: CrmAuthenticationHeader, entityFilter: String = "Entity", retrieveAsIfPublished: Boolean = false) =
    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
      <s:Header>
        <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute</a:Action>
        <SdkClientVersion xmlns="http://schemas.microsoft.com/xrm/2011/Contracts">7.0.0.3030</SdkClientVersion>
        { messageIdEl }
        <a:ReplyTo>
          <a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>
        </a:ReplyTo>
        <a:To s:mustUnderstand="1">{ endpoint(auth.url) }</a:To>
        { CreateSOAPSecurityHeader(auth.key, auth.token1, auth.token2) }
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

  import collection._

  case class Endpoint(name: String, url: String)

  implicit val readEndpoint: XmlReader[Endpoint] = (
    (__ \ "key").read[String] and
    (__ \ "value").read[String])(Endpoint.apply _)

  case class OrganizationDetail(friendlyName: String,
    guid: String,
    version: String,
    state: String,
    uniqueName: String,
    urlName: String,
    endpoints: Seq[Endpoint] = Nil)

  implicit val readOrganizationDetail: XmlReader[OrganizationDetail] = (
    (__ \ "FriendlyName").read[String] and
    (__ \ "OrganizationId").read[String] and
    (__ \ "OrganizationVersion").read[String] and
    (__ \ "State").read[String] and
    (__ \ "UniqueName").read[String] and
    (__ \ "UrlName").read[String] and
    (__ \ "Endpoints" \ "KeyValuePairOfEndpointTypestringztYlk6OT").read(seq[Endpoint]))(OrganizationDetail.apply _)

    implicit val readSeqOrganiatonDetail: XmlReader[Seq[OrganizationDetail]]= (__ \\ "OrganizationDetail").read(seq[OrganizationDetail])
    
}
