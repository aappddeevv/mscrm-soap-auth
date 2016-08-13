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
import java.util.Date;
import com.typesafe.scalalogging._

case class CrmAuthenticationHeader(Header: scala.xml.Elem, Expires: Date)

trait CrmAuth extends SoapHelpers with LazyLogging {

  /**
   * Gets a CRM Online SOAP header & expiration.
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
  def GetHeaderOnline(username: String, password: String, urlx: String): Future[CrmAuthenticationHeader] = {

    val url = if (!urlx.endsWith("/"))
      urlx + "/"
    else
      urlx

    val urnAddress = GetUrnOnline(url)
    val now = new Date()
    val createdNow = String.format("%tFT%<tT.%<tLZ", now)
    val createdExpires = String.format("%tFT%<tT.%<tLZ", AddMinutes(60, now))

    val xml = <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
                <s:Header>
                  <a:Action s:mustUnderstand="1">http://schemas.xmlsoap.org/ws/2005/02/trust/RST/Issue</a:Action>
                  <a:MessageID>urn:uuid:{ java.util.UUID.randomUUID() }</a:MessageID>
                  <a:ReplyTo><a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address></a:ReplyTo>
                  <a:To s:mustUnderstand="1">https://login.microsoftonline.com/RST2.srf</a:To>
                  <o:Security s:mustUnderstand="1" xmlns:o="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                    <u:Timestamp u:Id="_0">
                      <u:Created>{ createdNow }</u:Created>
                      <u:Expires>{ createdExpires }</u:Expires>
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
      val authHeader = CrmAuthenticationHeader(CreateSoapHeaderOnline(url, keyIdentifer, token1, token2), c.getTime())

      authHeader
    }
  }

  /**
   * Create a CRM Online SOAP header including the security elements.
   *
   * @return String The XML SOAP header to be used in future requests.
   * @param url
   *            The Url of the CRM Online organization
   *            (https://org.crm.dynamics.com).
   * @param keyIdentifer
   *            The KeyIdentifier from the initial request.
   * @param token1
   *            The first token from the initial request.
   * @param token2
   *            The second token from the initial request..
   */
  def CreateSoapHeaderOnline(url: String, keyIdentifier: String, token1: String, token2: String): scala.xml.Elem = {
    <s:Header>
      <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute</a:Action>
      <Security xmlns="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
        <EncryptedData Id="Assertion0" Type="http://www.w3.org/2001/04/xmlenc#Element" xmlns="http://www.w3.org/2001/04/xmlenc#">
          <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#tripledes-cbc"/>
          <ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
            <EncryptedKey>
              <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#rsa-oaep-mgf1p"/>
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
      </Security>
      <a:MessageID>{ "urn:uuid:" + java.util.UUID.randomUUID() }</a:MessageID>
      <a:ReplyTo>
        <a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>
      </a:ReplyTo>
      <a:To s:mustUnderstand="1">{ endpoint(url) }</a:To>
    </s:Header>
  }

  /**
   * Gets the correct URN Address based on the Online region.
   *
   * @return String URN Address.
   * @param url
   *            The Url of the CRM Online organization
   *            (https://org.crm.dynamics.com).
   */
  def GetUrnOnline(url: String): String = {
    val urlc = url.toUpperCase
    val regionmap = collection.Map(
      "CRM2.DYNAMICS.COM" -> "crmsam:dynamics.com",
      "CRM4.DYNAMICS.COM" -> "crmemea:dynamics.com",
      "CRM5.DYNAMICS.COM" -> "crmapac:dynamics.com",
      "CRM6.DYNAMICS.COM" -> "crmoce:dynamics.com",
      "CRM7.DYNAMICS.COM" -> "crmjpn:dynamics.com",
      "CRM8.DYNAMICS.COM" -> "crmgcc:dynamics.com")
    val default = "crmna:dynamics.com"

    regionmap.get(urlc) getOrElse default
  }

  /**
   *
   * @return Date The date with added minutes.
   * @param minutes
   *            Number of minutes to add.
   * @param time
   *            Date to add minutes to.
   */
  def AddMinutes(minutes: Int, time: Date): Date = {
    val ONE_MINUTE_IN_MILLIS = 60000;
    val currentTime = time.getTime();
    val newDate = new Date(currentTime + (minutes * ONE_MINUTE_IN_MILLIS));
    newDate
  }
}
