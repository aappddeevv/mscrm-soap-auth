package crm
package sdk
package messages

import org.log4s._
import cats._
import cats.data._
import cats.implicits._
import java.time._
import metadata._

/**
 * Implicits for rendering requests into XML for SOAP messages.
 * These rendered requests do not have authentication headers
 * included.
 */
object soaprequestwriters {

  import CrmAuth._
  import httphelpers._

  /** Convert WS Addressing information to XML. */
  def addressingTemplate(el: crm.sdk.driver.SoapAddress) =
    <a:Action s:mustUnderstand="1">{ el.action }</a:Action>
    <a:MessageID>urn:uuid:{ el.messageId }</a:MessageID>
    <a:ReplyTo>
      <a:Address>{ el.replyToAddress }</a:Address>
    </a:ReplyTo>
    <a:To s:mustUnderstand="1">{ el.to }</a:To>

  /**
   * Convert an onprem auth header to XML.
   */
  implicit def crmOnPremAuthenticationHeaderWriter(implicit ns: NamespaceLookup) = CrmXmlWriter[CrmOnPremAuthenticationHeader] { c =>
    soapSecurityHeaderOnPremTemplate(c)
  }

  /**
   * Convert an online auth header to XML.
   */
  implicit def crmAuthenticationHeaderWriter(implicit ns: NamespaceLookup) = CrmXmlWriter[CrmAuthenticationHeader] { c =>
    soapSecurityHeaderTemplate(c)
  }

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
   * Create a MS CRM SOAP security header.
   */
  def soapSecurityHeaderTemplate(auth: CrmAuthenticationHeader): xml.Elem =
    soapSecurityHeaderTemplate(auth.key, auth.token1, auth.token2)

  /** Timestamp fragment for on-prem hashing/encryption. Specially crafted. */
  def timestampFragment(created: String, expires: String) =
    s"""<u:Timestamp xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd" u:Id="_0"><u:Created>${created}</u:Created><u:Expires>${expires}</u:Expires></u:Timestamp>"""

  /** SignedInfo claus for on-prem hashing/encryption. */
  def signedInfo(digestValue: String) =
    """<SignedInfo xmlns="http://www.w3.org/2000/09/xmldsig#">""" +
      """<CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/><SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#hmac-sha1"/>""" +
      """<Reference URI="#_0"><Transforms><Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/></Transforms><DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/>""" +
      s"""<DigestValue>${digestValue}</DigestValue></Reference></SignedInfo>"""

  /**
   * Header template. Creates the entire envelope.
   */
  def soapSecurityHeaderOnPremTemplate(h: CrmOnPremAuthenticationHeader) = {
    <o:Security xmlns="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:o="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
      { timestampFragment(h.created, h.expiresStr) }
      <xenc:EncryptedData Type="http://www.w3.org/2001/04/xmlenc#Element" xmlns:xenc="http://www.w3.org/2001/04/xmlenc#">
        <xenc:EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#aes256-cbc"/>
        <KeyInfo xmlns="http://www.w3.org/2000/09/xmldsig#">
          <e:EncryptedKey xmlns:e="http://www.w3.org/2001/04/xmlenc#">
            <e:EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#rsa-oaep-mgf1p">
              <DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/>
            </e:EncryptionMethod>
            <KeyInfo>
              <o:SecurityTokenReference xmlns:o="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                <X509Data>
                  <X509IssuerSerial>
                    <X509IssuerName>{ h.x509IssuerName }</X509IssuerName>
                    <X509SerialNumber>{ h.x509SerialNumber }</X509SerialNumber>
                  </X509IssuerSerial>
                </X509Data>
              </o:SecurityTokenReference>
            </KeyInfo>
            <e:CipherData>
              <e:CipherValue>{ h.token1 }</e:CipherValue>
            </e:CipherData>
          </e:EncryptedKey>
        </KeyInfo>
        <xenc:CipherData>
          <xenc:CipherValue>{ h.token2 }</xenc:CipherValue>
        </xenc:CipherData>
      </xenc:EncryptedData>
      <Signature xmlns="http://www.w3.org/2000/09/xmldsig#">
        { signedInfo(h.digestValue) }
        <SignatureValue>{ h.signatureValue }</SignatureValue>
        <KeyInfo>
          <o:SecurityTokenReference xmlns:o="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
            <o:KeyIdentifier ValueType="http://docs.oasis-open.org/wss/oasis-wss-saml-token-profile-1.0#SAMLAssertionID">{ h.key }</o:KeyIdentifier>
          </o:SecurityTokenReference>
        </KeyInfo>
      </Signature>
    </o:Security>
  }

  def header2(h: CrmOnPremAuthenticationHeader) = {
    val xml = new StringBuilder()
    xml.append(
      "<o:Security xmlns:o=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd\">");
    xml.append(timestampFragment(h.created, h.expiresStr))
    //      "<u:Timestamp xmlns:u=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd\" u:Id=\"_0\">");
    //    xml.append("<u:Created>" + h.created + "</u:Created>");
    //    xml.append("<u:Expires>" + h.expiresStr + "</u:Expires>");
    //    xml.append("</u:Timestamp>");
    xml.append(
      "<xenc:EncryptedData Type=\"http://www.w3.org/2001/04/xmlenc#Element\" xmlns:xenc=\"http://www.w3.org/2001/04/xmlenc#\">");
    xml.append(
      "<xenc:EncryptionMethod Algorithm=\"http://www.w3.org/2001/04/xmlenc#aes256-cbc\"/>");
    xml.append("<KeyInfo xmlns=\"http://www.w3.org/2000/09/xmldsig#\">");
    xml.append(
      "<e:EncryptedKey xmlns:e=\"http://www.w3.org/2001/04/xmlenc#\">");
    xml.append(
      "<e:EncryptionMethod Algorithm=\"http://www.w3.org/2001/04/xmlenc#rsa-oaep-mgf1p\">");
    xml.append(
      "<DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/>");
    xml.append("</e:EncryptionMethod>");
    xml.append("<KeyInfo>");
    xml.append(
      "<o:SecurityTokenReference xmlns:o=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd\">");
    xml.append("<X509Data>");
    xml.append("<X509IssuerSerial>");
    xml.append("<X509IssuerName>" + h.x509IssuerName + "</X509IssuerName>");
    xml.append("<X509SerialNumber>" + h.x509SerialNumber + "</X509SerialNumber>");
    xml.append("</X509IssuerSerial>");
    xml.append("</X509Data>");
    xml.append("</o:SecurityTokenReference>");
    xml.append("</KeyInfo>");
    xml.append("<e:CipherData>");
    xml.append("<e:CipherValue>" + h.token1 + "</e:CipherValue>");
    xml.append("</e:CipherData>");
    xml.append("</e:EncryptedKey>");
    xml.append("</KeyInfo>");
    xml.append("<xenc:CipherData>");
    xml.append("<xenc:CipherValue>" + h.token2 + "</xenc:CipherValue>");
    xml.append("</xenc:CipherData>");
    xml.append("</xenc:EncryptedData>");
    xml.append("<Signature xmlns=\"http://www.w3.org/2000/09/xmldsig#\">");
    xml.append(signedInfo(h.digestValue));
    //    xml.append("<SignedInfo>");
    //    xml.append(
    //      "<CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>");
    //    xml.append(
    //      "<SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#hmac-sha1\"/>");
    //    xml.append("<Reference URI=\"#_0\">");
    //    xml.append("<Transforms>");
    //    xml.append(
    //      "<Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>");
    //    xml.append("</Transforms>");
    //    xml.append(
    //      "<DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/>");
    //    xml.append("<DigestValue>" + h.digestValue + "</DigestValue>");
    //    xml.append("</Reference>");
    //    xml.append("</SignedInfo>");
    xml.append("<SignatureValue>" + h.signatureValue + "</SignatureValue>");
    xml.append("<KeyInfo>");
    xml.append(
      "<o:SecurityTokenReference xmlns:o=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd\">");
    xml.append(
      "<o:KeyIdentifier ValueType=\"http://docs.oasis-open.org/wss/oasis-wss-saml-token-profile-1.0#SAMLAssertionID\">"
        + h.key + "</o:KeyIdentifier>");
    xml.append("</o:SecurityTokenReference>");
    xml.append("</KeyInfo>");
    xml.append("</Signature>");
    xml.append("</o:Security>");
    xml.toString
  }

  def createRequestTemplate(entity: String, parameters: Map[String, Any]) =
    <request i:type="a:CreateRequest" xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
      <a:Parameters xmlns:b="http://schemas.datacontract.org/2004/07/System.Collections.Generic" xmlns:e="http://schemas.microsoft.com/2003/10/Serialization">
        <a:KeyValuePairOfstringanyType>
          <b:key>Target</b:key>
          <b:value i:type="a:Entity">
            <a:Attributes>
              { makeKVPairs(parameters) }
            </a:Attributes>
            <a:EntityState i:nil="true"/>
            <a:FormattedValues/>
            <a:Id>00000000-0000-0000-0000-000000000000</a:Id>
            <a:LogicalName>{ entity }</a:LogicalName>
            <a:RelatedEntities/>
            <a:RowVersion i:nil="true"/>
          </b:value>
        </a:KeyValuePairOfstringanyType>
      </a:Parameters>
      <a:RequestId i:nil="true"/>
      <a:RequestName>Create</a:RequestName>
    </request>

  def updateRequestTemplate(entity: String, id: String, parameters: Map[String, Any]) =
    <request i:type="a:UpdateRequest" xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
      <a:Parameters xmlns:b="http://schemas.datacontract.org/2004/07/System.Collections.Generic" xmlns:e="http://schemas.microsoft.com/2003/10/Serialization">
        <a:KeyValuePairOfstringanyType>
          <b:key>Target</b:key>
          <b:value i:type="a:Entity">
            <a:Attributes>
              { makeKVPairs(parameters) }
            </a:Attributes>
            <a:EntityState i:nil="true"/>
            <a:FormattedValues/>
            <a:Id>{ id }</a:Id>
            <a:LogicalName>{ entity }</a:LogicalName>
            <a:RelatedEntities/>
            <a:RowVersion i:nil="true"/>
          </b:value>
        </a:KeyValuePairOfstringanyType>
      </a:Parameters>
      <a:RequestId i:nil="true"/>
      <a:RequestName>Update</a:RequestName>
    </request>

  def deleteRequestTemplate(entity: String, id: String) =
    <request i:type="a:DeleteRequest" xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
      <a:Parameters xmlns:b="http://schemas.datacontract.org/2004/07/System.Collections.Generic" xmlns:e="http://schemas.microsoft.com/2003/10/Serialization">
        <a:KeyValuePairOfstringanyType>
          <b:key>Target</b:key>
          <b:value i:type="a:EntityReference">
            <a:Id>{ id }</a:Id>
            <a:LogicalName>{ entity }</a:LogicalName>
            <a:Name i:nil="true"/>
          </b:value>
        </a:KeyValuePairOfstringanyType>
      </a:Parameters>
      <a:RequestId i:nil="true"/>
      <a:RequestName>Delete</a:RequestName>
    </request>

  /** Associate or disassociate. Kind must be capitalized. */
  def associateRequestTemplate(kind: String, source: EntityReference, relationship: String, to: Seq[EntityReference]) =
    <request i:type={ "a:" + kind + "Request" } xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts">
      <a:Parameters xmlns:b="http://schemas.datacontract.org/2004/07/System.Collections.Generic">
        <a:KeyValuePairOfstringanyType>
          <b:key>Target</b:key>
          <b:value i:type="a:EntityReference">
            <a:Id>{ source.id }</a:Id>
            <a:LogicalName>{ source.target }</a:LogicalName>
            <a:Name i:nil="true"/>
          </b:value>
        </a:KeyValuePairOfstringanyType>
        <a:KeyValuePairOfstringanyType>
          <b:key>Relationship</b:key>
          <b:value i:type="a:Relationship">
            <a:PrimaryEntityRole i:nil="true"/>
            <a:SchemaName>{ relationship }</a:SchemaName>
          </b:value>
        </a:KeyValuePairOfstringanyType>
        <a:KeyValuePairOfstringanyType>
          <b:key>RelatedEntities</b:key>
          <b:value i:type="a:EntityReferenceCollection">
            {
              to.map { er =>
                <a:EntityReference>
                  <a:Id>{ er.id }</a:Id>
                  <a:LogicalName>{ er.target }</a:LogicalName>
                  <a:Name i:nil="true"/>
                </a:EntityReference>
              }
            }
          </b:value>
        </a:KeyValuePairOfstringanyType>
      </a:Parameters>
      <a:RequestId i:nil="true"/>
      <a:RequestName>{ kind }</a:RequestName>
    </request>

  /**
   * Retrieve metadata based on the request content.
   *
   * @param request The <request>...</request> content including RequestId and RequestName.
   * @param auth Authentication header. If None, the <a:To></a:To> and security elments are not set.
   *
   * TODO: Rework SdkClientVersion.
   */
  def executeTemplate(request: xml.Elem, auth: Option[CrmAuthenticationHeader] = None) =
    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
      <s:Header>
        <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute</a:Action>
        { messageIdEl() }
        <a:ReplyTo>
          <a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>
        </a:ReplyTo>
        { auth.map(a => <a:To s:mustUnderstand="1">{ endpoint(a.uri) }</a:To>).getOrElse(new xml.NodeBuffer()) }
        { auth.map(a => soapSecurityHeaderTemplate(a.key, a.token1, a.token2)).getOrElse(new xml.NodeBuffer()) }
      </s:Header>
      <s:Body>
        <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services" xmlns:d="http://schemas.microsoft.com/xrm/2011/Contracts/Services">
          { request }
        </Execute>
      </s:Body>
    </s:Envelope>

  //    <SdkClientVersion xmlns="http://schemas.microsoft.com/xrm/2011/Contracts">7.0.0.3030</SdkClientVersion>

  /** Creates Action, MessageID, ReplyTo/Address and To SOAP elements. */
  implicit val soapAddressWriter = CrmXmlWriter[crm.sdk.driver.SoapAddress] { address =>
    <a:Action s:mustUnderstand="1">{ address.action }</a:Action>
    <a:MessageID>urn:uuid:{ address.messageId }</a:MessageID>
    <a:ReplyTo>
      <a:Address>{ address.replyToAddress }</a:Address>
    </a:ReplyTo>
    <a:To s:mustUnderstand="1">{ address.to }</a:To>
  }

  // Removed from Envelope: xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"

  /** Creates a full SOAP envelope. SoapRequest.Body is the inner element of the SOAP body element i.e. don't include the outer body element. */
  implicit val soapRequestWriter = CrmXmlWriter[crm.sdk.driver.SoapRequest] { req =>
    import crm.sdk.soapnamespaces.implicits._
    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing">
      <s:Header>
        { CrmXmlWriter.of[crm.sdk.driver.SoapAddress].write(req.address) }
        { req.security.getOrElse(scala.xml.NodeSeq.Empty) }
      </s:Header>
      <s:Body>
        { req.body }
      </s:Body>
    </s:Envelope>
  }

  /** Create the final SOAP envelope. */
  def toSoapEnvelope(req: crm.sdk.driver.SoapRequest): String = {
    import crm.sdk.soapnamespaces.implicits._
    s"""<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing">
      <s:Header>
        ${CrmXmlWriter.of[crm.sdk.driver.SoapAddress].write(req.address)}
        ${req.security.getOrElse("")}
      </s:Header>
      <s:Body>
        ${crm.sdk.driver.contentToString(Seq(req.body))}
      </s:Body>
    </s:Envelope>"""
  }

  // Removed: xmlns:d="http://schemas.microsoft.com/xrm/2011/Contracts/Services"
  /** Tiny wrapper that adds the <Execute> element around executeMe. */
  def executeWrapper(executeMe: scala.xml.Elem) =
    <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services">
      { executeMe }
    </Execute>
  
  // must have default NS otherwise, xmlns="" kills you...
  protected val whoamiX =
    <request i:type="c:WhoAmIRequest" xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services" xmlns:c="http://schemas.microsoft.com/crm/2011/Contracts" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
    <b:Parameters xmlns:d="http://schemas.datacontract.org/2004/07/System.Collections.Generic"/>
    <b:RequestId i:nil="true"/>
    <b:RequestName>WhoAmI</b:RequestName>
    </request >
	

  val whoamiStr = {
    val xml = new StringBuilder();
    xml.append(
      "<Execute xmlns=\"http://schemas.microsoft.com/xrm/2011/Contracts/Services\">");
    xml.append(
      "<request i:type=\"c:WhoAmIRequest\" xmlns:b=\"http://schemas.microsoft.com/xrm/2011/Contracts\" xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:c=\"http://schemas.microsoft.com/crm/2011/Contracts\">");
    xml.append(
      "<b:Parameters xmlns:d=\"http://schemas.datacontract.org/2004/07/System.Collections.Generic\"/>");
    xml.append("<b:RequestId i:nil=\"true\"/>");
    xml.append("<b:RequestName>WhoAmI</b:RequestName>");
    xml.append("</request>");
    xml.append("</Execute>");
    xml.toString
  }

  /**
   * The <Execute> element for a WhoAmIRequest. The request content
   *  does not any parameters to create.
   */
  val whoAmIRequestTemplate = executeWrapper(whoamiX)

  /** WhoAmIRequest -> Execute . */
  implicit val whoAmIRequestWriter = CrmXmlWriter[WhoAmIRequest] { req =>
    whoAmIRequestTemplate
  }

  /** CreateRequest -> SOAP Envelope */
  implicit val createRequestWriter = CrmXmlWriter[CreateRequest] { req =>
    val r = createRequestTemplate(req.entity, req.parameters)
    executeTemplate(r)
  }

  /**
   * Fast node transformer. Watch your stack since its recursive
   * and not tail-recursive. This still recurses through the  wsa:EndpointReferenceType
   * entire structure.
   *
   * Usage:
   * {{{
   * def changeLabel(node: Node): Node =
   *   trans(node, {case e: Elem => e.copy(label = "b")})
   * }}}
   */
  def trans(node: xml.Node, pf: PartialFunction[xml.Node, xml.Node]): xml.Node =
    pf.applyOrElse(node, identity[xml.Node]) match {
      case e: xml.Elem => e.copy(child = e.child.map(c => trans(c, pf)))
      case other => other
    }

  /**
   * Adds <To> and <Security> elements to <Header>. If To is None, the To is
   *  obtained from the auth.
   */
  def addAuth(frag: xml.Elem, auth: AuthenticationHeader, to: Option[String] = None): xml.Elem = {
    //    val adds =
    //      <a:To s:mustUnderstand="1">{ to.getOrElse(endpoint(auth.uri)) }</a:To> ++ auth.xml
    //    trans(frag, { case e: xml.Elem if (e.label == "Header") => e.copy(child = e.child ++ adds) }).asInstanceOf[xml.Elem]
    throw new RuntimeException("Do not use....rewrite code.")
  }

  /** Assumes fetch is the top level Elem. */
  implicit val fetchXmlWriter: CrmXmlWriter[FetchExpression] = CrmXmlWriter { fe =>
    import scala.xml._

    // Adding paging info to the request.
    val page = new UnprefixedAttribute("page", fe.pageInfo.page.toString, Null)
    val count = new UnprefixedAttribute("count", fe.pageInfo.count.toString, Null)
    val pagingCookie = fe.pageInfo.cookie.map(c => new UnprefixedAttribute("paging-cookie", c.toString, Null)) getOrElse Null

    val withPagingInfo = fe.xml % page % count % pagingCookie

    <query i:type="b:FetchExpression" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
      <b:Query>
        { withPagingInfo.toString }
      </b:Query>
    </query>
  }

  /** Uses b namespace. */
  implicit def columnsWriter(implicit ns: NamespaceLookup) = CrmXmlWriter[ColumnSet] { c =>
    c match {
      case Columns(names) =>
        <b:ColumnSet>
          <b:AllColumns>false</b:AllColumns>
          <b:Columns xmlns:c="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
            { names.map(n => <c:string>{ n }</c:string>) }
          </b:Columns>
        </b:ColumnSet>
      case AllColumns =>
        <b:ColumnSet><b:AllColumns>true</b:AllColumns></b:ColumnSet>

    }
  }

  /** Uses b namespace. */
  implicit def pagingInfoWriter(implicit ns: NamespaceLookup) = CrmXmlWriter[PagingInfo] { p =>
    <b:PageInfo>
      <b:Count>{ p.count }</b:Count>
      <b:PageNumber>{ p.page }</b:PageNumber>
      <b:PagingCookie>{ p.cookie.getOrElse("") }</b:PagingCookie>
      <b:ReturnTotalRecordCount>
        { p.returnTotalRecordCount }
      </b:ReturnTotalRecordCount>
    </b:PageInfo>
  }

  implicit def queryExpressionWriter(implicit ns: NamespaceLookup) = CrmXmlWriter[QueryExpression] { qe =>

    /*
     * <query i:type="b:QueryExpression" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
     * <b:ColumnSet>
     * <b:AllColumns>false</b:AllColumns>
     * <b:Columns xmlns:c="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
     * <c:string>{ enameId.getOrElse(ename + "id") }</c:string>
     * </b:Columns>
     * </b:ColumnSet>
     * <b:Criteria>
     * <b:Conditions/>
     * <b:FilterOperator>And</b:FilterOperator>
     * <b:Filters/>
     * </b:Criteria>
     * <b:Distinct>false</b:Distinct>
     * <b:EntityName>{ ename }</b:EntityName>
     * <b:LinkEntities/>
     * <b:Orders/>
     * <b:PageInfo>
     * <b:Count>0</b:Count>
     * <b:PageNumber>{ page }</b:PageNumber>
     * <b:PagingCookie>{ cookie.getOrElse("") }</b:PagingCookie>
     * <b:ReturnTotalRecordCount>
     * true
     * </b:ReturnTotalRecordCount>
     * </b:PageInfo>
     * </query>
     */
    <query i:type="b:QueryExpression" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
      { CrmXmlWriter.of[ColumnSet].write(qe.columns) }
      <b:Criteria>
        <b:Conditions/>
        <b:FilterOperator>And</b:FilterOperator>
        <b:Filters/>
      </b:Criteria>
      <b:Distinct>false</b:Distinct>
      <b:EntityName>{ qe.entityName }</b:EntityName>
      <b:LinkEntities/>
      { CrmXmlWriter.of[PagingInfo].write(qe.pageInfo) }
    </query>
  }

  implicit def conditionsWriter[T](implicit ns: NamespaceLookup) = CrmXmlWriter[Seq[ConditionExpression[T]]] { s =>
    <b:Conditions>
    </b:Conditions>
  }

}
