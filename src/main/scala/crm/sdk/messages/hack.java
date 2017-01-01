package crm.sdk.messages;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.binary.Base64;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class hack {

    /**
     * Gets a CRM On Premise SOAP header & expiration.
     * 
     * @return CrmAuthenticationHeader An object containing the SOAP header and
     *         expiration date/time of the header.
     * @param username
     *            Username of a valid CRM user.
     * @param password
     *            Password of a valid CRM user.
     * @param url
     *            The Url of the CRM On Premise (IFD) organization
     *            (https://org.domain.com).
     * @param org
     *            True if Org svc, false to discovery service.
     * @throws Exception
     */
    public static String GetHeaderOnPremise(String response, String url /*, String created, String expires*/) throws Exception {
	Date now = new Date();

	TimeZone gmtTZ = TimeZone.getTimeZone("GMT");
	SimpleDateFormat formatter = new SimpleDateFormat(
		"yyyy-MM-dd'T'HH:mm:ss.SSSSSSS");
	formatter.setTimeZone(gmtTZ);

	DocumentBuilderFactory builderFactory = DocumentBuilderFactory
		.newInstance();
	DocumentBuilder builder = builderFactory.newDocumentBuilder();
	Document x = builder
		.parse(new ByteArrayInputStream(response.getBytes()));

	NodeList cipherValue1 = x.getElementsByTagName("e:CipherValue");
	String token1 = cipherValue1.item(0).getFirstChild().getTextContent();

	NodeList cipherValue2 = x.getElementsByTagName("xenc:CipherValue");
	String token2 = cipherValue2.item(0).getFirstChild().getTextContent();

	NodeList keyIdentiferElements = x
		.getElementsByTagName("o:KeyIdentifier");
	String keyIdentifer = keyIdentiferElements.item(0).getFirstChild()
		.getTextContent();

	NodeList x509IssuerNameElements = x
		.getElementsByTagName("X509IssuerName");
	String x509IssuerName = x509IssuerNameElements.item(0).getFirstChild()
		.getTextContent();

	NodeList x509SerialNumberElements = x
		.getElementsByTagName("X509SerialNumber");
	String x509SerialNumber = x509SerialNumberElements.item(0)
		.getFirstChild().getTextContent();

	NodeList binarySecretElements = x
		.getElementsByTagName("trust:BinarySecret");
	String binarySecret = binarySecretElements.item(0).getFirstChild()
		.getTextContent();

	String created = formatter.format(AddMinutes(-1, now)) + "Z";
	String expires = formatter.format(AddMinutes(60, now)) + "Z";
	String timestamp = "<u:Timestamp xmlns:u=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd\" u:Id=\"_0\"><u:Created>"
		+ created + "</u:Created><u:Expires>" + expires
		+ "</u:Expires></u:Timestamp>";

	MessageDigest mDigest = MessageDigest.getInstance("SHA1");
	byte[] hashedDataBytes = mDigest.digest(timestamp.getBytes());
	String digestValue = Base64.encodeBase64String(hashedDataBytes);

	String signedInfo = "<SignedInfo xmlns=\"http://www.w3.org/2000/09/xmldsig#\"><CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"></CanonicalizationMethod><SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#hmac-sha1\"></SignatureMethod><Reference URI=\"#_0\"><Transforms><Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"></Transform></Transforms><DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"></DigestMethod><DigestValue>"
		+ digestValue + "</DigestValue></Reference></SignedInfo>";
	byte[] signedInfoBytes = signedInfo.getBytes("UTF-8");
	Mac hmac = Mac.getInstance("HmacSHA1");
	byte[] binarySecretBytes = Base64.decodeBase64(binarySecret);
	SecretKeySpec key = new SecretKeySpec(binarySecretBytes, "HmacSHA1");
	hmac.init(key);
	byte[] hmacHash = hmac.doFinal(signedInfoBytes);
	String signatureValue = Base64.encodeBase64String(hmacHash);

	NodeList tokenExpiresElements = x.getElementsByTagName("wsu:Expires");
	String tokenExpires = tokenExpiresElements.item(0).getTextContent();

	String header = CreateSoapHeaderOnPremise(url, keyIdentifer, token1,
		token2, x509IssuerName, x509SerialNumber, signatureValue,
		digestValue, created, expires);
	return header;
    }

    /**
     * Gets a CRM On Premise (IFD) SOAP header.
     * 
     * @return String SOAP Header XML.
     * @param url
     *            The Url of the CRM On Premise (IFD) organization
     *            (https://org.domain.com).
     * @param keyIdentifer
     *            The KeyIdentifier from the initial request.
     * @param token1
     *            The first token from the initial request.
     * @param token2
     *            The second token from the initial request.
     * @param issuerNameX509
     *            The certificate issuer.
     * @param serialNumberX509
     *            The certificate serial number.
     * @param signatureValue
     *            The hashsed value of the header signature.
     * @param digestValue
     *            The hashed value of the header timestamp.
     * @param created
     *            The header created date/time..
     * @param expires
     *            The header expiration date/tim.
     */
    public static String CreateSoapHeaderOnPremise(String url, String keyIdentifer,
	    String token1, String token2, String issuerNameX509,
	    String serialNumberX509, String signatureValue, String digestValue,
	    String created, String expires) {
	StringBuilder xml = new StringBuilder();
//	xml.append("<s:Header>");
//	xml.append(
//		"<a:Action s:mustUnderstand=\"1\">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute</a:Action>");
//	xml.append("<a:MessageID>urn:uuid:" + java.util.UUID.randomUUID()
//		+ "</a:MessageID>");
//	xml.append("<a:ReplyTo>");
//	xml.append(
//		"<a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>");
//	xml.append("</a:ReplyTo>");
//	xml.append("<a:To s:mustUnderstand=\"1\">" + url
//		+ "XRMServices/2011/Organization.svc</a:To>");
	xml.append(
		"<o:Security xmlns:o=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd\">");
	xml.append(
		"<u:Timestamp xmlns:u=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd\" u:Id=\"_0\">");
	xml.append("<u:Created>" + created + "</u:Created>");
	xml.append("<u:Expires>" + expires + "</u:Expires>");
	xml.append("</u:Timestamp>");
//	xml.append("<trivial xmlns=\"urn:trivial:trivial\"/>");
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
	xml.append("<X509IssuerName>" + issuerNameX509 + "</X509IssuerName>");
	xml.append("<X509SerialNumber>" + serialNumberX509
		+ "</X509SerialNumber>");
	xml.append("</X509IssuerSerial>");
	xml.append("</X509Data>");
	xml.append("</o:SecurityTokenReference>");
	xml.append("</KeyInfo>");
	xml.append("<e:CipherData>");
	xml.append("<e:CipherValue>" + token1 + "</e:CipherValue>");
	xml.append("</e:CipherData>");
	xml.append("</e:EncryptedKey>");
	xml.append("</KeyInfo>");
	xml.append("<xenc:CipherData>");
	xml.append("<xenc:CipherValue>" + token2 + "</xenc:CipherValue>");
	xml.append("</xenc:CipherData>");
	xml.append("</xenc:EncryptedData>");
	xml.append("<Signature xmlns=\"http://www.w3.org/2000/09/xmldsig#\">");
	xml.append("<SignedInfo>");
	xml.append(
		"<CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>");
	xml.append(
		"<SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#hmac-sha1\"/>");
	xml.append("<Reference URI=\"#_0\">");
	xml.append("<Transforms>");
	xml.append(
		"<Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>");
	xml.append("</Transforms>");
	xml.append(
		"<DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/>");
	xml.append("<DigestValue>" + digestValue + "</DigestValue>");
	xml.append("</Reference>");
	xml.append("</SignedInfo>");
	xml.append("<SignatureValue>" + signatureValue + "</SignatureValue>");
	xml.append("<KeyInfo>");
	xml.append(
		"<o:SecurityTokenReference xmlns:o=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd\">");
	xml.append(
		"<o:KeyIdentifier ValueType=\"http://docs.oasis-open.org/wss/oasis-wss-saml-token-profile-1.0#SAMLAssertionID\">"
			+ keyIdentifer + "</o:KeyIdentifier>");
	xml.append("</o:SecurityTokenReference>");
	xml.append("</KeyInfo>");
	xml.append("</Signature>");
	xml.append("</o:Security>");
//	xml.append("</s:Header>");

	return xml.toString();
    }

    /**
     * 
     * @return Date The date with added minutes.
     * @param minutes
     *            Number of minutes to add.
     * @param time
     *            Date to add minutes to.
     */
    private static Date AddMinutes(int minutes, Date time) {
	long ONE_MINUTE_IN_MILLIS = 60000;
	long currentTime = time.getTime();
	Date newDate = new Date(currentTime + (minutes * ONE_MINUTE_IN_MILLIS));
	return newDate;
    }
}
