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
import sdk.SoapHelpers._
import sdk.StreamHelpers._
import scala.util.matching._
import sdk.SoapNamespaces.implicits._
import sdk.crmwriters._
import sdk.responseReaders._
import sdk.metadata.readers._
import sdk._

object Create {

  import sdk.metadata._
  import sdk._

  private[this] implicit val logger = getLogger

  /**
   * Create some entities.
   */
  def apply(config: Config): Unit = {

    import Defaults._

    println("Create test.")
    val fut = GetHeaderOnline(config.username, config.password, config.url).flatMap { header =>
      val body1 =
        <s:Body>
          <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services">
            <request i:type="b:ExecuteMultipleRequest" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
              <b:Parameters xmlns:c="http://schemas.datacontract.org/2004/07/System.Collections.Generic">
                <b:KeyValuePairOfstringanyType>
                  <c:key>Requests</c:key>
                  <c:value i:type="d:OrganizationRequestCollection" xmlns:d="http://schemas.microsoft.com/xrm/2012/Contracts">
                    <d:OrganizationRequest i:type="b:CreateRequest">
                      <b:Parameters>
                        <b:KeyValuePairOfstringanyType>
                          <c:key>Target</c:key>
                          <c:value i:type="b:Entity">
                            <b:Attributes>
                              <b:KeyValuePairOfstringanyType>
                                <c:key>name</c:key>
                                <c:value i:type="e:string" xmlns:e="http://www.w3.org/2001/XMLSchema">Example Account 1</c:value>
                              </b:KeyValuePairOfstringanyType>
                            </b:Attributes>
                            <b:EntityState i:nil="true"/>
                            <b:FormattedValues/>
                            <b:Id>00000000-0000-0000-0000-000000000000</b:Id>
                            <b:LogicalName>account</b:LogicalName>
                            <b:RelatedEntities/>
                          </c:value>
                        </b:KeyValuePairOfstringanyType>
                      </b:Parameters>
                      <b:RequestId i:nil="true"/>
                      <b:RequestName>Create</b:RequestName>
                    </d:OrganizationRequest>
                  </c:value>
                </b:KeyValuePairOfstringanyType>
                <b:KeyValuePairOfstringanyType>
                  <c:key>Settings</c:key>
                  <c:value i:type="d:ExecuteMultipleSettings" xmlns:d="http://schemas.microsoft.com/xrm/2012/Contracts">
                    <d:ContinueOnError>false</d:ContinueOnError>
                    <d:ReturnResponses>true</d:ReturnResponses>
                  </c:value>
                </b:KeyValuePairOfstringanyType>
              </b:Parameters>
              <b:RequestId i:nil="true"/>
              <b:RequestName>ExecuteMultiple</b:RequestName>
            </request>
          </Execute>
        </s:Body>

      val body2 =
        <s:Body>
          <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
            <request i:type="a:CreateRequest" xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts">
              <a:Parameters xmlns:b="http://schemas.datacontract.org/2004/07/System.Collections.Generic">
                <a:KeyValuePairOfstringanyType>
                  <b:key>Target</b:key>
                  <b:value i:type="a:Entity">
                    <a:Attributes>
                      <a:KeyValuePairOfstringanyType>
                        <b:key>name</b:key>
                        <b:value i:type="c:string" xmlns:c="http://www.w3.org/2001/XMLSchema">My New Account Code</b:value>
                      </a:KeyValuePairOfstringanyType>
                      <a:KeyValuePairOfstringanyType>
                        <b:key>address1_city</b:key>
                        <b:value i:type="c:string" xmlns:c="http://www.w3.org/2001/XMLSchema">Minneapolis</b:value>
                      </a:KeyValuePairOfstringanyType>
                    </a:Attributes>
                    <a:EntityState i:nil="true"/>
                    <a:FormattedValues/>
                    <a:Id>00000000-0000-0000-0000-000000000000</a:Id>
                    <a:LogicalName>account</a:LogicalName>
                    <a:RelatedEntities/>
                    <a:RowVersion i:nil="true"/>
                  </b:value>
                </a:KeyValuePairOfstringanyType>
              </a:Parameters>
              <a:RequestId i:nil="true"/>
              <a:RequestName>Create</a:RequestName>
            </request>
          </Execute>
        </s:Body>

      def h2part(orgUrl: String) =
        <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute</a:Action>
        <SdkClientVersion xmlns="http://schemas.microsoft.com/xrm/2011/Contracts">7.0.0.3030</SdkClientVersion>
        <a:MessageID>urn:uuid:aa5ac821-ca34-4461-ba27-10899cd527eb</a:MessageID>
        <a:ReplyTo>
          <a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>
        </a:ReplyTo>
        <a:To s:mustUnderstand="1">{ orgUrl }</a:To>

      def xml2(orgUrl: String) =
        <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
          <s:Header>
            { h2part(orgUrl) }
            { CreateSOAPSecurityHeader(header.key, header.token1, header.token2) }
          </s:Header>
          { body2 }
        </s:Envelope>

      val headers = Map("SOAPAction" -> "http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute")

      val req = CrmAuth.createPost(config.url) <:< headers << xml2(header.url).toString
      println("req: " + req.toRequest)
      Http(req OKWithBody as.xml.Elem)
    }.map { resp =>
      println("response: " + resp)
      resp
    }.unwrapEx.recover {
      case x: ApiHttpError =>
        println(s"Exception during entity creation. Response code is: ${x.code}")
        println("Message: " + x.getMessage)
        println(s"Response body: ${x.response.getResponseBody}")
        logger.error(x)("Exception during entity creation processing.")
        null
      case scala.util.control.NonFatal(ex) =>
        println("Exception during entity creation (caught via NonFatal)")
        println("Message: " + ex.getMessage)
        logger.error(ex)("Exception during entity creation processing.")
        null
    }

    catchTimeout("create-test") {
      Await.ready(fut, config.timeout seconds)
    }
  }

}
