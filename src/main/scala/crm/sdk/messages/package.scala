package crm
package sdk

import org.log4s._
import cats._
import cats.data._
import cats.implicits._
import java.time._
import crm.sdk.metadata._
import crm.sdk.errorloggers._

/**
 * Messaging support for sending requests to CRM. For the moment, very
 * SOAP centric.
 */
package object messages {

  val Namespace = "http://schemas.microsoft.com/xrm/2011/Contracts"

  /**
   * Bit of non-static typing needed to handle parameters.
   *
   *  TODO: Make one based on scala Dynamic so its easier to use
   *  when the attribute names are known, etc.
   */
  type Parameters = Map[String, Any]

  /**
   * A typed value meant to be sent in a message to a CRM server.
   *  Most values are wrapped in a type so the rendering/processing can
   *  occur depending on the message type. In the case where a value is complex
   *  or has a type that does not match a JVM type, a
   *  CrmValue should be used.
   */
  sealed trait CrmValue
  /** An entity reference with a possible target. */
  case class EntityReference(id: String, target: Option[String] = None, name: Option[String] = None) extends CrmValue
  /** A simple GUID. Represented as a String inside the JVM. */
  case class Guid(id: String) extends CrmValue
  /** An OptionSetValue. If you do not have the code, you need to lookup the string value and convert it. */
  case class OptionSetValue(value: Int) extends CrmValue
  /** Unset value. Unsetting a value is not modeled by Option, use this or null or None instead. */
  case object UnsetValue extends CrmValue
  /** An invalid value. Causes an error during processing. */
  case class InvalidValue(msg: String) extends CrmValue
  /** Money value */
  case class MoneyValue(value: Float) extends CrmValue

  /** Empty parameters. */
  val EmptyParameters = Map.empty[String, Any]

  /**
   * Request types used with Execute or ExecuteMultiple methods. Some of
   * these overlap with the SOAP methods.
   * //CreateRequest
   * //UpdateRequest
   * //DeleteRequest
   * //AssociateRequest
   * //DisassociateRequest
   * //SetStateRequest
   * //WhoAmIRequest
   * //AssignRequest
   * //GrantAccessRequest
   * //ModifyAccessRequest
   * //RevokeAccessRequest
   * //RetrievePrincipleAccessRequest
   *
   */

  /**
   * A request is an immutable, non-protocol specific object that can be used to
   *  perform a CRM action. A Request needs to be used with the Execute or
   *  ExecuteMultiple SOAP method.
   */
  trait BaseOrgRequest {
    def name: String
    def msgId: Option[java.util.UUID]
  }

  /** Base class for org request. */
  abstract class OrgRequest(val name: String, val msgId: Option[java.util.UUID] = None) extends BaseOrgRequest

  /** Execute multiple requests. */
  case class ExecuteMultipleRequest(requests: Seq[OrgRequest]) extends OrgRequest("ExecuteMultipleRequest")

  /** Retrieve metadata. */
  case class AllEntitiesRequest(parameters: Parameters, asIfPublished: Boolean = false) extends OrgRequest("RetrieveAllEntitiesRequest")

  /** Retrieve endpoints. */
  case class EndpointsRequest() extends OrgRequest("Endpoints")

  /** Create request. */
  case class CreateRequest(entity: String, parameters: Parameters) extends OrgRequest("CreateRequest")

  /** Update request. */
  case class UpdateRequest(entity: String, id: String, parameters: Parameters) extends OrgRequest("UpdateRequest")

  /** Who Am I request */
  case class WhoAmIRequest() extends OrgRequest("WhoAmIRequest")

  /**
   * Methods.
   *
   * Create (or use Execute with a create request, the XML body is different)
   * Update (or use Execute with an update request, the XML body is different)
   * Delete (or use Execute with a Delete delete request, the XML body is different)
   * Retrieve
   * RetrieveMultiple - supported
   * RetrieveRelatedManyToMany
   * Associate
   * Disassociate
   * Fetch
   * SetState
   * Execute - supported
   *
   */

  /** Retrieve a single entity. */
  case class Retrieve(entity: String, id: String, parameters: Parameters, columns: Columns)

  /** Execute a request. */
  case class Execute(request: OrgRequest)
  
  
  
  /*
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
*/

  /**
   * Find attributes. Return either error messages or the attribute metadata objects mapped to the attribute name.
   * This function cycles through all attributes and does NOT fail fast.
   */
  def findAttributes(e: EntityDescription, attrs: Seq[String]): ValidatedNel[String, Map[String, Attribute]] = {
    val search = attrs.map { maybeAttr =>
      e.attributes.find(_.logicalName == maybeAttr) match {
        case Some(a) => (maybeAttr, a).validNel[String]
        case _ => s"Attribute ${e.logicalName}.${maybeAttr} not found.".invalidNel
      }
    }
    val t = search.toList.sequenceU
    t.map(_.toMap)
  }

  /**
   * Convert a scala map into XML KV pairs. The tricky part is
   *  making sure that the types are properly specified in the SOAP
   *  message and for that we need to refer to the schema. Simple
   *  types such as int or boolean use the 'c' namespace.
   */
  def makeKVPairs(p: Map[String, Any]): Iterable[xml.Elem] = {
    p.map {
      case (k, v) =>
        val fv = v match {
          case i: Int => ("c:int", Some(i.toString))
          case s: String => ("c:string", Some(s))
          case b: Boolean => ("c:boolean", Some(b))
          case os: OptionSetValue => ("a:OptionSetValue", Some(<a:Value>{ os.value }</a:Value>))
          case d: Instant => ("c:dateTime", Some(d.toString))
          case d: LocalDate => ("c:dateTime", Some(Instant.from(d).toString))
          case g: Guid => ("e:guid", Some(g.id))
          case m: MoneyValue => ("a:Money", Some(<a:Value>{ m.value }</a:Value>))
          case u: UnsetValue.type => ("e:int", None) //?? What to do?, this is wrong!
          case d: BigDecimal => ("c:decimal", Some(d))
          case er: EntityReference =>
            ("a:EntityReference", Some(<a:Id>{ er.id }</a:Id><a:LogicalName>{ er.target }</a:LogicalName><a:Name i:nil="true"/>))
          case i: InvalidValue =>
            throw new RuntimeException(s"Invalid value while encoding value for SOAP message: ${i.msg}")
        }
        <a:KeyValuePairOfstringanyType xmlns:c="http://www.w3.org/2001/XMLSchema">
          <b:key>{ k }</b:key>{ fv._2.map(v => <b:value i:type={ fv._1 }>{ v }</b:value>).getOrElse(<b:value i:nil="true"></b:value>) }
        </a:KeyValuePairOfstringanyType>
    }
  }

  import com.lucidchart.open.xtract._
  import com.ning.http.client._
  import httphelpers._

  /**
   * Process a response from an HTTP call. Performs application standard
   *  logging and response body conversion. If the server response contains a Fault
   *  it is logged by this function.
   *
   * @param request The XML request that generated this response. Needed for more robust error reporting if desired.
   * @param eresponse Either left with with a throwable or right with a Response object.
   * @param reader Envelope reader apply to the response body. It must read a full SOAP Envelope.
   * @param logger Logger for log messages.
   *  @return Left with a throwable or right with an Envelope.
   */
  def processResponse[E <: Envelope](request: Option[xml.Elem] = None)(eresponse: Either[Throwable, Response])(implicit reader: XmlReader[E], logger: Logger) = {
    import scala.util.{ Left => sleft, Right => sright }
    eresponse match {
      case Right(response) =>
        logger.debug(s"Response: ${show(response)}")
        val body = response.getResponseBody
        val crmResponse = responseToXml(response).flatMap(processXml(_)(reader, logger))
        crmResponse match {
          case Right(Envelope(_, Fault(n, msg))) =>
            logger.error(s"SOAP fault: $msg ($n)")
            sleft(new RuntimeException(s"Server error: $msg ($n)"))
          case Left(err) =>
            logger.error(s"Error occurred: $err")
            logger.error(s"User friendly message: ${toUserMessage(err)}")
            err.log
            sleft(new RuntimeException(toUserMessage(err)))
          case Right(e@Envelope(_, _)) =>
            logger.trace(e.toString)
            sright(e)
        }
      case Left(e) =>
        // Pass through.
        logger.error(e)("Request error")
        sleft(e)
    }
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

}

