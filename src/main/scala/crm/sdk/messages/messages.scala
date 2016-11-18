package crm
package sdk

package object messages {

  val Namespace = "http://schemas.microsoft.com/xrm/2011/Contracts"

  type Parameters = Map[String, Any]

  /** Empty parameters. */
  val EmptyParameters = Map.empty[String, Any]

  trait BaseOrgRequest {
    def name: String
    def parameters: Parameters
    def id: Option[java.util.UUID]
  }

  abstract class OrgRequest(val name: String, val id: Option[java.util.UUID] = None) extends BaseOrgRequest
  case class RetrieveAllEntitiesRequest(parameters: Parameters, asIfPublished: Boolean = false) extends OrgRequest("RetrieveAllEntitiesRequest")

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

}

object writers {

  import messages._

  implicit val parametersWriter = CrmXmlWriter[Parameters] { p =>
    <Parameters>
      {
        p.map {
          case (k, v) =>
            <key>{ k }</key><value>{ v }</value>
        }
      }
    </Parameters>
  }

  implicit val retrievewAllEntitiesRequestWriter = CrmXmlWriter[RetrieveAllEntitiesRequest] { r =>
    <request></request>
  }

}
