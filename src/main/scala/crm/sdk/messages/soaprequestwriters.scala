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
        { auth.map(a => <a:To s:mustUnderstand="1">{ endpoint(a.url) }</a:To>).getOrElse(new xml.NodeBuffer()) }
        { auth.map(a => soapSecurityHeaderTemplate(a.key, a.token1, a.token2)).getOrElse(new xml.NodeBuffer()) }
      </s:Header>
      <s:Body>
        <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services">
          { request }
        </Execute>
      </s:Body>
    </s:Envelope>

  //    <SdkClientVersion xmlns="http://schemas.microsoft.com/xrm/2011/Contracts">7.0.0.3030</SdkClientVersion>

  protected val whoami =
    <request i:type="c:WhoAmIRequest" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:c="http://schemas.microsoft.com/crm/2011/Contracts">
      <b:Parameters xmlns:d="http://schemas.datacontract.org/2004/07/System.Collections.Generic"/>
      <b:RequestId i:nil="true"/>
      <b:RequestName>WhoAmI</b:RequestName>
    </request>

  /** WhoAmIRequest -> SOAP Envelope. */
  implicit val whoAmIRequestWriter = CrmXmlWriter[WhoAmIRequest] { req =>
    executeTemplate(whoami)
  }

  /** CreateRequest -> SOAP Envelope */
  implicit val createRequestWriter = CrmXmlWriter[CreateRequest] { req =>
    val r = createRequestTemplate(req.entity, req.parameters)
    executeTemplate(r)
  }

  /**
   * Fast node transformer. Watch your stack since its recursive
   * and not tail-recursive.
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
  def addAuth(frag: xml.Elem, auth: CrmAuthenticationHeader, to: Option[String] = None): xml.Elem = {
    val adds =
      <a:To s:mustUnderstand="1">{ to.getOrElse(endpoint(auth.url)) }</a:To> ++
        soapSecurityHeaderTemplate(auth.key, auth.token1, auth.token2)
    trans(frag, { case e: xml.Elem if (e.label == "Header") => e.copy(child = e.child ++ adds) }).asInstanceOf[xml.Elem]
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
