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
trait soaprequestwriters {

  import CrmAuth._
  import httphelpers._

  /**
   * Retrieve metadata based on the request content.
   *
   * @param request The <request>...</request> content including RequestId and RequestName.
   * @param auth Authentication header. If None, the <a:To></a:To> and auth are not set.
   * @param retrieveAsIfPublished Retrieve entities as if they had been published even if they have not.
   *
   * TODO: Rework SdkClientVersion.
   */
  def executeTemplate(request: xml.Elem, auth: Option[CrmAuthenticationHeader] = None) =
    <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
      <s:Header>
        <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute</a:Action>
        <SdkClientVersion xmlns="http://schemas.microsoft.com/xrm/2011/Contracts">7.0.0.3030</SdkClientVersion>
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
   * Fast node transformer.
   *
   * Use like:
   * {{{
   * def changeLabel(node: Node): Node =
   *   trans(node, {case e: Elem => e.copy(label = "b")})
   * }}}
   */
//  def trans(node: Node, pf: PartialFunction[Node, Node]): Node =
//    pf.applyOrElse(node, identity[Node]) match {
//      case e: xml.Elem => e.copy(child = e.child map (c => trans(c, pf)))
//      case other => other
//    }

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

object soaprequestwriters extends soaprequestwriters