package crm
package sdk

import scala.util.control.Exception._
import dispatch._, Defaults._
import java.util.Date;
import cats._
import data._
import cats.syntax._
import org.log4s._
import better.files._
import fs2._
import com.lucidchart.open.xtract._
import com.lucidchart.open.xtract.{ XmlReader, __ }
import com.lucidchart.open.xtract.XmlReader._
import com.lucidchart.open.xtract._
import play.api.libs.functional.syntax._
import scala.language.implicitConversions

import SoapNamespaces.NSMap

object crmwriters {

  import scala.xml._
  import CrmXmlWriter._

  /**
   * Fast node transformer.
   *
   * Use like:
   * {{{
   * def changeLabel(node: Node): Node =
   *   trans(node, {case e: Elem => e.copy(label = "b")})
   * }}}
   */
  def trans(node: Node, pf: PartialFunction[Node, Node]): Node =
    pf.applyOrElse(node, identity[Node]) match {
      case e: Elem => e.copy(child = e.child map (c => trans(c, pf)))
      case other => other
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
  implicit def columnsWriter(implicit ns: NSMap) = CrmXmlWriter[ColumnSet] { c =>
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
  implicit def pagingInfoWriter(implicit ns: NSMap) = CrmXmlWriter[PagingInfo] { p =>
    <b:PageInfo>
      <b:Count>{ p.count }</b:Count>
      <b:PageNumber>{ p.page }</b:PageNumber>
      <b:PagingCookie>{ p.cookie.getOrElse("") }</b:PagingCookie>
      <b:ReturnTotalRecordCount>
        { p.returnTotalRecordCount }
      </b:ReturnTotalRecordCount>
    </b:PageInfo>
  }

  implicit def queryExpressionWriter(implicit ns: NSMap) = CrmXmlWriter[QueryExpression] { qe =>
    
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
      { qe.pageInfo.write }
    </query>
  }

  implicit def conditionsWriter[T](implicit ns: NSMap) = CrmXmlWriter[Seq[ConditionExpression[T]]] { s =>
    <b:Conditions>
    </b:Conditions>
  }

}
