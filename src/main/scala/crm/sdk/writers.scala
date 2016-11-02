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

/**
 *  General purpose XML writer typeclass.
 */
trait CrmXmlWriter[-A] {
  def write(a: A): xml.NodeSeq

  def transform(transformer: xml.NodeSeq => xml.NodeSeq): CrmXmlWriter[A] = CrmXmlWriter[A] { a =>
    transformer(this.write(a))
  }

  def transform(transformer: CrmXmlWriter[xml.NodeSeq]): CrmXmlWriter[A] = CrmXmlWriter[A] { a =>
    transformer.write(this.write(a))
  }
}

trait DefaultCrmXmlWriters {

  import scala.xml._

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

  /**
   * Get a writer without having to use the implicitly syntax.
   */
  def of[A](implicit r: CrmXmlWriter[A]): CrmXmlWriter[A] = r

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

  implicit val queryExpressionWriter: CrmXmlWriter[QueryExpression] = CrmXmlWriter { qe =>
    /**
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
      { qe.columns.write }
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

  implicit def conditionsWriter[T] = new CrmXmlWriter[Seq[ConditionExpression[T]]] {
    def write(ce: Seq[ConditionExpression[T]]) =
      <b:Conditions>
      </b:Conditions>
  }

  /** Uses b namespace. */
  implicit val pagingInfoWriter = new CrmXmlWriter[PagingInfo] {
    def write(p: PagingInfo) =
      <b:PageInfo>
        <b:Count>{ p.count }</b:Count>
        <b:PageNumber>{ p.page }</b:PageNumber>
        <b:PagingCookie>{ p.cookie.getOrElse("") }</b:PagingCookie>
        <b:ReturnTotalRecordCount>
          { p.returnTotalRecordCount }
        </b:ReturnTotalRecordCount>
      </b:PageInfo>
  }

  /** Uses b namespace. */
  implicit val columnsWriter: CrmXmlWriter[ColumnSet] = CrmXmlWriter { c =>
    c match {
      case AllColumns() =>
        <b:ColumnSet><b:AllColumns>true</b:AllColumns></b:ColumnSet>
      case Columns(names) =>
        <b:ColumnSet>
          <b:AllColumns>false</b:AllColumns>
          <b:Columns xmlns:c="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
            { names.map(n => <c:string>{ n }</c:string>) }
          </b:Columns>
        </b:ColumnSet>
    }
  }

  /**
   * Allow you to write `yourobject.write`.
   */
  implicit class RichCrmXmlWriter[A](a: A) {
    def write(implicit writer: CrmXmlWriter[A]) = writer.write(a)
  }

}

object CrmXmlWriter extends DefaultCrmXmlWriters {
  def apply[A](f: A => xml.NodeSeq): CrmXmlWriter[A] = new CrmXmlWriter[A] {
    def write(a: A): xml.NodeSeq = f(a)
  }
  //import play.api.libs.functional._
}
