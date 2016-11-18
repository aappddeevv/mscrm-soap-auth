package crm

import scala.language._
import scala.util.control.Exception._
import dispatch._, Defaults._
import java.util.Date;
import cats._
import cats.data._
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

import com.ning.http.client.Response
import com.lucidchart.open.xtract._
import com.lucidchart.open.xtract.{ XmlReader, _ }
import sdk.SoapNamespaces.NSMap

package object sdk {

  /**
   * Error objects representing different types of errors
   * from HTTP responses.
   */
  sealed abstract class ResponseError(raw: Response) {
    def body: String = raw.getResponseBody
    def logMsg = {
      import scala.collection.JavaConverters._
      val headers = raw.getHeaders().keySet().asScala.map(k => s"'$k' -> '${raw.getHeader(k)}'").mkString("\n")
      s"""Error during processing. Providing headers and response.
Headers:
$headers    
ResponseBody:
${raw.getResponseBody}
"""
    }

    def log(logger: Logger): Unit
  }

  /** The response status code was unexpected, the request was probably ill-formed. */
  case class UnexpectedStatus(raw: Response, code: Int, msg: Option[String] = None) extends ResponseError(raw) {
    def log(logger: Logger) = {
      logger.error(s"Unexpected status: $code")
      msg.foreach(logger.error(_))
      logger.error(logMsg)
    }
  }

  /** There was an unknown error. */
  case class UnknonwnResponseError(raw: Response, msg: String, ex: Option[Throwable] = None) extends ResponseError(raw) {
    def log(logger: Logger) = {
      logger.error(s"Unknown error: $msg")
      ex.foreach(logger.error(_)("Exception thrown."))
      logger.error(logMsg)
    }
  }

  /** There was en error parsing the response body, which is XML. */
  case class XmlParseError[T](raw: Response, parseError: ParseResult[T], msg: Option[String] = None) extends ResponseError(raw) {
    def log(logger: Logger) = {
      logger.error(s"Parse error: $parseError")
      logger.error(logMsg)
    }
  }

  /** Error on the server related to the request. Request was well formed. */
  case class CrmError(raw: Response, fault: Fault, msg: Option[String]) extends ResponseError(raw) {
    def log(logger: Logger) = {
      logger.error(s"CRM Server error" + fault.message)
      logger.error(s"Additional error info: " + msg.map(": " + _).getOrElse(""))
      logger.error(logMsg)
    }
  }

  /**
   * Exception that captures the response code and the response object.
   * Having the response and response body may allow you to diagnose the bad response
   * faster and easier.
   */
  case class ApiHttpError(code: Int, response: com.ning.http.client.Response)
    extends Exception("Unexpected response status: %d".format(code))

  /**
   * A response body from CRM SOAP has only a few type of possibilities.
   */
  sealed trait ResponseBody

  /**
   * Fault was returned.
   */
  case class Fault(
    errorCode: Int = -1,
    message: String = "A fault occurred.") extends ResponseBody

  case class PagingInfo(count: Int = 0, page: Int = 1, cookie: Option[String] = None,
    returnTotalRecordCount: Boolean = false)
  object EmptyPagingInfo extends PagingInfo()

  sealed trait ColumnSet
  case object AllColumns extends ColumnSet
  case class Columns(names: Seq[String]) extends ColumnSet

  object ColumnSet {
    def apply(cname: String) = Columns(Seq(cname))
    def apply(cnames: String*) = Columns(cnames)
  }

  sealed trait ExprOperator
  case object Equal extends ExprOperator
  case object In extends ExprOperator
  case object NotNull extends ExprOperator
  case object LastXDays extends ExprOperator
  case object Like extends ExprOperator

  case class ConditionExpression[T](attribute: String, op: ExprOperator, values: Seq[T])

  sealed trait Query

  case class QueryExpression(entityName: String, columns: ColumnSet = AllColumns,
    pageInfo: PagingInfo = PagingInfo(), lock: Boolean = false) extends Query

  /**
   * Expression with a fully formed fetch XML fragment. The fragment
   *  is altered with paging information and other enhancements as
   *  needed when the fetch xml query is issued. The element `fetch`
   *  should be the toplevel element. This class is really a "tag'
   *  on an XML Element.
   */
  case class FetchExpression(xml: scala.xml.Elem, pageInfo: PagingInfo = PagingInfo()) extends Query

  object FetchExpression {
    def fromXML(xml: String, pagingInfo: PagingInfo = PagingInfo()) =
      FetchExpression(scala.xml.XML.loadString(xml), pagingInfo)
  }

  case class Endpoint(name: String, url: String)

  case class OrganizationDetail(friendlyName: String,
    guid: String,
    version: String,
    state: String,
    uniqueName: String,
    urlName: String,
    endpoints: Seq[Endpoint] = Nil)

  /**
   * Value returned from the server. A value from the
   *  CRM may have multiple components to it. You should
   *  not think of a value returned from the CRM server
   *  as being a simple value such as an Int. A value
   *  can have multiple parts.
   */
  trait ServerValue {
    /** Raw server representation. */
    def repr: scala.xml.NodeSeq
    /**
     * String representation extracted from the raw server representation.
     *  This value does not reflect type information.
     */
    def text: String
  }

  /** A value that was not provided by the server but still may be processed. */
  object UnsetServerValue extends TypedServerValue("", <missing_></missing_>, "")

  /** A value from the server. It holds raw server data that can be re-interpreted if desired. */
  case class TypedServerValue(text: String, repr: xml.NodeSeq, t: String) extends ServerValue

  /** A value from the server that represets an EntityReference. */
  case class EntityReferenceServerValue(text: String, repr: xml.NodeSeq, logicalName: String) extends ServerValue

  /** A value from an option set. Has <Value> as child. */
  case class OptionSetValue(text: String, repr: xml.NodeSeq) extends ServerValue

  /**
   * Typeclass to expand out a server value to kv map. One attribute could
   *  turn into multiple kv pairs depending on the type of server
   *  value.
   */
  trait ServerValueExpander[A <: ServerValue] {
    def expand(attr: String, value: A): Map[String, String]
  }

  /** Entity is a map of keys to values, pretty much. */
  case class Entity(attributes: Map[String, ServerValue] = collection.immutable.HashMap[String, ServerValue](),
    formattedAttributes: Map[String, String] = collection.immutable.HashMap[String, String]())

  /**
   * A response that holds a collection of Entity objects.
   */
  case class EntityCollectionResult(name: String,
    entities: Seq[Entity] = Nil,
    totalRecordCount: Option[Int],
    limitExceeded: Boolean,
    moreRecords: Boolean,
    pagingCookie: Option[String]) extends ResponseBody

  case class ResponseHeader(action: String, relatedTo: String)

  /**
   * Response envelope. 
   * TODO: Change name so its obvious its a response vs request enveolpe?
   */
  case class Envelope(header: ResponseHeader, body: ResponseBody)

  /**
   *  General purpose XML writer typeclass. Namespaces are handled
   *  via implicits so you can setup a set of namespace abbrevations
   *  for your application.
   */
  trait CrmXmlWriter[-A] {

    def write(a: A)(implicit ns: NSMap): xml.NodeSeq

    def transform(transformer: xml.NodeSeq => xml.NodeSeq)(implicit ns: NSMap) = CrmXmlWriter[A] { a =>
      transformer(this.write(a)(ns))
    }

    def transform(transformer: CrmXmlWriter[xml.NodeSeq])(implicit ns: NSMap) = CrmXmlWriter[A] { a =>
      transformer.write(this.write(a)(ns))(ns)
    }
  }

  object CrmXmlWriter {

    /**
     * Get a writer without having to use the implicitly syntax.
     */
    def of[A](implicit r: CrmXmlWriter[A], ns: NSMap): CrmXmlWriter[A] = r

    /** Create a write using apply syntax. */
    def apply[A](f: A => xml.NodeSeq): CrmXmlWriter[A] = new CrmXmlWriter[A] {
      def write(a: A)(implicit ns: NSMap): xml.NodeSeq = f(a)
    }

    /**
     * Allows you to write `yourobject.write`.
     */
    implicit class RichCrmXmlWriter[A](a: A) {
      def write(implicit writer: CrmXmlWriter[A], ns: NSMap) = writer.write(a)(ns)
    }

  }

  /**
   * Authentication information for creating security headers. The URL
   * that this authentication applies to is also included.
   * @param url URL that the authentication is related to.
   */
  case class CrmAuthenticationHeader(Header: scala.xml.Elem = null, key: String = "", token1: String = "", token2: String = "", Expires: Date = null, url: String = "")

}
