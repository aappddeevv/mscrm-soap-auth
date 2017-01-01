package crm

import scala.language._
import scala.util.control.Exception._
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

import org.asynchttpclient._
import com.lucidchart.open.xtract._
import com.lucidchart.open.xtract.{ XmlReader, _ }
import scala.collection.JavaConverters._

package object sdk {

  /**
   * Error objects representing different types of errors
   * from server responses. This itemizes all of the different
   * types of errors that may be encountered when receiving a CRM SOAP message
   * from a server.
   *
   * @param body The string content of the error.
   */
  sealed abstract class ResponseError {
    def body: String
  }

  /** The response status code was unexpected, the original SOAP request was probably ill-formed. */
  case class UnexpectedStatus(val body: String, code: Int, msg: Option[String] = None) extends ResponseError()

  /** There was an unknown error. */
  case class UnknonwnResponseError(val body: String, msg: String, ex: Option[Throwable] = None) extends ResponseError()

  /**
   * There was en error parsing the response body using the XML parser, which is XML.
   *  This means that the XML was deserialized correctly but the SOAP XML reader could
   *  not create a JVM object from the XML response. The XML was ill-formed.
   */
  case class XmlParseError[T](xml: scala.xml.Elem, parseError: ParseResult[T], msg: Option[String] = None) extends ResponseError() {
    val body = xml.toString
  }

  /**
   * Error on the server related to the request. CRM returns "Fault" payloads
   *  when there is a CRM application level error.
   */
  case class CrmError(val body: String, fault: Fault, msg: Option[String]) extends ResponseError()

  /**
   * Convert a dispatch Response to a string so that the error
   *  objects do not need to carry a Response object directly.
   */
  def rtos(raw: Response) = {
    import scala.collection.JavaConverters._
    val headers = raw.getHeaders.names.asScala.map(k => s"'$k' -> '${raw.getHeader(k)}'").mkString("\n")
    s"""Error during processing. Providing headers and response.
Headers:
$headers    
ResponseBody:
${raw.getResponseBody}
"""
  }

  /**
   * Typeclasses to dump out logging information using standard loggers
   * {{{
   * import errorloggers._
   * implicit val logger = ...
   * ...
   * yourerror.log
   * ...
   * }}}
   */
  object errorloggers {

    trait ErrorLogger[E <: ResponseError] {
      def log_(e: E)(implicit logger: Logger): Unit
    }

    implicit val unexpectedStatusLogger = new ErrorLogger[UnexpectedStatus] {
      def log_(e: UnexpectedStatus)(implicit logger: Logger) = {
        logger.error(s"Unexpected status: ${e.code}")
        e.msg.foreach(logger.error(_))
        logger.error(e.body)
      }
    }

    implicit val unknonwnResponseErrorLogger = new ErrorLogger[UnknonwnResponseError] {
      def log_(e: UnknonwnResponseError)(implicit logger: Logger) = {
        logger.error(s"Unknown error: ${e.msg}")
        e.ex.foreach(logger.error(_)("Exception thrown."))
        logger.error(e.body)
      }
    }

    implicit val xmlParseErrorLogger = new ErrorLogger[XmlParseError[_]] {
      def log_(e: XmlParseError[_])(implicit logger: Logger) = {
        logger.error(s"Parse error while parsing XML using reader: ${e.parseError}")
        logger.error(e.body)
      }
    }

    implicit val crmErrorLogger = new ErrorLogger[CrmError] {
      def log_(e: CrmError)(implicit logger: Logger) = {
        logger.error(s"CRM Server error" + e.fault.message)
        e.msg.foreach(m => logger.error(s"Additional error info: $m"))
        logger.error(e.body)
      }
    }

    implicit class ErrorLoggerSyntax(e: ResponseError) {
      def log(implicit logger: Logger) = {
        e match {
          case x: UnexpectedStatus => unexpectedStatusLogger.log_(x)
          case x: XmlParseError[_] => xmlParseErrorLogger.log_(x)
          case x: CrmError => crmErrorLogger.log_(x)
          case x: UnknonwnResponseError => unknonwnResponseErrorLogger.log_(x)
        }
      }
    }
  }

  /**
   * Convert our ADT into a user message that is hopefully useful. This
   * does not find CRM Fault messages in th response.
   */
  def toUserMessage(error: ResponseError): String = error match {
    case UnexpectedStatus(r, c, msg) => s"""Unexpected status code returned from server: $c"""
    case UnknonwnResponseError(r, m, ex) => s"Unexpected error occurred: $m"
    case XmlParseError(r, e, m) => s"Error parsing response from server" + m.map(": " + _).getOrElse("")
    case CrmError(raw, fault, msgOpt) => "Processing error on server" + msgOpt.map(": " + _).getOrElse("")
  }

  /** Convert a raw response to a string listing headers and the response body. */
  def show(raw: Response) = {
    import scala.collection.JavaConverters._
    val headers = raw.getHeaders.names.asScala.map(k => s"'$k' -> '${raw.getHeader(k)}'").mkString("\n")
    s"""Headers:
$headers    
ResponseBody:
${raw.getResponseBody}
"""
  }

  /**
   * A response body from CRM SOAP has only a few type of possibilities.
   */
  sealed trait ResponseBody

  /**
   * Fault was returned.
   */
  case class Fault(
    errorCode: Int = -1,
    message: String = "A SOAP fault occurred.") extends ResponseBody

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

  /**
   * Ask CRM for one or more entities.
   */
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
   *  CRM may have multiple components to it. A subsequent
   *  transformation process may alter the server value
   *  and create a pure JVM type or another "wrapped" value.
   */
  trait ServerValue {
    /** Raw server representation. */
    def repr: scala.xml.NodeSeq
    /**
     * String representation extracted from the raw server representation.
     *  This value does not reflect type information or conversion.
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

  /** Result of an execute request which is generally key-value pairs. */
  case class ExecuteResult(name: String, results: Map[String, Any]) extends ResponseBody

  /**
   * SOAP response envelope.
   *
   * TODO: Change name so its obvious its a response vs request envelope?
   */
  case class Envelope(header: ResponseHeader, body: ResponseBody)

  /** Basic online authentication information. */
  sealed trait AuthenticationHeader {
    /** Token expiration. */
    def expires: Date
    /** Security token (<Security>...</Security> suitable for a SOAP XML Envelope. */
    def xmlContent: String
    /** URI that identifies the endpoint. It is usually in the form of an URL. */
    def uri: String
  }

  /**
   * Authentication information for creating security headers. The URL
   * that this authentication applies to is also included.
   * @param url URL that the authentication is related to.
   */
  case class CrmAuthenticationHeader( /*Header: scala.xml.Elem = null,*/
    expires: Date,
    xmlContent: String,
    uri: String,
    key: String = "", token1: String = "", token2: String = "") extends AuthenticationHeader

  /** On Prem authentication header. */
  case class CrmOnPremAuthenticationHeader(
    expires: java.util.Date,
    xmlContent: String,
    uri: String,
    key: String,
    token1: String,
    token2: String,
    x509IssuerName: String,
    x509SerialNumber: String,
    signatureValue: String,
    digestValue: String,
    created: String,
    expiresStr: String) extends AuthenticationHeader

  implicit class EnrichedEnvelope(e: Envelope) {
    /**
     * Convert an Envelope to an ExecuteResult or a ResponseError. If the
     *  Envelope is a Fault, the Fault is converted to a ResponseError.
     */
    def toExecuteResult: Either[ResponseError, ExecuteResult] = e match {
      case Envelope(_, er@ExecuteResult(_, _)) => Right(er)
      case Envelope(_, f@Fault(_, _)) => Left(CrmError("response body not available", f, Some("Returned result contained a server fault")))
    }
  }

  /**
   * Hack job...a library should have this somewhere that is much smarter.
   *  This only fixes type signatures but does not translate different
   *  representations. Both access methods throw exceptions if the
   *  expected type is wrong.
   */
  implicit class EnrichedMap(m: Map[String, Any]) {
    /** Get a typed optional value out of the map. You could get a Some(r) or None. */
    def getAs[R](k: String): Option[R] = m.get(k).map(_.asInstanceOf[R])

    /** Get a value directly but throw an exception if the value does not exist. You can get a R or an exception. */
    def as[R](k: String): R = {
      val tmp = m.get(k)
      tmp.map(_.asInstanceOf[R]).getOrElse(throw new RuntimeException(s"Could not convert value $tmp to proper type."))
    }
  }
}
