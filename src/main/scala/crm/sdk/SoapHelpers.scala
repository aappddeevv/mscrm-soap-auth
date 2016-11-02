package crm
package sdk

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

trait SoapHelpers {
  private[this] lazy val logger = getLogger

  /**
   * Create a new dispatch http client that honors the config parameters. Do not
   * forget to shut it down after using it.
   * @deprecated
   */
  def client(config: Config) = Http.configure { builder =>
    builder.setRequestTimeoutInMs(config.timeout * 1000)
    builder.setFollowRedirects(true)
    builder.setAllowPoolingConnection(true)
    builder.setCompressionEnabled(true)
    builder.setRequestCompressionLevel(9)
    builder
  }

  /**
   * Create a new asynchttpclient. You should probably let the client select
   * its own thread pool.
   */
  def client(timeoutInSeconds: Int,
    connectionPoolIdelTimeoutInSec: Int = 120,
    ec: Option[java.util.concurrent.ExecutorService] = None) = Http.configure { builder =>
    builder.setRequestTimeoutInMs(timeoutInSeconds * 1000)
    builder.setFollowRedirects(true)
    builder.setAllowPoolingConnection(true)
    builder.setCompressionEnabled(true)
    builder.setRequestCompressionLevel(9)
    ec.foreach(builder.setExecutorService(_))
    builder.setMaxRequestRetry(5)
    builder.setIdleConnectionTimeoutInMs(5 * 60 * 1000)
    builder.setIdleConnectionInPoolTimeoutInMs(5 * 60 * 1000)
    //builder.setIdleConnectionInPoolTimeoutInMs(connectionPoolIdelTimeoutInSec*1000)
    builder
  }

  /**
   * Handler for catching a future TimeoutException. Can be composed with other
   * handlers you need. Has Unit return type.
   */
  def catchTimeout(name: String) = handling(classOf[java.util.concurrent.TimeoutException]) by { t =>
    println(s"Timeout waiting for $name. Check your timeout arguments.")
  }

  /**
   * Wrap a SOAP body in an envelope and add the authentication header. xmlns s is soap-envelope,
   * xmlns a is addressing and u is wssecurty utils.
   */
  def wrap(auth: CrmAuthenticationHeader, soapBody: scala.xml.Elem*) = {
    (<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing">
     </s:Envelope>).copy(child = Seq(auth.Header) ++ soapBody)
  }

  /**
   * Create an Org Services endpoint from a URL that points to the org e.g. https://<yourorg>.crm.dynamics.com.
   * Adds the fixed OrganiaztionService string which should really be extracted from the WSDL.
   * If the url already containst the word XRMServices, no append occurs.
   */
  def endpoint(url: String) = {
    val alreadyHasServices = url.contains("XRMServices")
    if (alreadyHasServices) url
    else {
      url + (if (url.endsWith("/")) "" else "/") + "XRMServices/2011/Organization.svc"
    }
  }

  /** Create a timestamp useful for SOAP messages. */
  def timestamp(minutes: Int = 120) = {
    val now = new Date()
    val createdNow = String.format("%tFT%<tT.%<tLZ", now)
    val createdExpires = String.format("%tFT%<tT.%<tLZ", AddMinutes(minutes, now))
    (createdNow, createdExpires)
  }

  /**
   *
   * @return Date The date with added minutes.
   * @param minutes
   *            Number of minutes to add.-
   * @param time
   *            Date to add minutes to.
   * Retrieve entity metadata.+   * @param aspects Which aspects of the metadata to retrieve.
   * @param retrieveUnPublished Retrieve published or unpublshed. Default is false.
   * @return SOAP body.
   */
  def AddMinutes(minutes: Int, time: Date): Date = {
    val ONE_MINUTE_IN_MILLIS = 60000;
    val currentTime = time.getTime();
    val newDate = new Date(currentTime + (minutes * ONE_MINUTE_IN_MILLIS));
    newDate
  }

  def instantString = {
    import java.time._
    import java.time.format._

    val formatter =
      DateTimeFormatter.ofLocalizedDateTime(FormatStyle.LONG)
        .withZone(ZoneId.systemDefault());
    formatter.format(Instant.now)
  }

  /** Generate a message id SOAP element. Namespace a must map to Contracts. */
  def messageIdEl() = <a:MessageID>urn:uuid:{ java.util.UUID.randomUUID() }</a:MessageID>

  import com.ning.http.client._

  /** Hash a string consistently in this app. */
  def hash(value: String): String = getSHA256Hash(value)

  import javax.xml.bind.DatatypeConverter
  import java.security.MessageDigest
  import java.util.Scanner

  private[crm] def getSHA256Hash(data: String) = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hash = digest.digest(data.getBytes("UTF-8"))
    DatatypeConverter.printHexBinary(hash)
  }

  /** Get the contents of a file whose filename is the token hash. .cache is appended to filename. */
  def getCache(token: String): Option[String] = {
    val filename = (hash(token) + ".cache").toFile
    if (filename.exists) Option(filename!)
    else None
  }

  /** Create a cache file whose filename is the token hash. .cache is appended to filename. */
  def setCache(token: String, content: String): Unit = {
    val filename = hash(token) + ".cache"
    filename.toFile < content
    logger.debug(s"Cache created: $filename")
  }

  /**
   * Allows you to use `Http(req OkWithBody as.xml.Elem)` to obtain
   * the result as an Elem if successful or a ApiHttpError, if an error
   * is thrown.
   */
  implicit class MyRequestHandlerTupleBuilder(req: Req) {
    def OKWithBody[T](f: Response => T) =
      (req.toRequest, new OkWithBodyHandler(f))
  }

  implicit class EnhancedFuture[A](fut: scala.concurrent.Future[A]) {
    /**
     * Unwrap an exception in the Future if its an ExecutionException,
     * otherwise leave the exception alone.
     */
    def unwrapEx: Future[A] =
      fut.recoverWith {
        case x: java.util.concurrent.ExecutionException => Future.failed(x.getCause)
        case x@_ => Future.failed(x)
      }
  }

  import com.ning.http.client._

  /**
   * Apply Response transforming function to the response but return an ApiHttpError
   * if the response code is not Ok.
   */
  class OkWithBodyHandler[T](f: Response => T) extends AsyncCompletionHandler[T] {
    def onCompleted(response: Response) = {
      if (response.getStatusCode / 100 == 2) {
        f(response)
      } else {
        throw ApiHttpError(response.getStatusCode, response)
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
    val headers = raw.getHeaders().keySet().asScala.map(k => s"'$k' -> '${raw.getHeader(k)}'").mkString("\n")
    s"""Headers:
$headers    
ResponseBody:
${raw.getResponseBody}
"""
  }

  /**
   * Parse XML from a raw response using an implicit reader. Implements app standard
   * logging and converts errors to an algebaric data type.
   */
  def processXml[T](response: Response)(implicit reader: XmlReader[T], logger: Logger): ResponseError Xor T = {
    try {
      val body = scala.xml.XML.loadString(response.getResponseBody)
      reader.read(body) match {
        case ParseSuccess(a) => Xor.right(a)
        case PartialParseSuccess(a, errs) =>
          logger.debug("There were non-fatal parsing issues: " + errs)
          Xor.right(a)
        case x@ParseFailure(errs) =>
          logger.error("There are XML parsing issues: " + errs)
          logger.error(s"XML to parse:\n$body")
          Xor.left(XmlParseError(response, x, None))
      }
    } catch {
      case scala.util.control.NonFatal(e) =>
        logger.error(e)("Unknown non fatal error occurred")
        Xor.left(UnknonwnResponseError(response, "Unknown error occurred", ex = Some(e)))
    }
  }

  /**
   * Parse the body as XML using the implicit XmlReader. You could map into the result
   * of course. Parsing occurs on the response processing thread. If you want to control
   * the execution context, map into the future that contains the raw response.
   * This handler also automatically logs parsing issues using at the warning level.
   *                              a
   *
   * TODO: Make logger implicit.
   */
  class OkThenParse[T](implicit reader: XmlReader[T], logger: Logger) extends AsyncCompletionHandler[ResponseError Xor T] {
    def onCompleted(response: Response) = {
      logger.debug(s"Response: ${show(response)}")
      if (response.getStatusCode / 100 == 2) {
        processXml(response)(reader, logger)
      } else {
        Xor.left(UnexpectedStatus(response, response.getStatusCode, None))
      }
    }
  }

  implicit class MyParsingRequestHandlerTupleBuilder(req: Req) {
    def OKThenParse[T](implicit reader: XmlReader[T], logger: Logger) =
      (req.toRequest, new OkThenParse()(reader, logger))
  }

  /** Pipe version of an http executor. */
  def makeRequest(http: HttpExecutor)(implicit s: Strategy,
    ec: scala.concurrent.ExecutionContext): Pipe[Task, Req, Task[Response]] =
    pipe.lift { req =>
      Task.fromFuture(http(req))
    }

  /** Auto convert parse result to an Xor. */
  implicit class ParseResultToXor[A](p: ParseResult[A]) {
    def toXor = p match {
      case ParseSuccess(v) => Xor.right(v)
      case PartialParseSuccess(v, msgs) => Xor.right(v)
      case ParseFailure(errs) => Xor.left(errs)
    }
  }

  /**
   * Augment the values array with any missing schema attributes.
   * Missing values will have a value of `MissingServerValue`
   */
  def augment(values: Map[String, ServerValue], adds: Set[String], schema: sdk.metadata.EntityDescription) = {
    val hasKeys = values.keySet
    val allKeysToAdd = adds intersect schema.retrievableAttributes.map(_.logicalName).toSet
    val missing = allKeysToAdd -- hasKeys
    values ++ missing.map(_ -> UnsetServerValue)
  }

}

object SoapHelpers extends SoapHelpers

import com.ning.http.client.Response
import com.lucidchart.open.xtract._
import com.lucidchart.open.xtract.{ XmlReader, _ }

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

/** There was en error parsing the XML response. */
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

sealed trait ResponseBody

case class Fault(
  errorCode: Int = -1,
  message: String = "A fault occurred.") extends ResponseBody

case class PagingInfo(count: Int = 0, page: Int = 1, cookie: Option[String] = None,
  returnTotalRecordCount: Boolean = false)
object EmptyPagingInfo extends PagingInfo()
sealed trait ColumnSet
case class AllColumns() extends ColumnSet
case class Columns(names: Seq[String]) extends ColumnSet

object ColumnSet {
  def apply(cname: String) = Columns(Seq(cname))
  def apply(cnames: String*) = Columns(cnames)
  val all = AllColumns()
}

sealed trait ExprOperator
case object Equal extends ExprOperator
case object In extends ExprOperator
case object NotNull extends ExprOperator
case object LastXDays extends ExprOperator
case object Like extends ExprOperator

case class ConditionExpression[T](attribute: String, op: ExprOperator, values: Seq[T])

sealed trait Query

case class QueryExpression(entityName: String, columns: ColumnSet = AllColumns(),
  pageInfo: PagingInfo = PagingInfo(), lock: Boolean = false) extends Query

/** Expression with a fully formed fetch XML fragment. The fragment
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

/** Value returned from the server. */
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

/** Entity is a map of keys to values, pretty much. */
case class Entity(attributes: Map[String, ServerValue] = collection.immutable.HashMap[String, ServerValue](),
  formattedAttributes: Map[String, String] = collection.immutable.HashMap[String, String]())

case class EntityCollectionResult(name: String,
  entities: Seq[Entity] = Nil,
  totalRecordCount: Option[Int],
  limitExceeded: Boolean,
  moreRecords: Boolean,
  pagingCookie: Option[String]) extends ResponseBody

case class ResponseHeader(action: String, relatedTo: String)

/**
 * Response envelope.
 */
case class Envelope(header: ResponseHeader, body: ResponseBody)

/** Readers helpful in reading raw XML responses */
object responseReaders {

  /**
   * Impossibly hard due to awful SOAP encodings.
   *  Assumes that the type in the value element has a prefix and requires
   *  a very expensive attribute search :-)
   */
  val obtuseKeyValueReader: XmlReader[(String, ServerValue)] =
    XmlReader { xml =>
      val typeReader = (__ \ "value").read[scala.xml.NodeSeq].map { vx =>
        vx(0).attribute(CrmAuth.NSSchemaInstance, "type") match {
          case Some(Seq(n)) if (n.length > 0) =>
            val text = n(0).text
            val pos = text.indexOf(":")
            if (pos >= 0) text.substring(text.indexOf(":") + 1)
            else text
          case _ => ""
        }
      }
      val k = (__ \ "key").read[String].read(xml)
      val valueReader = (
        (__ \ "value" \ "Id").read[String] orElse
        (__ \ "value" \ "Value").read[String] orElse
        (__ \ "value").read[String])

      val v = (valueReader and
        (__ \ "value").read[scala.xml.NodeSeq] and
        typeReader)(TypedServerValue.apply _).read(xml)

      val r = for {
        kk <- k
        vv <- v
      } yield (kk, vv)
      r
    }

  val stringStringReader: XmlReader[(String, String)] =
    ((__ \ "key").read[String] and
      (__ \ "value").read[String])((_, _))

  /** Apply to the envelope */
  val body: XmlReader[scala.xml.NodeSeq] =
    (__ \ "Body").read

  val fault: XmlReader[scala.xml.NodeSeq] = (__ \ "Fault").read

  val retrieveMultipleResponse: XmlReader[xml.NodeSeq] =
    (__ \ "RetrieveMultipleResponse").read

  val retrieveMultipleResult: XmlReader[xml.NodeSeq] =
    (__ \ "RetrieveMultipleResult").read

  val organizationServiceFault: XmlReader[xml.NodeSeq] =
    (__ \ "OrganizationServiceFault").read

  /** Apply to the envelope. */
  val header: XmlReader[xml.NodeSeq] = (__ \\ "Header").read

  implicit val responseHeaderReader: XmlReader[ResponseHeader] =
    header andThen (
      (__ \\ "Action").read[String] and
      (__ \\ "RelatesTo").read[String])(ResponseHeader.apply _)

  implicit val entityReader: XmlReader[Entity] =
    (
      //XmlReader.pure(HashMap[String, ServerValue]()) and
      (__ \ "Attributes").children.read(seq(obtuseKeyValueReader)).map { v =>
        collection.immutable.HashMap[String, ServerValue]() ++ v
      } and
      (__ \ "FormattedValues" \\ "KeyValuePairOfstringstring").read(seq(stringStringReader)).map { v =>
        collection.immutable.HashMap[String, String]() ++ v
      })(Entity.apply _)

  implicit val entityCollectionResultReader: XmlReader[EntityCollectionResult] =
    ((__ \ "EntityName").read[String] and
      (__ \ "Entities" \\ "Entity").read(strictReadSeq[Entity]).default(Nil) and
      (__ \ "TotalRecordCount").read[Int].optional and
      (__ \ "TotalRecordCountLimitExceeded").read[Boolean] and
      (__ \ "MoreRecords").read[Boolean] and
      (__ \ "PagingCookie").read[String].filter(!_.trim.isEmpty).optional)(EntityCollectionResult.apply _)

  implicit val detail: XmlReader[xml.NodeSeq] = (__ \ "Detail").read

  implicit val faultReader = (
    (__ \ "ErrorCode").read[Int] and
    (__ \ "Message").read[String])(Fault.apply _)

  val reasonReader = (
      XmlReader.pure(-1) and
      (__ \\ "Reason").read[String])(Fault.apply _)
    
  implicit val pagingCookieReader =
    ((__ \\ "PagingCookie").read[String].filter(!_.trim.isEmpty).optional and
      (__ \\ "MoreRecords").read[Boolean] and
      (__ \\ "TotalRecordCount").read[Int])((_, _, _))

  /** Read a key-value */
  implicit val keyValuePairOfstringanyTypeReader =
    ((__ \ "key").read[String] and
      (__ \ "value" \ "Value").read[String])((_, _))

  implicit val seqKVReader =
    (__ \\ "KeyValuePairOfstringanyTypeReader").
      read(seq(keyValuePairOfstringanyTypeReader))

  implicit val readEndpoint: XmlReader[Endpoint] = (
    (__ \ "key").read[String] and
    (__ \ "value").read[String])(Endpoint.apply _)

  implicit val readOrganizationDetail: XmlReader[OrganizationDetail] = (
    (__ \ "FriendlyName").read[String] and
    (__ \ "OrganizationId").read[String] and
    (__ \ "OrganizationVersion").read[String] and
    (__ \ "State").read[String] and
    (__ \ "UniqueName").read[String] and
    (__ \ "UrlName").read[String] and
    (__ \ "Endpoints" \ "KeyValuePairOfEndpointTypestringztYlk6OT").read(seq[Endpoint]).default(Seq()))(OrganizationDetail.apply _)

  implicit val readSeqOrganizationDetail: XmlReader[Seq[OrganizationDetail]] = (__ \\ "OrganizationDetail").read(seq[OrganizationDetail])

  /**
   * Reader that reads a Fault or the results of a RetrieveMultipleRequest.
   */
  val retrieveMultipleRequestReader =
    (responseHeaderReader and
      (body andThen (
        (fault andThen ((detail andThen organizationServiceFault andThen faultReader) or reasonReader)) or
        (retrieveMultipleResponse andThen retrieveMultipleResult andThen entityCollectionResultReader))))(Envelope.apply _)

}
  