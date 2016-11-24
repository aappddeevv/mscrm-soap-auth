package crm
package sdk

import scala.language._
import scala.util.control.Exception._
import dispatch._, Defaults._
import java.util.Date;
import cats._
import cats.data._
import cats.syntax._
import cats.implicits._
import org.log4s._
import better.files._
import fs2._
import com.lucidchart.open.xtract._
import com.lucidchart.open.xtract.{ XmlReader, __ }
import com.lucidchart.open.xtract.XmlReader._
import com.lucidchart.open.xtract._
import play.api.libs.functional.syntax._
import scala.language.implicitConversions

trait httphelpers {
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
    if (filename.exists) { logger.debug(s"Cache hit for token: $token, $filename"); Option(filename!) }
    else { logger.debug(s"No cache hit for token: $token, $filename"); None }
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

  /** Automatically unwrap an ExecutionException to get to the inner exception. */
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
   * Exception that captures the response code and the response object.
   * Having the response and response body may allow you to diagnose the bad response
   * faster and easier.
   */
  case class ApiHttpError(code: Int, response: com.ning.http.client.Response)
    extends Exception("Unexpected response status: %d".format(code))

  /** Convert a dispatch Response to XML. */
  def responseToXml(response: Response): Either[ResponseError, xml.Elem] = {
    val body = response.getResponseBody
    Either.catchNonFatal { scala.xml.XML.loadString(body) }.leftMap(t => UnknonwnResponseError(rtos(response), "Unable to deserialize XML payload.", Some(t)))
  }

  /**
   * Parse XML from a raw response using an implicit reader. Implements app standard
   * logging and converts errors to an algebraic data type.
   * Parsing errors are non-fatal errors are captured and converted to a Left.
   * Because `T` is generic, this function does not translate SOAP Fault's
   * into Lefts.
   *
   * @param T The expected return type.
   * @param reader A reader of the XML payload that returns type T.
   * @param logger Logger instance.
   * @return ResponseError object or a value of type T
   */
  def processXml[T](body: xml.Elem)(implicit reader: XmlReader[T], logger: Logger): Either[ResponseError, T] = {
    reader.read(body) match {
      case ParseSuccess(a) => Right(a)
      case PartialParseSuccess(a, errs) =>
        logger.debug("There were non-fatal parsing issues: " + errs)
        Right(a)
      case x@ParseFailure(errs) =>
        logger.error("There are XML parsing issues: " + errs)
        logger.error(s"XML to parse:\n$body")
        Left(XmlParseError(body, x, None))
    }
  }

  /**
   * An AsyncCompletionHandler specific to AHC that parses the body as XML using
   * the implicit XmlReader. Parsing occurs on the response processing thread. If you want to control
   * the execution context, map into the future that contains the raw response.
   */
  class OkThenParse[T](implicit reader: XmlReader[T], logger: Logger) extends AsyncCompletionHandler[Either[ResponseError, T]] {
    def onCompleted(response: Response) = {
      logger.debug(s"Response: ${show(response)}")
      if (response.getStatusCode / 100 == 2) {
        responseToXml(response).flatMap(processXml(_)(reader, logger))
      } else {
        Left(UnexpectedStatus(rtos(response), response.getStatusCode, None))
      }
    }
  }

  /** Allows OKThenParse to be used in the HttpExecutor fluently. */
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

  /** Auto convert parse result to an Either. */
  implicit class ParseResultToEither[A](p: ParseResult[A]) {
    def toEither = p match {
      case ParseSuccess(v) => Right(v)
      case PartialParseSuccess(v, msgs) => Right(v)
      case ParseFailure(errs) => Left(errs)
    }
  }

  /**
   * Augment the values array with any missing schema attributes.
   * Missing values will have a value of `MissingServerValue`
   */
  def augment(values: Map[String, ServerValue], adds: Set[String], schema: metadata.EntityDescription) = {
    val hasKeys = values.keySet
    val allKeysToAdd = adds intersect schema.retrievableAttributes.map(_.logicalName).toSet
    val missing = allKeysToAdd -- hasKeys
    values ++ missing.map(_ -> UnsetServerValue)
  }
}

object httphelpers extends httphelpers

/** Set of expander implicits and an importable syntax. */
object expanders {

  implicit val unsetServerValueExpander = new ServerValueExpander[UnsetServerValue.type] {
    def expand(attr: String, value: UnsetServerValue.type) = Map(attr -> "")
  }

  implicit val typedServerValueExpander = new ServerValueExpander[TypedServerValue] {
    def expand(attr: String, v: TypedServerValue) = Map(attr -> v.text)
  }

  implicit val erServerValueExpander = new ServerValueExpander[EntityReferenceServerValue] {
    def expand(attr: String, v: EntityReferenceServerValue) = Map(attr -> v.text, attr + "_target" -> v.logicalName)
  }

  implicit val osValueExpander = new ServerValueExpander[OptionSetValue] {
    def expand(attr: String, v: OptionSetValue) = Map(attr -> v.text)
  }

  object syntax {
    implicit class ExpandedServerValue[A <: ServerValue](sv: A) {
      def expand(attr: String)(implicit expander: ServerValueExpander[A]) =
        expander.expand(attr, sv)
    }
  }

  /** Expand any server value. */
  def expand(attr: String, sv: ServerValue): Map[String, String] = {
    import syntax._
    sv match {
      case s: UnsetServerValue.type => s.expand(attr)
      case s: TypedServerValue => s.expand(attr)
      case s: EntityReferenceServerValue => s.expand(attr)
      case s: OptionSetValue => s.expand(attr)
    }
  }
}
