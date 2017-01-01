package crm
package sdk

import scala.language._

import org.log4s._
import cats._
import cats.data._
import scala.util.control._
import scala.concurrent._
import fs2._
import sdk.messages._
import org.asynchttpclient._
import org.asynchttpclient.request.body._
import org.asynchttpclient.request.body.generator._
import _root_.io.netty.buffer._
import Body._
import scala.concurrent._
import scala.util.control.Exception._
import collection.JavaConverters._
import org.asynchttpclient._
import retry._
import scala.concurrent.duration._

package object driver {

  private lazy val logger = getLogger

  /**
   * A request result is Task with either a ResponseError or
   *  the output value.
   */
  type RequestResult[A] = EitherT[Task, ResponseError, A]

  /** Wrapper around a function. */
  type Service[A, B] = Kleisli[Task, A, B]

  trait Client {
    /**
     * Execute a single request.
     *
     */
    def execute(request: OrgRequest): RequestResult[ExecuteResult]

    /** Free any resources. Blocks until completed. */
    def shutdownNow(): Unit
  }

  /** Complete set of errors returned from a SoapConnection. */
  sealed abstract class SoapError(msgs: NonEmptyList[String]) extends RuntimeException(msgs.toList(0))
  /** Error at the HTTP layer e.g. wrong status returned or unable to reach the server. */
  case class HttpError(msgs: NonEmptyList[String]) extends SoapError(msgs)
  /** Error parsing the SOAP http response into XML. */
  case class ResponseParseError(msgs: NonEmptyList[String]) extends SoapError(msgs)
  /**
   * Error on the server due to the SOAP message. SOAP protocol level. A Fault
   *  may or may not be accompanied by a bad HTTP status as well.
   */
  case class FaultError(msgs: NonEmptyList[String], fault: Option[Fault] = None) extends SoapError(msgs)
  /** Error in converting XML to caller's domain object. */
  case class HandlerError(msgs: NonEmptyList[String]) extends SoapError(msgs)

  /** Simple wrapper to tag an XML element as a SOAP envelope. */
  case class InputEnv(content: scala.xml.Elem)

  /** A wrapped response that can process a dispose action. */
  final case class DisposableResponse(response: Response, dispose: Task[Unit]) {
    /** Apply a handler and automatically call the `dispose` method`. */
    def apply[A](handler: Response => Task[A]): Task[A] = {
      val task = try handler(response) catch {
        case scala.util.control.NonFatal(e) => Task.fail(e)
      }
      task.attempt.flatMap(result => dispose.flatMap(_ => result.fold(Task.fail, Task.now)))
    }
  }

  /**
   * A SoapConnection is a low-level object for composing and sending
   *  SOAP messages to an endpoint. Modelled after http4s. It can also
   *  provide a basic HTTP service for custom calls.
   *
   *  @param open Function to obtain a Response from a Request. The Request
   *  may be munged by the underlying service creator so check those docs.
   *  @param dispose Function to call to dispose of this connection.
   */
  class SoapConnection(open: Service[Request, DisposableResponse], auths: fs2.async.immutable.Signal[Task, AuthenticationHeader], dispose: Task[Unit]) {
    import AsyncHttpClient._
    import crm.sdk.soapnamespaces.implicits._
    import crm.sdk.messages.soaprequestwriters._

    implicit def soapRequestToRequest(env: SoapRequest): Request = {
      val authenv = env.withSecurity(auths.get.unsafeRun)
      val httpBody = toSoapEnvelope(authenv)
      //println("httpBody: " + httpBody)
      new RequestBuilder("POST")
        .setBody(httpBody)
        .build()
    }

    /** Create the SOAP request. Execute it by running the task. */
    def fetch[A](env: SoapRequest)(handler: Response => Task[A])(implicit F: Monad[Task]): Task[A] = {
      open(env).flatMap(_.apply(handler))
    }

    /**
     * Use this at your call sites if the handler is always the same or use
     *  this to compose into other Kleisli functions using a raw Request object.
     */
    def toHttpService[A](handler: Response => Task[A])(implicit F: FlatMap[Task]): Service[Request, A] = {
      open.flatMapF(_.apply(handler))
    }

    /** Shutdown. Block until shutdown. */
    def shutdownNow(): Unit = dispose.unsafeRun()
  }

  /** Factory methods for creating a SoapConnection. */
  object SoapConnection {
    /**
     * Create a new Connection from the Config object. Request objects
     * are probed to see if the method and content-type have been set
     * and if not, set them to POST and "application/soap+xml" respectively.
     * The target URL of a request is set if it is not already set.
     * A connection automatically retries failed requests based on http
     * such as a bad status or unable to reach the server but it does not retry
     * on SOAP Faults, which typically indicate some issue at the SOAP
     * message composition or domain level vs transport or request level.
     * Security tokens are automatically added to the Envelope>Header
     * element if the body is XML.
     *
     *  @param config Configuration information for the connection.
     *  @param url Override the url in config.
     */
    def fromConfig(config: Config, url: Option[String] = None)(implicit ec: ExecutionContext): SoapConnection = {
      import fs2.interop.cats._

      require(config.httpRetrys >= 0)
      require(config.pauseBetweenRetriesInSeconds >= 0)

      implicit val s = Strategy.fromExecutionContext(ec)
      implicit val scheduler = Scheduler.fromFixedDaemonPool(2, "soap-connection-scheduler")
      implicit val F = fs2.util.Async[Task]
      val (httpsvc, closehttp) = AsyncHttpClientHttpService.fromConfig(config, None)
      val ct = "Content-Type" -> Seq("application/soap+xml", "charset=UTF-8")
      val urlX = (url orElse Option(config.url.trim)).filterNot(_.isEmpty)

      val stopAuths = async.signalOf[Task, Boolean](false).unsafeRun
      def getAuth(): Task[AuthenticationHeader] = config.auth match {
        case crm.sdk.client.Online => Task.now(null) // GetHeaderOnline()
        case crm.sdk.client.Federation =>
          val a = CrmAuth.GetHeaderOnPremise(config.username, config.password, config.url, httpsvc andThen toXml, config.leaseTimeInMin)
          logger.debug("Obtained auth")
          a
      }
      val authSignal = async.signalOf[Task, AuthenticationHeader](getAuth().unsafeRun).unsafeRun
      val renewAuthF = stopAuths.interrupt(time.awakeEvery(config.leaseTimeInMin * config.leaseTimeRenewalFraction minutes).evalMap(_ => getAuth().flatMap(auth => authSignal.set(auth)))).run.unsafeRunAsyncFuture

      val open = Kleisli[Task, Request, DisposableResponse] { (req: Request) =>
        val method = Option(req.getMethod()).getOrElse("POST")
        val targeturl = (Option(req.getUrl()).filterNot(_.isEmpty).filterNot(_.contains("localhost")) orElse urlX).getOrElse(throw new RuntimeException("No remote system URL specified."))
        val rb = new RequestBuilder(req)
          .setUrl(targeturl)
          .setMethod(method)
          .addHeader(ct._1, ct._2.mkString("; "))
        httpsvc(rb.build).map { resp => DisposableResponse(resp, Task(())) }
      }

      val close = stopAuths.set(true).flatMap(_ => closehttp)
      new SoapConnection(open, authSignal, close)
    }
  }

  /**
   * Return a simple HTTP only service and a callback to free any resources
   * once you are done with the service function.
   *  The connection does not set a default
   *  URL so either the URL must be set or the host header value must be set.
   */
  object AsyncHttpClientHttpService {
    /**
     * Create an service from a config. The Kleisli automatically
     *  retries a request that fails.
     *
     * @param config Application level config
     * @param ahcConfig Optionally provide a completed specified AsyncHttClient config instead of deriving it from config.
     * @param ec ExecutionContext for Future operations. Strategy for fs2's Task is also derived from it.
     */
    def fromConfig(config: Config, ahcConfig: Option[AsyncHttpClientConfig])(implicit ec: ExecutionContext): (Service[Request, Response], Task[Unit]) = {
      implicit val timer = odelay.netty.NettyTimer.newTimer
      implicit val s = Strategy.fromExecutionContext(ec)
      val _ahcConfig = ahcConfig getOrElse {
        new DefaultAsyncHttpClientConfig.Builder()
          .setRequestTimeout(-1)
          .setFollowRedirect(true)
          .setUserAgent("crmauth/0.1.0")
          .setMaxConnections(400)
          //.setThreadFactor(...) // rename the threads so can identify them...
          .build()
      }
      val c = new DefaultAsyncHttpClient(_ahcConfig)
      val close = Task.delay {
        c.close()
      }

      val svc = Kleisli[Task, Request, Response] { (req: Request) =>
        Task.fromFuture {
          def doReq(): Future[Response] = {
            nonFatalCatch withApply { t => Promise.failed(t).future } apply {
              val promise = Promise[Response]()
              logger.debug("Request: " + req)
              logger.debug("Request body: " + req.getStringData)
              val lfut = c.executeRequest(req, asyncHandler)
              lfut.toCompletableFuture().whenComplete { (r: Response, t: Throwable) =>
                if (t != null) {
                  { translateApiHttpErrorToSoapError andThen promise.failure } applyOrElse (t, promise.failure)
                } else {
                  logger.debug("Response: " + r)
                  //logger.debug("Response Body: " + r.getResponseBody)
                  promise.success(r)
                }
              }
              promise.future
            }
          }
          // We rely on a Future to hold on an exception for error management, so...
          // we need to use the long syntax since failure is not carried in the
          // value algebra directly.
          retry.Pause(config.httpRetrys, config.pauseBetweenRetriesInSeconds seconds)(timer) { () => doReq() }(Success.always, ec)
        }
      }
      (svc, close)
    }
  }

  /**
   * Return the response if status=200, otherwise throw an ApiHttpError.
   * The caller may want to translate the ApiHttpError to another error.
   */
  val asyncHandler = new AsyncCompletionHandler[Response] {
    def onCompleted(response: Response) = {
      if (response.getStatusCode / 100 == 2) {
        response
      } else {
        logger.debug("Unexpected response status: " + response.getStatusCode)
        logger.debug("Response: " + response)
        throw httphelpers.ApiHttpError(response.getStatusCode, response)
      }
    }
  }

  /**
   * PF for translating ApiHttpError exception into SoapError.
   */
  val translateApiHttpErrorToSoapError: PartialFunction[Throwable, SoapError] = {
    case httphelpers.ApiHttpError(code, resp) =>
      val body = resp.getResponseBody
      if (body.trim.size > 0) {
        catching(classOf[org.xml.sax.SAXParseException]) withApply { t =>
          ResponseParseError(NonEmptyList.of(
            s"Unexpected HTTP staus $code",
            s"Unable to parse XML body while parsing for a SOAP Fault: ${body}"))
        } apply {
          val xml = scala.xml.XML.loadString(body)
          val fault = crm.sdk.soapreaders.simpleFaultResponseReader.read(xml).
            getOrElse(Fault(-1, s"Unable to read fault object from xml: $xml"))
          FaultError(NonEmptyList.of(s"${fault.message}"), Option(fault))
        }
      } else {
        new HttpError(NonEmptyList.of(s"Unexpected HTTP status: ${code}"))
      }
  }

  /**
   * Convert the string body of the response into an XML object.
   *  Return a failed Task with ResponseParseError if SAXParseException is thrown.
   */
  val toXml = Kleisli[Task, Response, xml.Elem] { (r: Response) =>
    Task.delay {
      try {
        xml.XML.loadString(r.getResponseBody)
      } catch {
        case x: org.xml.sax.SAXParseException => throw new ResponseParseError(NonEmptyList.of(x.getMessage, r.getResponseBody))
        case x@e => throw new ResponseParseError(NonEmptyList.of(x.getMessage, r.getResponseBody))
      }
    }
  }

  /** A piece of content is either a string (left) or an XML element (right). */
  type Content = Either[String, scala.xml.NodeSeq]

  /** Convert a sequence of Content items to a string. */
  def contentToString(c: Seq[Content]) = c.map(_.fold(identity, _.toString)).mkString("")

//  /**
//   * Body class that keeps the payload as a mix of XML or strings
//   * until the last moment. This allows us to manipulate the payload in
//   * an asynchttpclient filter. Keep it loose until it needs to be serialized
//   * into a message.
//   */
//  protected[driver] case class XmlBody(content: Seq[Content]) extends Body {
//
//    val bytes = contentToString(content).getBytes
//    var eof: Boolean = false
//
//    def getContentLength(): Long = bytes.size
//
//    def transferTo(target: ByteBuf): BodyState = {
//      target.writeBytes(bytes)
//      BodyState.STOP
//    }
//    def close(): Unit = {}
//  }
//
//  case class FlexiBodyG(content: Seq[Content]) extends BodyGenerator {
//    def this(xml: scala.xml.Elem) = this(Seq(Right(xml)))
//    def this(str: String) = this(Seq(Left(str)))
//    val _content = XmlBody(content)
//    def createBody(): Body = _content
//  }

  /**
   * WS Addressing:
   *  <ul>
   *  <li>https://www.w3.org/TR/2005/CR-ws-addr-core-20050817</li>
   *  <li>https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509</li>
   * </ul>
   */
  case class SoapAddress(
    /** URI */
    to: String = "http://www.w3.org/2005/08/addressing/anonymous",
    /** URI */
    action: String,
    /** URI */
    replyToAddress: String = "http://www.w3.org/2005/08/addressing/anonymous",
    /** URI */
    messageId: String = java.util.UUID.randomUUID().toString)

  val wsaNS = "http://www.w3.org/2005/08/addressing"

  /**
   * SOAP request broken down into pieces.
   *
   *  @param address SoapAddress for header
   *  @param security Optional security block.
   *  @param body The element that goes inside the <Body> element.
   */
  case class SoapRequest(address: SoapAddress, body: Content, security: Option[String]) {
    def withSecurity(auth: AuthenticationHeader): SoapRequest = withSecurity(auth.xmlContent)
    def withSecurity(xmlContent: String): SoapRequest = this.copy(security = Option(xmlContent))
  }

  case class AddressedSoapRequest(address: SoapAddress) {
    def withBody(xml: scala.xml.NodeSeq): SoapRequest = SoapRequest(this.address, Right(xml), None)
    def withBody(str: String): SoapRequest = SoapRequest(this.address, Left(str), None)
  }

  object SoapRequest {
    /** Create a SOAP request given the `to` and `action` elements. */
    def apply(uri: String, action: String) = to(uri, action)

    /** Create a SOAP request given the `to` and `action` elements. */
    def to(uri: String, action: String): AddressedSoapRequest =
      AddressedSoapRequest(SoapAddress(to = uri, action = action))
  }

}

