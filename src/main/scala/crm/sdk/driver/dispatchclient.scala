package crm
package sdk
package driver

import fs2._
import scala.concurrent._
import scala.concurrent.duration._
import org.log4s._
import sdk.messages._
import com.lucidchart.open.xtract._
import cats._
import cats.data._
import cats.implicits._
import sdk.httphelpers._
import sdk.metadata.soapwriters._
import sdk.soapnamespaces.implicits._
import sdk.metadata._
import sdk.metadata.xmlreaders._
import sdk.soapreaders._
import sdk.CrmAuth._
import sdk.messages.soaprequestwriters._
import scala.util._
import org.asynchttpclient._



/**
 * The client side API provides methods for calling a CRM online server using
 * a request and response model but using CRM domain objects for the request
 * and response abstractions. The client handles the underlying http connections
 * and authentication. Each connection targets a single host.
 */
final case class DefaultClient(conn: Service[xml.Elem, Response], shutdown: Task[Unit]) extends Client {

  implicit private lazy val logger = getLogger

  def execute(request: OrgRequest): RequestResult[ExecuteResult] = {
    request match {
      case r: RetrieveAllEntitiesRequest =>
        val soaprequest = CrmXmlWriter.of[RetrieveAllEntitiesRequest].write(r).asInstanceOf[xml.Elem]
        implicit val reader = executeRequestResponseReader
        val f: xml.Elem => Either[ResponseError, ExecuteResult] =
          elem => processXml[Envelope](elem)(reader, logger).flatMap(_.toExecuteResult)
        EitherT(conn(soaprequest).map { r => responseToXml(r).flatMap(f) })

      case r: UpdateRequest =>
        val soaprequest = executeTemplate(updateRequestTemplate(r.entity, r.id, r.parameters))
        implicit val reader = executeRequestResponseReader
        val f: xml.Elem => Either[ResponseError, ExecuteResult] =
          elem => processXml[Envelope](elem)(reader, logger).flatMap(_.toExecuteResult)
        EitherT(conn(soaprequest).map { r => responseToXml(r).flatMap(f) })

      case r: CreateRequest =>
        val soaprequest = executeTemplate(createRequestTemplate(r.entity, r.parameters))
        implicit val reader = executeRequestResponseReader
        val f: xml.Elem => Either[ResponseError, ExecuteResult] =
          elem => processXml[Envelope](elem)(reader, logger).flatMap(_.toExecuteResult)
        EitherT(conn(soaprequest).map { r => responseToXml(r).flatMap(f) })

      case r: DeleteRequest =>
        val soaprequest = executeTemplate(deleteRequestTemplate(r.entity, r.id))
        implicit val reader = executeRequestResponseReader
        val f: xml.Elem => Either[ResponseError, ExecuteResult] =
          elem => processXml[Envelope](elem)(reader, logger).flatMap(_.toExecuteResult)
        EitherT(conn(soaprequest).map { r => responseToXml(r).flatMap(f) })

      case r: WhoAmIRequest =>
        import Auth._
        val soaprequest = executeTemplate(whoAmIBodyTemplate)
        implicit val reader = executeRequestResponseReader
        val f: xml.Elem => Either[ResponseError, ExecuteResult] =
          elem => processXml[Envelope](elem)(reader, logger).flatMap(_.toExecuteResult)
        EitherT(conn(soaprequest).map { r => responseToXml(r).flatMap(f) })

      case r: AssociateRequest =>
        val soaprequest = executeTemplate(associateRequestTemplate("Associate", r.source, r.relationship, r.to))
        implicit val reader = executeRequestResponseReader
        val f: xml.Elem => Either[ResponseError, ExecuteResult] =
          elem => processXml[Envelope](elem)(reader, logger).flatMap(_.toExecuteResult)
        EitherT(conn(soaprequest).map { r => responseToXml(r).flatMap(f) })

      case r: DisassociateRequest =>
        val soaprequest = executeTemplate(associateRequestTemplate("Disassociate", r.source, r.relationship, r.to))
        implicit val reader = executeRequestResponseReader
        val f: xml.Elem => Either[ResponseError, ExecuteResult] =
          elem => processXml[Envelope](elem)(reader, logger).flatMap(_.toExecuteResult)
        EitherT(conn(soaprequest).map { r => responseToXml(r).flatMap(f) })

      case _ => throw new RuntimeException("Unsupported request type.")
    }
  }

  def shutdownNow(): Unit = shutdown.unsafeAttemptRunSync()
}


/*
object DispatchClient {

  implicit private val logger = getLogger

  private def mkReq(e: xml.Elem) = dispatch.Req(_.setBody(e.toString))

  /**
   * Stream of valid auths every `d` minutes. Provide a simple "auth"
   *  generator as `f`.
   */
  def auths(f: => scala.concurrent.Future[CrmAuthenticationHeader], renewalInMin: Int)(implicit strategy: Strategy, scheduler: Scheduler, ec: ExecutionContext) =
    time.awakeEvery[Task](renewalInMin.minutes).evalMap { _ => Task.fromFuture(f) }

  /** Convenience function to get the auth. */
  def orgAuthF(http: HttpExecutor, config: Config)(implicit ec: ExecutionContext) = {
    logger.info("Renewing org auth.")
    orgServicesAuthF(http, config.username, config.password, config.url, config.region, config.leaseTime)
  }

  def renewalTimeInMin(config: Config) = (config.leaseTimeRenewalFraction * config.leaseTime).toInt

  //  def authSignal(config: Config, initial: CrmAuthenticationHeader)(implicit s: Strategy): Task[Signal[CrmAuthenticationHeader]] =
  //    Stream.eval(fs2.async.signalOf[Task, CrmAuthenticationHeader](orgAuth)).flatMap { authTokenSignal =>
  //      auths(orgAuthF(config), renewalTimeInMin(config)).evalMap(newToken => authTokenSignal.set(newToken))
  //    }

  /**
   * Create a driver from a config. Immediately calls some discovery
   *  services to obtain various endpoint information and sets up
   *  a scheduled task to obtain fresh auths.
   */
  def fromConfig(config: Config)(implicit ec: ExecutionContext): DefaultClient = {

    import java.util.concurrent._
    val http = httphelpers.client(config.timeout)
    implicit val s = Strategy.fromExecutionContext(ec)
    implicit val scheduler = Scheduler.fromFixedDaemonPool(2, "auth-scheduler")

    // Must get services URL, so we need to run discovery call.
    // For the client, throw exceptions if we cannot even get the org services URL to start with...
    val urlF = discoveryAuth(http, config.username, config.password, config.region, config.leaseTime).flatMap { authorerror =>
      authorerror match {
        case Right(auth) => orgServicesUrl(http, auth, config.url)
        case Left(errmsg) => throw new RuntimeException(s"Unable to access discovery services: $errmsg")
      }
    }
    val dataurl = Await.result(urlF, config.timeout seconds) match {
      case Right(url) => url
      case Left(msg) => throw new RuntimeException(s"Unable to obtain organization data services url: $msg")
    }

    def authF = {
      logger.info("Renewing org auth.")
      orgServicesAuthF(http, config.username, config.password, dataurl, config.leaseTime)
    }
    val initialAuth = Await.result(orgServicesAuthF(http, config.username, config.password, dataurl, config.leaseTime), config.timeout seconds)
    val authSignal = async.signalOf[Task, CrmAuthenticationHeader](initialAuth)
    val interruptSignal = async.signalOf[Task, Boolean](false).unsafeRun
    val authStream = auths(authF, config.leaseTime).evalMap(newAuth => authSignal.flatMap(_.set(newAuth))).interruptWhen(interruptSignal)

    val conn: Service[xml.Elem, Response] = Kleisli { el =>
      Task.fromFuture {
        orgServicesAuthF(http, config.username, config.password, dataurl, config.leaseTime).flatMap { auth =>
          val authel = addAuth(el, auth)
          val modified = url(endpoint(dataurl)).secure.POST
            .setContentType("application/soap+xml", "utf-8") << authel.toString
          logger.debug(s"CRM request body: ${authel.toString}")
          logger.debug(s"CRM request: ${modified.toRequest.toString}")
          val f = http(modified)
          f onComplete { t =>
            t match {
              case Success(r) =>
                logger.debug(s"CRM response: ${r.getResponseBody}")
              case Failure(t) =>
                logger.error(t)(s"Error in CRM response: $t.getMessage")
            }
          }
          f
        }
      }
    }

    val shutdownTask = Task.delay {
      http.shutdown
      interruptSignal.set(true).unsafeRun
    }

    DefaultClient(conn, shutdownTask)
  }

}
*/