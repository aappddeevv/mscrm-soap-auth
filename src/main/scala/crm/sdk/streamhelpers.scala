package crm
package sdk

import scala.language._
import scala.util.control.Exception._
import scopt._
import org.w3c.dom._
import dispatch._, Defaults._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.async.Async._
import scala.util._
import org.log4s._
import com.lucidchart.open.xtract.{ XmlReader, _ }
import XmlReader._
import play.api.libs.functional.syntax._
import cats._
import cats.data._
import better.files._
import java.io.{ File => JFile }
import fs2._
import scala.concurrent.ExecutionContext
import scala.util.matching._
import sdk.metadata._
import scala.concurrent.duration._
import fs2.async.immutable.Signal
import org.log4s._
import CrmAuth._

/**
 * Functions that compose streams of values from the underlying
 * CRM server requests.
 */
trait StreamHelpers {

  private[this] lazy val logger = getLogger

  /**
   * Report every `n` units based on a zipWithIndex-like input. Errors
   * from report are not rethrown.
   */
  def reportEveryN[F[_], I](n: Int, report: (I, Int) => F[Unit])(implicit F: fs2.util.Async[F]): Pipe[F, (I, Int), I] =
    pipe.lift {
      case (el, index) =>
        {
          // what happens if report throws?
          if (index % n == 0 && index != 0) F.unsafeRunAsync(report(el, index)) { tup => () }
          el
        }
    }

  import org.apache.commons.lang3.StringEscapeUtils._

  /**
   * Strip quotes, backslashes, replace newlines with stringified versions,
   *  strip non UTF-8 chars and escape anything remaining.
   */
  def cleanString(s: String) =
    escapeCsv(s.replace("\\", "")
        .replace("\"", "")
        .replace("\r", "")
        .replace("\n", "\\n")
        .replaceAll("[^\\u0000-\\uFFFF]", ""))

  /**
   * Pipe that makes CSV output rows. All of the attributes in the
   *  entity are output. If you need fewer attributes, change the entity
   *  prior to this pipe. Slow as anything. For safety, removes
   *  all backslashes and all double quote characters.
   *
   *  TODO: Don't be slow.
   */
  def makeOutputRow(rs: String = ",", eor: String = "\n")(cols: Traversable[String]): Pipe[Task, sdk.Entity, String] =
    pipe.lift { ent =>
      val keys = ent.attributes.keys.toList.sorted
      keys.map(ent.attributes(_).text).map(cleanString(_)).mkString(rs) + eor
    }

  //  def defaultMakeOutputRow(cols: Traversable[String]) = makeOutputRow() _
  val defaultMakeOutputRow = makeOutputRow()(Seq())

  /**
   * Convert a list of strings to list of regexs useful for match
   *  metadata names e.g. logical names. Match will be case insensitive.
   */
  def makeFilters(f: Seq[String]) = f.
    filterNot(_.isEmpty).
    filterNot(_.trim.charAt(0) == '#').
    map(r => new Regex("(?i)" + r.trim))

  /**
   * Given a schema and some filters, find all the attributes that meet the filter
   * spec and map that to all retrievable attributes for that entity.
   */
  def entityAttributeSpec(efilters: Seq[String], schema: CRMSchema): Map[String, Seq[String]] = {
    import scala.util.matching._
    val _allowed = makeFilters(efilters.distinct)

    def allowed(ename: String) =
      if (_allowed.length == 0) true
      else (_allowed.filter(pat => ename match { case pat() => true; case _ => false }).size > 0)

    schema.entities.
      filter(ent => allowed(ent.logicalName)).
      map { ent => (ent.logicalName, ent.retrievableAttributes.map(_.logicalName)) }.toMap
  }

  //(config.leaseTimeRenewalFraction * config.leaseTime)

  /**
   * Stream of valid auths every `d` minutes. Provide a simple "auth"
   *  generator as `f`.
   */
  def auths(f: => Future[CrmAuthenticationHeader], renewalInMin: Int)(implicit strategy: Strategy, scheduler: Scheduler, ec: ExecutionContext) =
    time.awakeEvery[Task](renewalInMin.minutes).evalMap { _ =>
      Task.fromFuture(f)
    }

  type PagingState = (Option[PagingInfo], Boolean)

  /**
   * Create a stream of SOAP envelopes that are the result of a request to the server.
   *  The auths signal should always provide a valid auth when you need it that
   *  has a lifetime that is longer than the time to make the request.
   *
   *  You can create another stream that sets the signal's value with a
   *  valid auth and then combine the "auth" stream with this stream
   *  to create envelopes.
   *
   *  TODO: Factor out the QueryExpression since this is pretty generic iteration.
   */
  def envelopes[T <: Envelope, R](http: HttpExecutor,
    authTokenSignal: Signal[Task, CrmAuthenticationHeader],
    initialState: PagingState,
    query: Option[PagingInfo] => R,
    httpRetrys: Int = 5,
    pauseBetween: Int = 30)(implicit strategy: Strategy, ec: ExecutionContext, reader: XmlReader[T], writer: CrmXmlWriter[R]) = {

    Stream.unfoldEval(initialState) { t =>
      if (!t._2) Task.now(None)
      else {
        val q = query(t._1)
        logger.info(s"Issuing query expression: $q")
        val xml = writer.write(q).asInstanceOf[scala.xml.Elem]
        authTokenSignal.get.flatMap { auth =>
          getMultipleRequestPage(http, xml, auth, t._1, retrys = httpRetrys, pauseInSeconds = pauseBetween).map {
            _ match {
              case Xor.Right((e, pagingOpt, more)) => Some((e, (pagingOpt, more)))
              case Xor.Left(t) => throw t
              case _ => None
            }
          }
        }
      }
    }
  }

  /**
   * Compose a stream that provides auths with a call to get SOAP response Envelopes.
   *  You need to provide an initial auth. The auth stream will always return
   *  a fresh auth and automatically refresh the auth when specified by the parameters.
   *
   *  @param initalState Initial state used to iterate between requests in a MultipleRequest. This
   *  data structure is highly specialized to the need to generate multiple response Envelopes when
   *  the requested data is larger than a single request can hold. The second value should almost
   *  always be true and says, yes you expect more data.
   *  @param getOneAuth Generate a single auth wrapped in a Future.
   *  @param authRenewalInMin Minutes to renew auth. Should be less than lease time of the auth created by getOneAuth
   *  @param query Query generator. Takes an optional PagingInfo and returns some output.
   *  @param reader A reader that can read Envelope objects from an XML response object.
   *  @param writer A writer that can take the output of the query generator and create XML.
   *
   *  TODO: Factor out the dependency on entity and columns and genericize to any type of
   *  XML that should be executed in a RequestExecuteMultple SOAP request.
   */
  def envelopesStream[T <: Envelope, R](http: HttpExecutor,
    initialAuth: CrmAuthenticationHeader, getOneAuth: => Future[CrmAuthenticationHeader], authRenewalInMin: Int,
    initialState: PagingState,
    query: Option[PagingInfo] => R,
    httpRetrys: Int = 5,
    pauseBetween: Int = 30)(
      implicit strategy: Strategy, scheduler: Scheduler, ec: ExecutionContext, reader: XmlReader[T], writer: CrmXmlWriter[R]) = {

    Stream.eval(fs2.async.signalOf[Task, CrmAuthenticationHeader](initialAuth)).flatMap { authTokenSignal =>
      auths(getOneAuth, authRenewalInMin).evalMap(newToken => authTokenSignal.set(newToken)).drain mergeHaltBoth
        envelopes(http, authTokenSignal, initialState, query, httpRetrys, pauseBetween)
    }
  }

  /**
   * Given an input value, create a stream of Envelopes.
   * Assume that the stream elements take a long time to execute.
   * The input is combined with paging state to create a query output that
   * is converted to an XML element and inserted into a CRM MultipleRequest.
   *
   * Query takes both the input and paging information in order to formulate
   * a query that may require paging through multiple pages of results. Even if
   * paging info is None, you need to generate a valid value that can be serialized
   * with the writer.
   *
   * @return A Stream of Envelopes.
   */
  def envelopesFromInput[T <: Envelope, I, R](http: HttpExecutor,
    authTokenSignal: Signal[Task, CrmAuthenticationHeader],
    query: (I, Option[PagingInfo]) => R,
    httpRetrys: Int = 5,
    pauseBetween: Int = 30)(input: I)(implicit strategy: Strategy, ec: ExecutionContext, reader: XmlReader[T], writer: CrmXmlWriter[R]) = {
    val initialState: (Option[PagingInfo], Boolean) = (Some(EmptyPagingInfo), true)
    Stream.unfoldEval(initialState) { t =>
      if (!t._2) Task.now(None)
      else {
        val q = query(input, t._1)
        logger.info(s"Issuing query expression: $q")
        val xml = writer.write(q).asInstanceOf[scala.xml.Elem]
        authTokenSignal.get.flatMap { auth =>
          getMultipleRequestPage(http, xml, auth, t._1, retrys = httpRetrys, pauseInSeconds = pauseBetween).map {
            _ match {
              case Xor.Right((e, pagingOpt, more)) => Some((e, (pagingOpt, more)))
              case Xor.Left(t) => throw t
              case _ => None
            }
          }
        }
      }
    }
  }

}

object StreamHelpers extends StreamHelpers