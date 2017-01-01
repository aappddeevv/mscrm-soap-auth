package crm
package sdk
package driver

import scala.language._
import java.io._
import java.net._
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util._

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers._
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.binary.Base64;
import org.w3c.dom._
import org.xml.sax.SAXException;
import org.log4s._
import com.lucidchart.open.xtract._
import com.lucidchart.open.xtract.{ XmlReader, __ }
import com.lucidchart.open.xtract.XmlReader._
import com.lucidchart.open.xtract._
import play.api.libs.functional.syntax._
import cats._
import cats.data._
import scala.util.control._
import scala.concurrent.Future
import scala.concurrent._
import scala.collection.generic.{ CanBuild, CanBuildFrom }
import scala.collection.mutable.ArrayBuffer
import scala.util._

/**
 * A backend for working with CRM organization endpoints. This
 * allows us to abstract out the specific HTTP subsystem and other
 * design choices.
 *
 */
trait BasicBackend { self =>
  type This >: this.type <: BasicBackend
  type Context >: Null <: BasicActionContext
  type Session >: Null <: SessionDef
  type Org <: OrgDef
  type OrgFactory

  val Org: OrgFactory

  trait ActionContext

  trait OrgDef extends java.io.Closeable { this: Org =>
    /**
     * A session encompasses a variety of concepts, such an authentication
     * window or some type of cache.
     */
    def createSession(): Session

    /** Free all resources. Wait until this has returned. */
    def shutdown: Future[Unit]

    /**
     * Free resources that have been allocated. Synchcronous.
     *  Used for backends that have synchrounous shutdown
     *  procedures
     */
    def close: Unit

    /**
     * Run an action asynchronously and return the result as a future.
     */
    final def run[R](a: CrmIOAction[R]): Future[R] = runInternal(a, false)

    /**
     * Run an action. Non fatal exceptions are mapped to
     * a failed future.
     */
    private[crm] def runInternal[R](a: CrmIOAction[R], useSameThread: Boolean): Future[R] = {
      try {
        runInContext(a, createActionContext(useSameThread), true)
      } catch {
        case NonFatal(e) => Future.failed(e)
      }
    }

    protected[this] def runInContext[R](a: CrmIOAction[R], ctx: Context, topLevel: Boolean): Future[R] = {
      logAction(a, ctx)
      a match {
        case SuccessAction(v) => Future.successful(v)
        case FailureAction(t) => Future.failed(t)
        case FutureAction(f) => f
        case FlatMapAction(base, f, ec) =>
          runInContext(base, ctx, topLevel).flatMap(v => runInContext(f(v), ctx, false))(ctx.getEC(ec))
        case AndThenAction(actions) =>
          val last = actions.length - 1
          def run(pos: Int, v: Any): Future[Any] = {
            val f1 = runInContext(actions(pos), ctx, pos == 0)
            if (pos == last) f1
            else f1.flatMap(run(pos + 1, _))(CrmIO.sameThreadExecutionContext)
          }
          run(0, null).asInstanceOf[Future[R]]

        case sa@SequenceAction(actions) =>
          import java.util.concurrent.atomic.AtomicReferenceArray
          val len = actions.length
          val results = new AtomicReferenceArray[Any](len)
          def run(pos: Int): Future[Any] = {
            if (pos == len) Future.successful {
              val b = sa.cbf()
              var i = 0
              while (i < len) {
                b += results.get(i)
                i += 1
              }
              b.result()
            }
            else runInContext(actions(pos), ctx, pos == 0).flatMap { (v: Any) =>
              results.set(pos, v)
              run(pos + 1)
            }(CrmIO.sameThreadExecutionContext)
          }
          run(0).asInstanceOf[Future[R]]

        case CleanUpAction(base, f, keepFailure, ec) =>
          val p = Promise[R]()
          runInContext(base, ctx, topLevel).onComplete { t1 =>
            try {
              val a2 = f(t1 match {
                case Success(_) => None
                case Failure(t) => Some(t)
              })
            } catch {
              case NonFatal(e) =>
                throw (t1 match {
                  case Failure(t) if keepFailure => t
                  case _ => e
                })
            }
          }(ctx.getEC(ec))
          p.future

        case FailedAction(a) => runInContext(a, ctx, topLevel).failed.asInstanceOf[Future[R]]
        case AsTryAction(a) =>
          val p = Promise[R]()
          runInContext(a, ctx, topLevel).onComplete(v =>
            p.success(v.asInstanceOf[R]))(CrmIO.sameThreadExecutionContext)
          p.future

        case a: ContextualAction[_, _] =>
          runContextualAction(a.asInstanceOf[ContextualAction[R, This]], ctx, !topLevel)

        //        case a: CrmIOAction[_] =>
        //          throw new CrmException("Unknown action $a for $this")
      }
    }

    /**
     * Return the default ExecutionContext for this Org to run actions.
     */
    protected[this] def getExceutionContext: ExecutionContext
    
    protected[this] final def acquireSession(ctx: Context): Unit = {
      // ensure that the state is there to connect, whatever that means...
      if(ctx.session == null) ctx.currentSession = createSession()
    }

    protected[this] final def releaseSession(ctx: Context, discardErrors: Boolean): Unit = {
      ctx.currentSession = null
    }

    /**
     * Run an action with a context. Perform session management, if needed.
     */
    protected[this] def runContextualAction[R](a: ContextualAction[R, This], ctx: Context, topLevel: Boolean): Future[R] = {
      val promise = Promise[R]()
      // add session managaement here....
      try {
        val result = a.run(ctx)
        promise.success(result)
      } catch {
        case NonFatal(e) => promise.tryFailure(e)
      }
      promise.future
    }

    protected[this] def logAction(a: CrmIOAction[_], ctx: Context): Unit = {      
    }    
  }

  /** Factory method to create a new context. */
  protected[this] def createActionContext[T](_useSameThread: Boolean): Context

  trait BasicActionContext extends ActionContext {
    protected[BasicBackend] val useSameThread: Boolean

    private[BasicBackend] def getEC(ec: ExecutionContext): ExecutionContext =
      if (useSameThread) CrmIO.sameThreadExecutionContext else ec

    private[BasicBackend] var currentSession: Session = null

    def session: Session = currentSession
  }

  trait SessionDef extends Closeable {
    def close(): Unit
  }

}

/**
 * Basic context when running actions. Lives outside the cake layer.
 */
trait ActionContext {
}

/**
 * Basic functionality for all Profiles. A Profile helps tease
 * out SOAP vs REST vs version differences and is what is exposed
 * through the API.
 */
trait BasicProfile { self: BasicProfile =>
  type Backend <: BasicBackend
  val backend: Backend

  trait API {
    type Org = Backend#Org
    val Org = backend.Org
    type CrmException = driver.CrmException
    implicit val crmProfile: self.type = self
  }

  /** Import this to get the API for a specific profile/backend combination. */
  val api: API

}

/** Exception for classes in this framework. Most are passed along directly. */
class CrmException(msg: String, parent: Throwable = null) extends RuntimeException(msg, parent)
