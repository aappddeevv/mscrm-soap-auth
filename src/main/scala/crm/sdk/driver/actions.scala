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


sealed trait CrmIOAction[+R] {

  import scala.collection.immutable.Vector

  def map[R2](f: R => R2)(implicit ec: ExecutionContext): CrmIOAction[R2] = flatMap[R2](r => SuccessAction[R2](f(r)))

  def flatMap[R2](f: R => CrmIOAction[R2])(implicit ec: ExecutionContext): CrmIOAction[R2] =
    FlatMapAction[R2, R](this, f, ec)

  def flatten[R2](implicit ev: R <:< CrmIOAction[R2]) = flatMap(ev)(CrmIO.sameThreadExecutionContext)

  def andThen[R2](a: CrmIOAction[R2]): CrmIOAction[R2] = a match {
    case AndThenAction(as2) => AndThenAction[R2](this +: as2)
    case a => AndThenAction[R2](scala.collection.immutable.Vector(this, a))
  }

  def zip[R2](a: CrmIOAction[R2]): CrmIOAction[(R, R2)] =
    SequenceAction[Any, ArrayBuffer[Any]](Vector(this, a)).map { r =>
      (r(0).asInstanceOf[R], r(1).asInstanceOf[R2])
    }(CrmIO.sameThreadExecutionContext)

  def zipWith[R2, R3](a: CrmIOAction[R2])(f: (R, R2) => R3)(implicit ec: ExecutionContext): CrmIOAction[R3] =
    SequenceAction[Any, ArrayBuffer[Any]](Vector(this, a)).map { r =>
      f(r(0).asInstanceOf[R], r(1).asInstanceOf[R2])
    }(CrmIO.sameThreadExecutionContext)

  def andFinally(a: CrmIOAction[_]): CrmIOAction[R] = cleanUp(_ => a)(CrmIO.sameThreadExecutionContext)

  def cleanUp(f: Option[Throwable] => CrmIOAction[_], keepFailure: Boolean = true)(implicit ec: ExecutionContext) =
    CleanUpAction[R](this, f, keepFailure, ec)

  def filter(p: R => Boolean)(implicit ec: ExecutionContext): CrmIOAction[R] =
    flatMap(v => if (p(v)) SuccessAction(v) else throw new NoSuchElementException("CrmIOAction.withFilter failed"))

  def collect[R2](pf: PartialFunction[R, R2])(implicit ec: ExecutionContext): CrmIOAction[R2] =
    map(r1 => pf.applyOrElse(r1, (r: R) => throw new NoSuchElementException(s"CrmIOAction.collection partial function is not defined at: $r")))

  def failed: CrmIOAction[Throwable] = FailedAction(this)

  def asTry: CrmIOAction[Try[R]] = AsTryAction[R](this)

}

/**
 * General purpose lifter.
 */
trait ContextualAction[+R, -B <: BasicBackend] extends CrmIOAction[R] { self =>
  /**
   *  Run this action with a context.
   */
  def run(ctx: B#Context): R
}

/**
 * Create some convenience functions to create `IOAction`s. Generally,
 * a layer in between the programmer and the Backend hides the
 * creation of actions.
 */
object ContextualAction {
  /**
   *  Create a patch action
   */
  def apply[R, B <: BasicBackend](f: B#Context => R) = new ContextualAction[R, B] {
    def run(ctx: B#Context) = f(ctx)
  }

  /**
   * Create a patch action from a simple action. It ignores the context.
   */
  def apply[R, B <: BasicBackend](f: => R) = new ContextualAction[R, B] {
    def run(ctx: B#Context) = f
  }
}

case class SuccessAction[+R](value: R) extends ContextualAction[R, BasicBackend] {
  def run(ctx: BasicBackend#Context): R = value
}

case class FailureAction(t: Throwable) extends ContextualAction[Nothing, BasicBackend] {
  def run(ctx: BasicBackend#Context): Nothing = throw t
}

case class FutureAction[+R](f: Future[R]) extends CrmIOAction[R]

case class FlatMapAction[+R, P](base: CrmIOAction[P], f: P => CrmIOAction[R], ec: ExecutionContext) extends CrmIOAction[R]

case class AndThenAction[R](as: IndexedSeq[CrmIOAction[Any]]) extends CrmIOAction[R] {
  override def andThen[R2](a: CrmIOAction[R2]): CrmIOAction[R2] = a match {
    case AndThenAction(as2) => AndThenAction[R2](as ++ as2)
    case a => AndThenAction[R2](as :+ a)
  }
}

case class AsTryAction[+R](a: CrmIOAction[R]) extends CrmIOAction[Try[R]]

case class SequenceAction[R, +R2](as: IndexedSeq[CrmIOAction[R]])(implicit val cbf: CanBuild[R, R2]) extends CrmIOAction[R2]

case class CleanUpAction[+R](base: CrmIOAction[R], f: Option[Throwable] => CrmIOAction[_], keepFailure: Boolean, ec: ExecutionContext) extends CrmIOAction[R]

case class FailedAction(a: CrmIOAction[_]) extends CrmIOAction[Throwable]

object CrmIO {
  def from[R](f: Future[R]): CrmIOAction[R] = FutureAction[R](f)

  def successful[R](v: R): CrmIOAction[R] = SuccessAction[R](v)

  def failed[R](t: Throwable): CrmIOAction[R] = FailureAction(t)

  def fold[T](actions: Seq[CrmIOAction[T]], zero: T)(f: (T, T) => T)(implicit ec: ExecutionContext): CrmIOAction[T] =
    actions.foldLeft[CrmIOAction[T]](CrmIO.successful(zero)) { (za, va) => za.flatMap(z => va.map(v => f(z, v))) }

  def seq(actions: CrmIOAction[_]*): CrmIOAction[Unit] =
    (actions :+ SuccessAction(())).reduceLeft(_ andThen _).asInstanceOf[CrmIOAction[Unit]]

  private[crm] object sameThreadExecutionContext extends ExecutionContext {
    import scala.collection.immutable._

    private[this] val trampoline = new ThreadLocal[List[Runnable]]

    private[this] def runTrampoline(first: Runnable): Unit = {
      trampoline.set(Nil)
      try {
        var err: Throwable = null
        var r = first
        while (r ne null) {
          try r.run() catch { case t: Throwable => err = t }
          trampoline.get() match {
            case r2 :: rest =>
              trampoline.set(rest)
              r = r2
            case _ => r = null
          }
        }
        if (err ne null) throw err
      } finally trampoline.set(null)
    }

    override def execute(runnable: Runnable): Unit = trampoline.get() match {
      case null => runTrampoline(runnable)
      case r => trampoline.set(runnable :: r)
    }

    override def reportFailure(t: Throwable): Unit = throw t
  }

  /**
   * Lift a function that takes a context.
   */
  def withContext[R, B <: BasicBackend](f: B#Context => R) = ContextualAction(f)

  /**
   * Lift a by-name value into an CrmIOAction. This delays the computation until it is needed.
   */
  def lift[R, B <: BasicBackend](f: => R) = ContextualAction(f)

}
