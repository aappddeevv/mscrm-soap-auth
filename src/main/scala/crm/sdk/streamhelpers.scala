package crm

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

trait StreamHelpers {
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

  /**
   * Pipe that makes CSV output rows. All of the attributes in the
   *  entity are output. If you need fewer attributes, change the entity
   *  prior to this pipe.
   */
  def makeOutputRow(rs: String = ",", eor: String = "\n"): Pipe[Task, sdk.Entity, String] = pipe.lift { ent =>
    import org.apache.commons.lang3.StringEscapeUtils._
    val keys = ent.attributes.keys.toList.sorted
    keys.map(ent.attributes(_).text).map(escapeCsv(_)).mkString(rs) + eor
  }
  
  val defaultMakeOutputRow = makeOutputRow()

}

object StreamHelpers extends StreamHelpers