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
import scala.util.matching._
import sdk.metadata._

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
    keys.map(ent.attributes(_).text).map(escapeCsv(_).replace("\r","").replace("\n", "\\n").replaceAll("[^\\u0000-\\uFFFF]", "")).mkString(rs) + eor
  }

  val defaultMakeOutputRow = makeOutputRow()

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

}

object StreamHelpers extends StreamHelpers