package crm
package sdk
package messages

import scala.util.control.Exception._
import dispatch._, Defaults._
import java.util.Date;
import cats._
import data._
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

import soapnamespaces.implicits

/**
 *  General purpose XML writer typeclass. Namespaces are handled
 *  via implicits so you can setup a set of namespace abbrevations
 *  for your application.
 */
trait CrmXmlWriter[-A] {

  def write(a: A)(implicit ns: NamespaceLookup): xml.NodeSeq

  def transform(transformer: xml.NodeSeq => xml.NodeSeq)(implicit ns: NamespaceLookup) = CrmXmlWriter[A] { a =>
    transformer(this.write(a)(ns))
  }

  def transform(transformer: CrmXmlWriter[xml.NodeSeq])(implicit ns: NamespaceLookup) = CrmXmlWriter[A] { a =>
    transformer.write(this.write(a)(ns))(ns)
  }
}

object CrmXmlWriter {

  /**
   * Get a writer without having to use the implicitly syntax.
   */
  def of[A](implicit r: CrmXmlWriter[A], ns: NamespaceLookup): CrmXmlWriter[A] = r

  /** Create a write using apply syntax. */
  def apply[A](f: A => xml.NodeSeq): CrmXmlWriter[A] = new CrmXmlWriter[A] {
    def write(a: A)(implicit ns: NamespaceLookup): xml.NodeSeq = f(a)
  }

  /**
   * Allows you to write `yourobject.write`.
   */
  implicit class RichCrmXmlWriter[A](a: A) {
    def write(implicit writer: CrmXmlWriter[A], ns: NamespaceLookup) = writer.write(a)(ns)
  }

}

