package crm
package sdk

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
import dispatch._, Defaults._
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
import fs2._
import sdk.messages._

package object driver {

  /** A request result is Task with either a ResponseError or
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
}