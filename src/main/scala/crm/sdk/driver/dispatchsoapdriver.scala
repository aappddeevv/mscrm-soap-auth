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
import scala.concurrent.ExecutionContext


trait DispatchSoapBackend extends BasicBackend {
  type This = DispatchSoapBackend
  type Context = DispatchSoapContext
  type Session = SessionDef
  type Org = OrgDef
  type OrgFactory = OrgFactoryDef

  val Org = new OrgFactoryDef {}
  val backend: DispatchSoapBackend = this // allows us to get the type right downclass...

  class OrgDef(username: String, password: String, servicesUrl: String, val executor: ExecutionContext) extends super.OrgDef {

    def createSession() = new BaseSession(this)

    def shutdown(): Future[Unit] = Future.successful(Unit)

    def close: Unit = {}
    
    protected[this] def getExceutionContext: ExecutionContext = executor
    
  }

  trait OrgFactoryDef {
    /**
     * Create a new Org based on the username, password, web app URL (which is typically better known
     * to clients) and the region abbreviation. The services URL is looked up based on this information.
     */
    def forWebAppUrlAndRegion(username: String, password: String, webAppUrl: String, regionAbrev: String = "NA")(implicit ec: ExecutionContext): OrgDef = {
      new OrgDef(username, password, "", ec)
    }
    
    /**
     * Ceate a new Org using the services URL directly.
     */
    def forServicesUrl(username: String, password: String, servicesUrl: String)(implicit ec: ExecutionContext): OrgDef = {
      new OrgDef(username, password, servicesUrl, ec)
    }
    
  }

  override protected[this] def createActionContext[T](_useSameThread: Boolean): Context =
    new DispatchSoapContext { val useSameThread = _useSameThread }

  trait DispatchSoapContext extends BasicActionContext {    
  }
    
  trait SessionDef extends super.SessionDef { self =>
    def org: Org
    def auth: CrmAuthenticationHeader
  }

  class BaseSession(val org: Org) extends SessionDef {
    val auth: CrmAuthenticationHeader = {
      null
    }
    
    def close(): Unit = { }
  }

}

object DispatchSoapBackend extends DispatchSoapBackend

trait DispatchSoapProfile extends BasicProfile {

  type Backend = DispatchSoapBackend
  val backend: Backend = DispatchSoapBackend

  trait API extends super.API {
  }

  val api = new API {}
}

object DispatchSoapProfile extends DispatchSoapProfile
