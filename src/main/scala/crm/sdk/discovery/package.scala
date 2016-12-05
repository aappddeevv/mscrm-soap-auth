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
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

import org.log4s._
import cats._
import cats.data._
import cats.implicits._
import cats.syntax._
import sdk.driver.CrmException
import fs2._
import dispatch.retry._
import metadata._

/**
 * CRM online provides discovery services. Given a region, you can
 * obtain the discovery URL. Given a discovery URL, user credentials
 * and a web app URL, you can obtain an org services data URL. While
 * the form of the org services data URL has not changed much, the 
 * approach to obtain an org data servces unl that will always work is 
 * to go through the discovery services.
 */
package object discovery {

  val ns = "http://schemas.microsoft.com/xrm/2011/Contracts/Discovery"

  /** The discovery service has discovery requests. */
  sealed trait DiscoveryRequest
  
  /** Retrieve organization detail information for each org. */
  case object RetrieveOrganizationsRequest extends DiscoveryRequest

  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers._
  import net.ceedubs.ficus.Ficus.toFicusConfig
  import com.typesafe.config.{ Config, ConfigFactory }
  import scala.collection._
  val _config: Config = ConfigFactory.load()

  private[this] val urnMap = mapValueReader[String].read(_config, "auth.urlToUrn")
  private[this] val defaultUrn = _config.as[String]("auth.defaultUrn")

  /** Region abbrevs mapped to their discovery URLs. */
  val regionToDiscoveryUrl = mapValueReader[String].read(_config, "auth.discoveryUrls")

  /**
   * Gets the correct URN Address based on the Online region.
   *
   * @return String URN Address.
   * @param url
   *            The Url of the CRM Online organization
   *            (e.g. https://org.crm.dynamics.com).
   */
  def urlToUrn(url: String): String = {
    val urlx = new java.net.URL(url.toUpperCase)
    urnMap.get(urlx.getHost) getOrElse defaultUrn
  }

}
