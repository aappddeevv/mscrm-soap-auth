package crm
package sdk
package driver

import fs2._
import scala.concurrent._
import scala.concurrent.duration._
import dispatch._
import org.log4s._

/**
 *  Connection that hides some facets of HTTP communication and
 *  is foremost, asynchchronous and stream oriented.
 */
trait Connection {

  /** Return a new auth. */
  def auth(implicit ec: ExecutionContext): scala.concurrent.Future[CrmAuthenticationHeader]
  

  
  

  def shutdown: Unit
}


object Connection {

  private[this] implicit val logger = getLogger

  /** Create a driver from a config. */
  def apply(config: Config): Connection = new Connection {

    def auth(implicit ec: ExecutionContext): scala.concurrent.Future[CrmAuthenticationHeader] = {
      logger.debug("Obtaining auth.")
      CrmAuth.orgServicesAuthF(Http, config.username, config.password, config.url, config.region, config.leaseTime)
    }

    def http: HttpExecutor = {
      httphelpers.client(config.timeout)
    }

    def shutdown: Unit = {
      http.shutdown
    }
  }

}
