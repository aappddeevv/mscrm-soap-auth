package crm
package sdk
package driver

import fs2._
import scala.concurrent._
import scala.concurrent.duration._
import dispatch._
import org.log4s._

/**
 *  Simple driver that combines Http calling with
 *  authentication. It is designed to assist
 *  making calls to CRM not but encompassing request-response
 *  processing directly.
 */
trait Driver {

  /** Return a new auth. */
  def auth(implicit ec: ExecutionContext): scala.concurrent.Future[CrmAuthenticationHeader]

  /** Return the same HttpExecutor. */
  def http: HttpExecutor

  def shutdown: Unit
}


trait OrgDriver extends Driver {
  
  /** Provide a stream of auths. */
  def auths: Stream[Task, CrmAuthenticationHeader]

}

object Driver {

  private[this] implicit val logger = getLogger

  /** Create a driver from a config. */
  def apply(config: Config): Driver = new Driver {

    def auth(implicit ec: ExecutionContext): scala.concurrent.Future[CrmAuthenticationHeader] = {
      logger.debug("Obtaining auth.")
      CrmAuth.orgServicesAuthF(Http, config.username, config.password, config.url, config.region, config.leaseTime)
    }

    def http: HttpExecutor = {
      SoapHelpers.client(config.timeout)
    }

    def shutdown: Unit = {
      http.shutdown
    }
  }

}
