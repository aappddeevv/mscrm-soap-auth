package crm
package sdk

import scala.concurrent._
import crm.sdk.driver._
import fs2._
import cats._
import cats.data._
import org.asynchttpclient._

package object client {

  /** Authentication model. */
  sealed trait AuthenticationProviderType
  /** Federated identity provider e.g. ADFS such as used with an CRM IFD. */
  case object Federation extends AuthenticationProviderType
  /** Online CRM Azure AD e.g. O365 */
  case object Online extends AuthenticationProviderType

  /** Basic CrmClient that can access a CRM service: Discovery or Org but not both at the same time. */
  abstract class CrmClient(sc: SoapConnection, config: Config) {

    /** Fetch a response. */
    def fetch[A](req: SoapRequest)(handler: Response => Task[A])(implicit F: Monad[Task]): Task[A] = sc.fetch(req)(handler)

    /** Create a new SOAP request read to be filled in. */
    def request: AddressedSoapRequest
    
    /** Shutdown. */
    def shutdownNow(): Unit = sc.shutdownNow()
  }

  /** Client for Discovery services. */
  class DiscoveryCrmClient(sc: SoapConnection, config: Config) extends CrmClient(sc, config) {
    def request = SoapRequest.to(config.url, crm.sdk.discovery.discoveryServiceExecuteAction)
  }

  /** Client for Organization services. */
  class OrganizationCrmClient(sc: SoapConnection, config: Config) extends CrmClient(sc, config) {
    def request = SoapRequest.to(config.url, "http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute")
  }

  object DiscoveryCrmClient {
    /** Create a new CrmClient from a Config. */
    def fromConfig(config: Config)(implicit ec: ExecutionContext) = new DiscoveryCrmClient(SoapConnection.fromConfig(config), config)
  }

  object OrganizationCrmClient {
    /** Create a new CrmClient from a Config. */
    def fromConfig(config: Config)(implicit ec: ExecutionContext) = new OrganizationCrmClient(SoapConnection.fromConfig(config), config)
  }

}