package crm

trait SoapHelpers extends com.typesafe.scalalogging.LazyLogging {

  import scala.util.control.Exception._
  import dispatch._, Defaults._

  /**
   * Handler for catching a future TimeoutException. Can be composed with other
   * handlers you need.
   */
  def catchTimeout = handling(classOf[java.util.concurrent.TimeoutException]) by { t =>
    println("Timeout waiting for name. Check your arguments.")
  } andFinally { Http.shutdown }

  /**
   * Wrap a SOAP body in an envelope and add the authentication header. xmlns s is soap-envelope
   * and xmlns a is addressing.
   */
  def wrap(auth: CrmAuthenticationHeader, soapBody: scala.xml.Elem*) = {
    (<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing">
     </s:Envelope>).copy(child = Seq(auth.Header) ++ soapBody)
  }

  /**
   * Create an Org Services endpoint from a URL that points to the org e.g. https://<yourorg>.crm.dynamics.com.
   * Adds the fixed OrganiaztionService string which should really be extracted from the WSDL.
   */
  def endpoint(url: String) = url + (if (url.endsWith("/")) "" else "/") + "XRMServices/2011/Organization.svc"
}
