package crm

import scala.util.control.Exception._
import dispatch._, Defaults._
import java.util.Date;

trait SoapHelpers  {

  /**
   * Handler for catching a future TimeoutException. Can be composed with other
   * handlers you need.
   */
  def catchTimeout(name: String) = handling(classOf[java.util.concurrent.TimeoutException]) by { t =>
    println(s"Timeout waiting for $name. Check your timeout arguments.")
  }

  /**
   * Wrap a SOAP body in an envelope and add the authentication header. xmlns s is soap-envelope,
   * xmlns a is addressing and u is wssecurty utils.
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

  /** Create a timestamp useful for SOAP messages. */
  def timestamp = {
    val now = new Date()
    val createdNow = String.format("%tFT%<tT.%<tLZ", now)
    val createdExpires = String.format("%tFT%<tT.%<tLZ", AddMinutes(60, now))
    (createdNow, createdExpires)
  }

  /**
   *
   * @return Date The date with added minutes.
   * @param minutes
   *            Number of minutes to add.-
   * @param time
   *            Date to add minutes to.
   * Retrieve entity metadata.+   * @param aspects Which aspects of the metadata to retrieve.
   * @param retrieveUnPublished Retrieve published or unpublshed. Default is false.
   * @return SOAP body.
   */
  def AddMinutes(minutes: Int, time: Date): Date = {
    val ONE_MINUTE_IN_MILLIS = 60000;
    val currentTime = time.getTime();
    val newDate = new Date(currentTime + (minutes * ONE_MINUTE_IN_MILLIS));
    newDate
  }

  /** Generate a message id SOAP element. Namespace a. */
  def messageIdEl =  <a:MessageID>urn:uuid:{ java.util.UUID.randomUUID() }</a:MessageID>

}
