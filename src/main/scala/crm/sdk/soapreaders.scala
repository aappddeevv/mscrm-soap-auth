package crm
package sdk

import scala.language._
import scala.util.control.Exception._
import dispatch._, Defaults._
import java.util.Date;
import cats._
import cats.data._
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

/** Readers helpful in reading raw XML responses */
object soapreaders {

  import metadata.xmlreaders.iTypeReader

  /** Remove a colon separated prefix from the string if it exists. */
  def removePrefix(in: String): String = {
    val idx = in.indexOf(":")
    if(idx>=0) in.substring(idx+1)
    else in
  }
 

  /** Read <value> */
  val entityReferenceValueReader = (
    (__ \ "Id").read[String] and
    nodeReader and
    (__ \ "LogicalName").read[String])(EntityReferenceServerValue.apply _)

  /** Read <value> */
  val optionSetValueReader = (
    (__ \ "Value").read[String] and
    nodeReader)(OptionSetValue.apply _)

  /** Read <value> */
  val typedValueReader = (
    nodeReader.map(_.text) and
    nodeReader and
    iTypeReader(soapnamespaces.NSSchemaInstance).map(removePrefix).default(""))(TypedServerValue.apply _)

  /** Read a value from the key-value pair. Read <value>. */
  val valueReader =
    optionSetValueReader orElse
      entityReferenceValueReader orElse
      typedValueReader

  /**
   * Impossibly hard due to awful SOAP encodings.
   *  Assumes that the type in the value element has a prefix and requires
   *  a very expensive attribute search :-)
   */
  val obtuseKeyValueReader: XmlReader[(String, ServerValue)] =
    XmlReader { xml =>
      val k = (__ \ "key").read[String].read(xml)
      val v = (__ \ "value").read(valueReader).read(xml)
      val r = for {
        kk <- k
        vv <- v
      } yield (kk, vv)
      r
    }

  val stringStringReader: XmlReader[(String, String)] =
    ((__ \ "key").read[String] and
      (__ \ "value").read[String])((_, _))

  /** Apply to the envelope */
  val body: XmlReader[scala.xml.NodeSeq] =
    (__ \ "Body").read

  val faultPath = (__ \ "Fault")

  val fault: XmlReader[scala.xml.NodeSeq] = faultPath.read

  val retrieveMultipleResponse: XmlReader[xml.NodeSeq] =
    (__ \ "RetrieveMultipleResponse").read

  val retrieveMultipleResult: XmlReader[xml.NodeSeq] =
    (__ \ "RetrieveMultipleResult").read

  val executeResponse: XmlReader[xml.NodeSeq] =
    (__ \ "ExecuteResponse").read

  val executeResult: XmlReader[xml.NodeSeq] =
    (__ \ "ExecuteResult").read

  val organizationServiceFault: XmlReader[xml.NodeSeq] =
    (__ \ "OrganizationServiceFault").read

  /** Apply to the envelope. */
  val header: XmlReader[xml.NodeSeq] = (__ \\ "Header").read

  implicit val responseHeaderReader: XmlReader[ResponseHeader] =
    header andThen (
      (__ \\ "Action").read[String] and
      (__ \\ "RelatesTo").read[String])(ResponseHeader.apply _)

  implicit val entityReader: XmlReader[Entity] =
    (
      //XmlReader.pure(HashMap[String, ServerValue]()) and
      (__ \ "Attributes").children.read(seq(obtuseKeyValueReader)).map { v =>
        collection.immutable.HashMap[String, ServerValue]() ++ v
      } and
      (__ \ "FormattedValues" \\ "KeyValuePairOfstringstring").read(seq(stringStringReader)).map { v =>
        collection.immutable.HashMap[String, String]() ++ v
      })(Entity.apply _)

  implicit val entityCollectionResultReader: XmlReader[EntityCollectionResult] =
    ((__ \ "EntityName").read[String] and
      (__ \ "Entities" \\ "Entity").read(strictReadSeq[Entity]).default(Nil) and
      (__ \ "TotalRecordCount").read[Int].optional and
      (__ \ "TotalRecordCountLimitExceeded").read[Boolean] and
      (__ \ "MoreRecords").read[Boolean] and
      (__ \ "PagingCookie").read[String].filter(!_.trim.isEmpty).optional)(EntityCollectionResult.apply _)

  implicit val detail: XmlReader[xml.NodeSeq] = (__ \ "Detail").read

  implicit val faultReader = (
    (__ \ "ErrorCode").read[Int] and
    (__ \ "Message").read[String])(Fault.apply _)

  val reasonReader = (
    XmlReader.pure(-1) and
    (__ \\ "Reason").read[String])(Fault.apply _)

  implicit val pagingCookieReader =
    ((__ \\ "PagingCookie").read[String].filter(!_.trim.isEmpty).optional and
      (__ \\ "MoreRecords").read[Boolean] and
      (__ \\ "TotalRecordCount").read[Int])((_, _, _))

  /** Read a key-value */
  implicit val keyValuePairOfstringanyTypeReader =
    ((__ \ "key").read[String] and
      (__ \ "value" \ "Value").read[String])((_, _))

  //  implicit val seqKVReader =
  //    (__ \\ "KeyValuePairOfstringanyTypeReader").
  //      read(seq(keyValuePairOfstringanyTypeReader))

  implicit val readExecuteResult: XmlReader[ExecuteResult] = {
    (
      (__ \ "ResponseName").read[String] and
      (__ \ "Results").children.read(seq(obtuseKeyValueReader)).map { v =>
        collection.immutable.HashMap[String, ServerValue]() ++ v
      })(ExecuteResult.apply _)
  }

  /**
   * Reader that reads a Fault or the results of a RetrieveMultipleRequest.
   */
  val retrieveMultipleRequestReader = {
    val tmp = (responseHeaderReader and
      (body andThen (
        (fault andThen ((detail andThen organizationServiceFault andThen faultReader) or reasonReader)) or
        (retrieveMultipleResponse andThen retrieveMultipleResult andThen entityCollectionResultReader))))
    tmp(Envelope.apply _)
  }

  /** Reader for create message. */
  val executeRequestResponseReader = {
    (responseHeaderReader and
      (body andThen (
        (fault andThen ((detail andThen organizationServiceFault andThen faultReader) or reasonReader)) or
        (executeResponse andThen executeResult andThen readExecuteResult))))(Envelope.apply _)
  }

}
