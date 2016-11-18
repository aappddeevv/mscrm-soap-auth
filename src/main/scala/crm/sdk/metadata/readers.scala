package crm
package sdk
package metadata

import scala.language._
import scala.util.control.Exception._

import scopt._
import org.w3c.dom._
import dispatch._, Defaults._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.async.Async._
import scala.util._
import xml.XML
import com.lucidchart.open.xtract.{ XmlReader, __ }
import com.lucidchart.open.xtract.XmlReader._
import com.lucidchart.open.xtract._
import play.api.libs.functional.syntax._
import better.files._
import scala.util.matching.Regex
import org.log4s._

object readers {

  import crm.sdk.SoapNamespaces._
  
  val schemaNameReader = (__ \ "SchemaName").read[String]
  val logicalNameReader = (__ \ "LogicalName").read[String]
  val attributeTypeReader = (__ \ "AttributeType").read[String]
  val isValidForReadReader = (__ \ "IsValidForRead").read[Boolean]
  val isPrimaryIdReader = (__ \ "IsPrimaryId").read[Boolean]
  val isLogicalReader = (__ \ "IsLogical").read[Boolean]
  val attributeOfReader = (__ \ "AttributeOf").read[String].optional.filter(!_.isEmpty)
  val columnNumberReader = (__ \ "ColumnNumber").read[Int].default(-1)
  val metadataIdReader = (__ \ "MetadataId").read[String]
  val entityLogicalNameReader = (__ \ "EntityLogicalName").read[String]

  val stringArray: XmlReader[Seq[String]] = (__ \\ "string").read(seq[String])

  private[this] val readCommonAttributesExceptType =
    schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader

  protected val basicAttributeReader: XmlReader[BasicAttribute] =
    (attributeTypeReader and
      schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader)(BasicAttribute.apply _)

  val targetsReader = (__ \ "Targets").read[xml.NodeSeq] andThen stringArray

  /** Read LookupAttribute. Fail fast by looking at AttributeType. */
  val lookupAttributeReader: XmlReader[LookupAttribute] =
    (attributeTypeReader.filter(WrongTypeError("Lookup"))(_ == "Lookup") and
      schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader and
      targetsReader)(LookupAttribute.apply _)

  val minLengthReader = (__ \ "MinLength").read[Int].default(0)
  val maxLengthReader = (__ \ "MaxLength").read[Int]
  val formatReader = (__ \ "Format").read[String].optional

  val stringAttributeReader: XmlReader[StringAttribute] =
    (attributeTypeReader.filter(WrongTypeError("String"))(_ == "String") and
      schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader and
      minLengthReader and
      maxLengthReader and
      formatReader)(StringAttribute.apply _)

  val optionMetadataReader = (
    (__ \ "Label" \ "UserLocalizedLabel" \ "Label").read[String] and
    (__ \ "Value").read[String])(OptionMetadata.apply _)

  val optionSetReader = (
    (__ \ "Name").read[String] and
    (__ \ "DisplayName" \ "UserLocalizedLabel" \ "Label").read[String] and
    (__ \ "Description" \ "UserLocalizedLabel" \ "Label").read[String] and
    (__ \ "IsGlobal").read[Boolean] and
    (__ \ "OptionSetType").read[String] and
    (__ \ "OptionSet" \\ "OptionSetMetadata").read(seq(optionMetadataReader)) and
    (__ \ "MetadataId").read[String])(OptionSet.apply _)

  val pickListAttributeReader: XmlReader[PicklistAttribute] = (
    attributeTypeReader.filter(WrongTypeError("Picklist"))(_ == "Picklist") and
    schemaNameReader and
    logicalNameReader and
    isValidForReadReader and
    isPrimaryIdReader and
    isLogicalReader and
    attributeOfReader and
    columnNumberReader and
    metadataIdReader and
    entityLogicalNameReader and
    optionSetReader)(PicklistAttribute.apply _)

  implicit val attributeReader: XmlReader[Attribute] =
    stringAttributeReader orElse
      lookupAttributeReader orElse
      pickListAttributeReader orElse
      basicAttributeReader

  /** ParseError returned if the "type" is wrong. */
  case class WrongTypeError(expected: String) extends ValidationError

  /** Read an attribute. */
  def iTypeReader(ns: String) = XmlReader.attribute[String](s"{$ns}type")

  /** Fail fast if the i:type attribute does not match `t`. Return a NodeSeq reader to allow XmlReader composition. */
  def filteritype(t: String) = nodeReader.filter(WrongTypeError(t))(n =>
    iTypeReader(NSSchemaInstance).read(n).getOrElse("") == t)

  implicit val relationshipReader: XmlReader[Relationship] = (
    (__ \ "SchemaName").read[String] and
    (__ \ "ReferencedAttribute").read[String].default("") and
    (__ \ "ReferencedEntity").read[String].default("") and
    (__ \ "ReferencingAttribute").read[String].default("") and
    (__ \ "ReferencedEntityNavigationPropertyName").read[String].default("") and
    (__ \ "ReferencingEntity").read[String].default("") and
    (__ \ "ReferencingEntityNavigationPropertyName").read[String].default(""))(Relationship.apply _)

  /** Navigate to "Attributes" */
  val attributesReader: XmlReader[xml.NodeSeq] = (__ \\ "Attributes").read

  /** Read a single AttributeMetdata */
  val attributeMetadataReader: XmlReader[xml.NodeSeq] = (__ \\ "AttributeMetadata").read

  implicit val entityDescriptionReader: XmlReader[EntityDescription] = (
    (__ \ "SchemaName").read[String] and
    (__ \ "LogicalName").read[String] and
    (__ \ "PrimaryIdAttribute").read[String] and
    (__ \\ "OneToManyRelationshipMetadata").read(seq[Relationship]) and
    (__ \\ "ManyToOneRelationshipMetadata").read(seq[Relationship]) and
    (__ \\ "ManyToManyRelationshipMetadata").read(seq[Relationship]) and
    (__ \\ "AttributeMetadata").read(seq[Attribute]))(EntityDescription.apply _)

  //(attributesReader andThen attributeMetadataReader andThen seq(attributeReader))
  val x = (__ \\ "AttributeMetadata").read(seq[Attribute])

  implicit val schemaReader: XmlReader[CRMSchema] = (__ \\ "EntityMetadata").read(seq[EntityDescription]).map(CRMSchema(_))

}

