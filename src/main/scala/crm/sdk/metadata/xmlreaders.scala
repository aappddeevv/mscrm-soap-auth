package crm
package sdk
package metadata

import scala.language._
import scala.util.control.Exception._

import scopt._
import org.w3c.dom._
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

object xmlreaders {

  import sdk.soapnamespaces._

  val schemaNameReader = (__ \ "SchemaName").read[String]
  val logicalNameReader = (__ \ "LogicalName").read[String]
  val attributeTypeReader = (__ \ "AttributeType").read[String]
  val isValidForReadReader = (__ \ "IsValidForRead").read[Boolean]
  val isPrimaryIdReader = (__ \ "IsPrimaryId").read[Boolean]
  val isLogicalReader = (__ \ "IsLogical").read[Boolean]
  val attributeOfReader = (__ \ "AttributeOf").read[String].filter(!_.trim.isEmpty).optional
  val columnNumberReader = (__ \ "ColumnNumber").read[Int].default(-1)
  val metadataIdReader = (__ \ "MetadataId").read[String]
  val entityLogicalNameReader = (__ \ "EntityLogicalName").read[String]
  val displayNameReader = (__ \ "DisplayName" \ "UserLocalizedLabel").read[String].optional
  val descriptionNameReaer = (__ \ "Description" \ "UserLocalizedLabel").read[String].optional
  val minValueD = (__ \ "MinValue").read[Double]
  val maxValueD = (__ \ "MaxValue").read[Double]
  val precisionReader = (__ \ "Precision").read[Int].optional
  //val minValueDec = (__ \ "MinValue").read[BigDecimal].optional
  //val maxValueDec = (__ \ "MaxValue").read[BigDecimal].optional

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

  protected val dateTimeAttributeReader: XmlReader[DateTimeAttribute] =
    (attributeTypeReader.filter(WrongTypeError("DateTime"))(_ == "DateTime") and
      schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader)(DateTimeAttribute.apply _)

  protected val integerAttributeReader: XmlReader[IntegerAttribute] =
    (attributeTypeReader.filter(WrongTypeError("Integer"))(_ == "Integer") and
      schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader)(IntegerAttribute.apply _)

  protected val booleanAttributeReader: XmlReader[BooleanAttribute] =
    (attributeTypeReader.filter(WrongTypeError("Boolean"))(_ == "Boolean") and
      schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader)(BooleanAttribute.apply _)

  protected val doubleAttributeReader: XmlReader[DoubleAttribute] =
    (attributeTypeReader.filter(WrongTypeError("Double"))(_ == "Double") and
      schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader and
      minValueD and maxValueD)(DoubleAttribute.apply _)

  protected val bigIntAttributeReader: XmlReader[BigIntAttribute] =
    (attributeTypeReader.filter(WrongTypeError("BigInt"))(_ == "BigInt") and
      schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader)(BigIntAttribute.apply _)

  protected val moneyAttributeReader: XmlReader[MoneyAttribute] =
    (attributeTypeReader.filter(WrongTypeError("Money"))(_ == "Money") and
      schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader)(MoneyAttribute.apply _)

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

  val decimalAttributeReader: XmlReader[DecimalAttribute] =
    (attributeTypeReader.filter(WrongTypeError("Decimal"))(_ == "Decimal") and
      schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader and
      precisionReader)(DecimalAttribute.apply _)

  val memoAttributeReader: XmlReader[MemoAttribute] =
    (attributeTypeReader.filter(WrongTypeError("Memo"))(_ == "Memo") and
      schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader and
      maxLengthReader and
      formatReader)(MemoAttribute.apply _)

  val entityNameAttributeReader: XmlReader[EntityNameAttribute] =
    (attributeTypeReader.filter(WrongTypeError("EntityName"))(_ == "EntityName") and
      schemaNameReader and
      logicalNameReader and
      isValidForReadReader and
      isPrimaryIdReader and
      isLogicalReader and
      attributeOfReader and
      columnNumberReader and
      metadataIdReader and
      entityLogicalNameReader and
      formatReader)(EntityNameAttribute.apply _)

  val optionMetadataReader = (
    (__ \ "Label" \ "UserLocalizedLabel" \ "Label").read[String] and
    (__ \ "Value").read[String])(OptionMetadata.apply _)

  val optionSetReader = (
    (__ \ "Name").read[String] and
    (__ \ "DisplayName" \ "UserLocalizedLabel" \ "Label").read[String].default("") and
    (__ \ "Description" \ "UserLocalizedLabel" \ "Label").read[String].default("") and
    (__ \ "IsGlobal").read[Boolean] and
    (__ \ "OptionSetType").read[String] and // always Picklist???
    (__ \ "Options" \\ "OptionMetadata").read(seq(optionMetadataReader)) and
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
    (__ \ "OptionSet").read(optionSetReader))(PicklistAttribute.apply _)

  val statusAttributeReader: XmlReader[StatusAttribute] = (
    attributeTypeReader.filter(WrongTypeError("Status"))(_ == "Status") and
    schemaNameReader and
    logicalNameReader and
    isValidForReadReader and
    isPrimaryIdReader and
    isLogicalReader and
    attributeOfReader and
    columnNumberReader and
    metadataIdReader and
    entityLogicalNameReader and
    (__ \ "OptionSet").read(optionSetReader))(StatusAttribute.apply _)

  val stateAttributeReader: XmlReader[StateAttribute] = (
    attributeTypeReader.filter(WrongTypeError("State"))(_ == "State") and
    schemaNameReader and
    logicalNameReader and
    isValidForReadReader and
    isPrimaryIdReader and
    isLogicalReader and
    attributeOfReader and
    columnNumberReader and
    metadataIdReader and
    entityLogicalNameReader and
    (__ \ "OptionSet").read(optionSetReader))(StateAttribute.apply _)

  implicit val attributeReader: XmlReader[Attribute] =
    stringAttributeReader orElse
      lookupAttributeReader orElse
      pickListAttributeReader orElse
      stateAttributeReader orElse
      statusAttributeReader orElse
      memoAttributeReader orElse
      entityNameAttributeReader orElse
      decimalAttributeReader orElse
      dateTimeAttributeReader orElse
      booleanAttributeReader orElse
      bigIntAttributeReader orElse
      integerAttributeReader orElse
      doubleAttributeReader orElse
      moneyAttributeReader orElse
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
  //val x = (__ \\ "AttributeMetadata").read(seq[Attribute])

  implicit val schemaReader: XmlReader[CRMSchema] = (__ \\ "EntityMetadata").read(seq[EntityDescription]).map(CRMSchema(_))

}

