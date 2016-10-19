package crm
package sdk

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

object metadata {

  import collection._

  /**
   * Attribute metadata.
   */
  sealed trait Attribute {
    def schemaName: String
    def logicalName: String
    def attributeType: String
    def isValidForRead: Boolean // can be read in a retrieve
    def isPrimaryId: Boolean
    def isLogical: Boolean // whether stored in a different table
    def attributeOf: Option[String]
    def columnNumber: Int
    def metadataId: String
    def entityLogicalName: String
  }

  case class BasicAttribute(attributeType: String,
    schemaName: String,
    logicalName: String,
    isValidForRead: Boolean, // can be read in a retrieve
    isPrimaryId: Boolean,
    isLogical: Boolean, // whether stored in a different table
    attributeOf: Option[String] = None,
    columnNumber: Int = -1,
    metadataId: String,
    entityLogicalName: String) extends Attribute

  case class DoubleAttribute(attributeType: String,
    schemaName: String,
    logicalName: String,
    isValidForRead: Boolean, // can be read in a retrieve
    isPrimaryId: Boolean,
    isLogical: Boolean, // whether stored in a different table
    attributeOf: Option[String] = None,
    columnNumber: Int = -1,
    metadataId: String,
    entityLogicalName: String,
    minValue: Double = 0,
    maxValue: Double = 0) extends Attribute

  case class StringAttribute(attributeType: String,
    schemaName: String,
    logicalName: String,
    isValidForRead: Boolean, // can be read in a retrieve
    isPrimaryId: Boolean,
    isLogical: Boolean, // whether stored in a different table
    attributeOf: Option[String] = None,
    columnNumber: Int = -1,
    metadataId: String,
    entityLogicalName: String,
    minLength: Int = 0,
    maxLength: Int,
    format: Option[String]) extends Attribute

  case class LookupAttribute(attributeType: String,
    schemaName: String,
    logicalName: String,
    isValidForRead: Boolean, // can be read in a retrieve
    isPrimaryId: Boolean,
    isLogical: Boolean, // whether stored in a different table
    attributeOf: Option[String] = None,
    columnNumber: Int = -1,
    metadataId: String,
    entityLogicalName: String,
    targets: Seq[String]) extends Attribute

  sealed trait DateTimeBehavior
  case object DateOnly extends DateTimeBehavior
  case object TimeZoneIndependent extends DateTimeBehavior
  case object USerlocal extends DateTimeBehavior  
    
  /**
   * The referenced/referencing names are logical names.
   */
  case class Relationship(schemaName: String,
    referencedAttribute: String,
    referencedEntity: String,
    referencingAttribute: String,
    referencedEntityNavigationPropertyName: String,
    referencingEntity: String,
    referencingEntityNavigationPropertyName: String)

  /**
   * Entity metadata.
   */
  case class EntityDescription(schemaName: String,
      logicalName: String,
      primaryId: String,
      oneToMany: Seq[Relationship] = Nil,
      manyToOne: Seq[Relationship] = Nil,
      manyToMany: Seq[Relationship] = Nil,
      attributes: Seq[Attribute] = Nil) {

    /**
     * List of attributes that can be retrieved in a query/fetch request.
     * This is different than the metadata concept IsRetrievable which is
     * for internal use only.
     */
    def retrievableAttributes = attributes.filter(a => !a.isLogical && a.isValidForRead)

  }

  object UnknownEntityDescription extends EntityDescription("unkown", "unkown", "unknown")

  /** Overall CRM schema. */
  case class CRMSchema(entities: Seq[EntityDescription])

  /** Find a primary id logical name for a specific entity. Entity name is not case sensitive. */
  def primaryId(ename: String, schema: CRMSchema): Option[String] =
    for {
      e <- schema.entities.find(_.logicalName.trim.toUpperCase == ename.trim.toUpperCase)
      //a <- e.attributes.find(_.isPrimaryId)
    } yield e.primaryId //yield a.logicalName

  /**
   * Find an entity ignoring case in the entity's logical name.
   */
  def findEntity(ename: String, schema: CRMSchema) = schema.entities.find(_.logicalName.trim.toUpperCase == ename.trim.toUpperCase)

  object readers {

    case class WrongTypeError(expected: String) extends ValidationError

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

    implicit val attributeReader: XmlReader[Attribute] =
      stringAttributeReader orElse lookupAttributeReader orElse basicAttributeReader

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

    /** Read a single AttributeMetdat */
    val attributeMetadataReader: XmlReader[xml.NodeSeq] = (__ \\ "AttributeMetadata").read

    implicit val entityReader: XmlReader[EntityDescription] = (
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
}
