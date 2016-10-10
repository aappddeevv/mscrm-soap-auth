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
  case class Attribute(schemaName: String,
    logicalName: String,
    attributeType: String,
    isValidForRead: Boolean, // can be read in a retrieve
    isPrimaryId: Boolean,
    isLogical: Boolean, // whether stored in a different table
    attributeOf: Option[String] = None,
    columnNumber: Int = -1,
    metadataId: String,
    enityLogicalName: String)

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

    implicit val readAttribute: XmlReader[Attribute] = (
      (__ \ "SchemaName").read[String] and
      (__ \ "LogicalName").read[String] and
      (__ \ "AttributeType").read[String] and
      (__ \ "IsValidForRead").read[Boolean] and
      (__ \ "IsPrimaryId").read[Boolean] and
      (__ \ "IsLogical").read[Boolean] and
      (__ \ "AttributeOf").read[String].optional.filter(!_.isEmpty) and
      (__ \ "ColumnNumber").read[Int].default(-1) and
      (__ \ "MetadataId").read[String] and
      (__ \ "EntityLogicalName").read[String])(Attribute.apply _)

    implicit val readRelationship: XmlReader[Relationship] = (
      (__ \ "SchemaName").read[String] and
      (__ \ "ReferencedAttribute").read[String].default("") and
      (__ \ "ReferencedEntity").read[String].default("") and
      (__ \ "ReferencingAttribute").read[String].default("") and
      (__ \ "ReferencedEntityNavigationPropertyName").read[String].default("") and
      (__ \ "ReferencingEntity").read[String].default("") and
      (__ \ "ReferencingEntityNavigationPropertyName").read[String].default(""))(Relationship.apply _)

    implicit val readEntity: XmlReader[EntityDescription] = (
      (__ \ "SchemaName").read[String] and
      (__ \ "LogicalName").read[String] and
      (__ \ "PrimaryIdAttribute").read[String] and
      (__ \\ "OneToManyRelationshipMetadata").read(seq[Relationship]) and
      (__ \\ "ManyToOneRelationshipMetadata").read(seq[Relationship]) and
      (__ \\ "ManyToManyRelationshipMetadata").read(seq[Relationship]) and
      (__ \\ "AttributeMetadata").read(seq[Attribute]))(EntityDescription.apply _)

    implicit val schemaReader: XmlReader[CRMSchema] = (__ \\ "EntityMetadata").read(seq[EntityDescription]).map(CRMSchema(_))

  }
}
