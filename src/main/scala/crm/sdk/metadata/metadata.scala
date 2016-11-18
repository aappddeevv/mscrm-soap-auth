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

package object metadata {

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

  case class PicklistAttribute(attributeType: String,
    schemaName: String,
    logicalName: String,
    isValidForRead: Boolean, // can be read in a retrieve
    isPrimaryId: Boolean,
    isLogical: Boolean, // whether stored in a different table
    attributeOf: Option[String] = None,
    columnNumber: Int = -1,
    metadataId: String,
    entityLogicalName: String,
    options: OptionSet) extends Attribute

  sealed trait DateTimeBehavior
  case object DateOnly extends DateTimeBehavior
  case object TimeZoneIndependent extends DateTimeBehavior
  case object USerlocal extends DateTimeBehavior

  /** An Option in an OptionSet */
  case class OptionMetadata(label: String, value: String)

  /** An option list for a PickListAttribute. */
  case class OptionSet(name: String,
    displayName: String,
    description: String,
    isGlobal: Boolean,
    optionSetType: String,
    options: Seq[OptionMetadata],
    id: String)

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
}
