package crm
package sdk
package messages

import org.log4s._
import cats._
import cats.data._
import cats.implicits._
import java.time._
import metadata._

/**
 * Render a single value of Any to a result of type R or an error message if
 * the value cannot be rendered.
 */
trait CrmValueRenderer[A <: Attribute, R] {
  /** Return either an error string or a rendered value. */
  def render(attrName: String, v: Any, a: A): Either[String, R]
}

/** Render a map of values. */
trait CrmValuesRenderer[R] {
  def render(values: Map[String, Any], e: EntityDescription): Either[String, R]
}

/** Implicits to convert CrmValue/JVM values -> SOAP parameter items. */
trait SoapAttributeRenderers {

  protected def renderNil(attrName: String) =
    kv(attrName, <b:value i:nil="true"></b:value>)

  protected def kv(attrName: String, value: xml.Elem) =
    Right(
      <a:KeyValuePairOfstringanyType xmlns:c="http://www.w3.org/2001/XMLSchema">
        <b:key>{ attrName }</b:key>
        { value }
      </a:KeyValuePairOfstringanyType>)

  implicit val entityReferenceRenderer = new CrmValueRenderer[LookupAttribute, xml.Elem] {
    def render(attrName: String, v: Any, a: LookupAttribute) = {
      val vv = if (v == None || v == null || v == UnsetValue) None else v
      vv match {
        case s: String =>
          // Assumes a default target, so there had better be only one in targets
          if (a.targets.size > 1 || a.targets.size == 0)
            Left(s"No entity target defined and attribute $attrName needs a target.")
          else
            kv(attrName,
              <b:value i:type="a:EntityReference">
                <a:Id>{ s }</a:Id><a:LogicalName>{ a.targets(0) }</a:LogicalName><a:Name i:nil="true"/>
              </b:value>)
        case EntityReference(id, target, name) =>
          if (!target.isDefined)
            Left(s"No entity target defined and attribute $attrName needs a target.")
          else
            kv(attrName,
              <b:value i:type="a:EntityReference">
                <a:Id>{ id }</a:Id><a:LogicalName>{ target.get }</a:LogicalName><a:Name i:nil="true"/>
              </b:value>)
        case None =>
          kv(attrName,
            <b:value i:type="EntityReference">
              <a:Id i:nil="true"/><a:LogicalName i:nil="true"/><a:Name i:nil="true"/>
            </b:value>)
        case _ => Left("Invalid value $v for a lookup on attribute $attrName.")
      }
    }
  }

  implicit val optionSetValueRenderer = new CrmValueRenderer[PicklistAttribute, xml.Elem] {
    def render(attrName: String, v: Any, a: PicklistAttribute) = {
      val vv = if (v == None || v == null || v == UnsetValue) None else v
      vv match {
        case OptionSetValue(value) =>
          kv(attrName,
            <b:value i:type="a:OptionSetValue">{ value }</b:value>)
        case None => renderNil(attrName)
        case _ => Left("Invalid value $v for a lookup on attribute $attrName.")
      }
    }
  }

  // guid

  implicit val moneyValueRenderer = new CrmValueRenderer[MoneyAttribute, xml.Elem] {
    def render(attrName: String, v: Any, a: MoneyAttribute) = {
      val vv = if (v == None || v == null || v == UnsetValue) None else v
      vv match {
        case d: BigDecimal =>
          kv(attrName,
            <b:value i:type="a:Money">{ d.toFloat }</b:value>)
        case f: Int =>
          kv(attrName,
            <b:value i:type="a:Money">{ f }</b:value>)
        case f: Float =>
          kv(attrName,
            <b:value i:type="a:Money">{ f }</b:value>)
        case d: Double =>
          kv(attrName,
            <b:value i:type="a:Money">{ d }</b:value>)
        case MoneyValue(value) =>
          kv(attrName,
            <b:value i:type="a:Money">{ value }</b:value>)
        case None => renderNil(attrName)
        case _ => Left("Invalid value $v for money attribute $attrName.")
      }

    }
  }

  implicit val dateTimeValueRenderer = new CrmValueRenderer[DateTimeAttribute, xml.Elem] {
    def render(attrName: String, v: Any, a: DateTimeAttribute) = {
      val vv = if (v == None || v == null || v == UnsetValue) None else v
      vv match {
        case i: Instant =>
          kv(attrName,
            <b:value i:type="c:dateTime">{ i.toString }</b:value>)
        case d: LocalDate =>
          kv(attrName,
            <b:value i:type="c:dateTime">{ Instant.from(d).toString }</b:value>)
        case None => renderNil(attrName)
        case _ => Left("Invalid value $v for a lookup on attribute $attrName.")

      }
    }
  }

  implicit val basicValueRenderer = new CrmValueRenderer[BasicAttribute, xml.Elem] {
    def render(attrName: String, v: Any, a: BasicAttribute) = {
      val vv = if (v == None || v == null || v == UnsetValue) None else v
      vv match {
        case i: String =>
          kv(attrName,
            <b:value i:type="c:string">{ i }</b:value>)
        case d: Int =>
          kv(attrName,
            <b:value i:type="c:integer">{ d }</b:value>)
        case d: BigDecimal =>
          kv(attrName,
            <b:value i:type="c:decimal">{ d }</b:value>)
        case b: Boolean =>
          kv(attrName,
            <b:value i:type="c:boolean">{ b }</b:value>)
        case None => renderNil(attrName)
        case _ => Left("Invalid value $v for a lookup on attribute $attrName.")

      }
    }
  }

  implicit val valuesRenderer = new CrmValuesRenderer[xml.Elem] {
    def render(values: Map[String, Any], e: EntityDescription) = {
      null
    }
  }

}

object soapattributerenderers extends SoapAttributeRenderers
