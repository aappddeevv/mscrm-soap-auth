package crm

import scala.language._
import scala.util.control.Exception._

import scopt._
import org.w3c.dom._
import dispatch._, Defaults._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.async.Async._
import com.typesafe.scalalogging._
import scala.util._
import xml.XML
import com.lucidchart.open.xtract.{ XmlReader, __ }
import com.lucidchart.open.xtract.XmlReader._
import com.lucidchart.open.xtract._
import play.api.libs.functional.syntax._
import better.files._
import scala.util.matching.Regex

case class DiagramConfig(
  help: Boolean = false,
  filterFilename: String = "filters.txt",
  input: String = "metadata.xml",
  output: String = "metadata.dot",
  metadataDumpFilename: String = "metadata.csv",
  filters: collection.immutable.Seq[String] = collection.immutable.Seq.empty,
  excludes: collection.immutable.Seq[String] = collection.immutable.Seq.empty)

/** Node in graph. */
case class Node(label: String, id: String, style: Map[String, String] = Map())

/** Edge in graph. */
case class Edge(start: String, stop: String, label: Option[String] = None,
  directed: Boolean = false, style: Map[String, String] = Map())

/**
 *  Small program to read CRM XML metadata and output a dot file (diagram).
 */
object diagram extends LazyLogging {

  val parser = new scopt.OptionParser[DiagramConfig]("diagram") {
    override def showUsageOnError = true
    head("diagram", "0.1.0")
    opt[String]("filterfile").valueName("<filename>").text("Input regexp filter, one filter per line. No filters means accept everything.")
      .action((x, c) => c.copy(filterFilename = x)).validate { filename =>
        if (File(filename).exists) success
        else failure("No filter file $filename found.")
      }
    opt[String]('o', "output").valueName("<filename>").text("Output file for diagram.")
      .action((x, c) => c.copy(output = x))
    arg[String]("<file>").unbounded().optional().text("Input metadata in CRM XML format. This can be the full SOAP response from RetrieveAllEntities.")
      .action((x, c) => c.copy(input = x)).validate { filename =>
        if (File(filename).exists) success
        else failure(s"No input file $filename found.")
      }
    opt[String]('e', "excludefilter").unbounded().valueName("<regex>").text("Regex used to identify which entites are excluded. Repeate option as needed. May need to be escaped or quoted on command line.")
      .action((x, c) => c.copy(excludes = c.excludes :+ x))
    opt[String]('f', "filter").unbounded().valueName("<regex>").text("Regex used to identify which entities are included. Repeat option as needed. May need to be escaped or quoted on command line.")
      .action((x, c) => c.copy(filters = c.filters :+ x))
    opt[String]('m', "metadatafile").valueName("<filename>").text("Metadata dump output file.")
      .action((x, c) => c.copy(metadataDumpFilename = x))
    help("help").text("Show help")
  }

  val zap = (x: Option[String]) => x map { _.trim } filterNot { _.isEmpty }

  def main(args: Array[String]): Unit = {

    val config = parser.parse(args, DiagramConfig()) match {
      case Some(c) => c
      case None => return
    }

    import CRMMetadata._

    val xml = nonFatalCatch withApply { ex =>
      println(s"Unable to read input XML file: ${config.input}")
      println(s"Error: ${ex.getMessage}")
      System.exit(-1)
      return
    } apply { XML.loadFile(config.input) }

    val schema = XmlReader.of[CRMSchema].read(xml) match {
      case ParseFailure(errors) =>
        println("Error parsing MS CRM Metadata. Program halted.")
        errors.foreach(println)
        return
      case PartialParseSuccess(_, errors) =>
        println("Error parsing MS CRM Metadata. Program halted.")
        errors.foreach(println)
        return
      case ParseSuccess(x) => x
    }

    // Setp the filters to filter the entities that are processed, these are schema names.
    val _allowed = nonFatalCatch withApply { t => Seq() } apply {
      config.filterFilename.lines.map(pat => new Regex(pat.trim)).toSeq
    } ++ config.filters.map(new Regex(_))
    val _excludes = config.excludes.map(pat => new Regex(pat.trim)).toSeq
    val allowAll = _allowed.size == 0 && _excludes.size == 0
    if (allowAll) println("Allowing all entities for processing")
    else println(s"Using ${_allowed.size} entity schema name filter patterns for processing.")

    def allowed(ename: String) =
      if (allowAll) true
      else {
        val yes = _allowed.filter(pat => ename match { case pat() => true; case _ => false; }).size > 0
        val no = _excludes.filter(pat => ename match { case pat() => true; case _ => false }).size > 0
        if (yes && !no) true
        else false
      }
    // Create the list of allowed entities to process
    val entitiesToProcess = schema.entities.filter(ent => allowed(ent.schemaName))
    // Get the list of allowed entities using their schema name.
    val schemaEntityNamesToProcess = entitiesToProcess.map(_.logicalName).toSet
    println(s"# entities to process: ${entitiesToProcess.size}")

    def allowedSchema(sname: String) = schemaEntityNamesToProcess(sname)

    import collection.mutable.ListBuffer
    val edges = ListBuffer[crm.Edge]() // we need to post-process edgets :-(
    val nodes = ListBuffer[crm.Node]()
    File(config.output).newPrintWriter(true).autoClosed.map { output =>
      output.println("graph { ")
      output.println("overlap=scale; splines=true;")
      entitiesToProcess.foreach { ent =>

        nodes += Node(label = ent.logicalName, id = ent.schemaName)

        println("Processing entity: " + ent.schemaName)
        var otmCounter = 0
        var mtoCounter = 0
        val mtmCounter = 0
        ent.oneToMany.filter(ent => allowedSchema(ent.referencedEntity) && allowedSchema(ent.referencingEntity)).foreach { rel =>
          //output.println(s"""${rel.referencingEntity} -- ${rel.referencedEntity} [label="${rel.schemaName}"];""") // referencingAttribute
          edges += Edge(rel.referencingEntity, rel.referencedEntity, zap(Option(rel.schemaName)))
          otmCounter += 1
        }
        //.filter(rel => schemaEntityNamesToProcess.contains(rel.referencingEntity))
        ent.manyToOne.filter(ent => allowedSchema(ent.referencedEntity) && allowedSchema(ent.referencingEntity)).foreach { rel =>
          //output.println(s"""${rel.referencingEntity} -- ${rel.referencedEntity} [label="${rel.schemaName}"];""")
          edges += Edge(rel.referencingEntity, rel.referencedEntity, zap(Option(rel.schemaName)))
          mtoCounter += 1
        }

        println(s"\t# one to many : $otmCounter")
        println(s"\t# many to one : $mtoCounter")
        println(s"\t# many to many: $mtoCounter")
      }

      nodes.foreach { n => output.println(s"""${n.label} [label="${n.id}"];""") }

      // check for recursive nodes
      val baddies = edges.filter(el => el.start == el.stop)
      edges --= baddies
      // add at least one back in
      if (baddies.size > 0) {
        val reduced_baddies = baddies.map(_.start).toSet
        reduced_baddies.foreach { el => edges += Edge(el, el, Some("*")) }
      }
      edges.foreach { e => output.println(s"${e.start} -- ${e.stop}" + e.label.map(lab => s""" [label="$lab"];""").getOrElse(";")) }

      output.println("}")
    }

    println(s"Creating metadata dump file: ${config.metadataDumpFilename}")
    config.metadataDumpFilename.toFile.newPrintWriter(true).autoClosed.map { csv =>
      entitiesToProcess.foreach { ent =>
        csv.println(s"${ent.schemaName}")
        ent.oneToMany.foreach { r => csv.println(s"OTM,${r}") }
        ent.manyToOne.foreach { r => csv.println(s"MTO,${r}") }
        ent.manyToMany.foreach { r => csv.println(s"MTM,${r}") }
        ent.attributes.sortBy(_.columnNumber).foreach { a => csv.println(s"ATT,${a}") }
      }
    }
  }
}

object CRMMetadata {

  import collection._

  /**
   * Attribute metadata.
   */
  case class Attribute(schemaName: String,
    logicalName: String,
    attributeType: String,
    columnNumber: Int)

  implicit val readAttribute: XmlReader[Attribute] = (
    (__ \ "SchemaName").read[String] and
    (__ \ "LogicalName").read[String] and
    (__ \ "AttributeType").read[String] and
    (__ \ "ColumnNumber").read[Int])(Attribute.apply _)

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

  implicit val readRelationship: XmlReader[Relationship] = (
    (__ \ "SchemaName").read[String] and
    (__ \ "ReferencedAttribute").read[String].default("") and
    (__ \ "ReferencedEntity").read[String].default("") and
    (__ \ "ReferencingAttribute").read[String].default("") and
    (__ \ "ReferencedEntityNavigationPropertyName").read[String].default("") and
    (__ \ "ReferencingEntity").read[String].default("") and
    (__ \ "ReferencingEntityNavigationPropertyName").read[String].default(""))(Relationship.apply _)

  /**
   * Entity metadata.
   */
  case class Entity(schemaName: String,
    logicalName: String,
    oneToMany: Seq[Relationship],
    manyToOne: Seq[Relationship],
    manyToMany: Seq[Relationship],
    attributes: Seq[Attribute])

  implicit val readEntity: XmlReader[Entity] = (
    (__ \ "SchemaName").read[String] and
    (__ \ "LogicalName").read[String] and
    (__ \\ "OneToManyRelationshipMetadata").read(seq[Relationship]) and
    (__ \\ "ManyToOneRelationshipMetadata").read(seq[Relationship]) and
    (__ \\ "ManyToManyRelationshipMetadata").read(seq[Relationship]) and
    (__ \\ "AttributeMetadata").read(seq[Attribute]))(Entity.apply _)

  /** Overall CRM schema. */
  case class CRMSchema(entities: Seq[Entity])

  implicit val schemaReader: XmlReader[CRMSchema] = (__ \\ "EntityMetadata").read(seq[Entity]).map(CRMSchema(_))
}
