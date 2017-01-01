package crm

import scala.collection.JavaConverters._
import scala.language._
import scala.util.control.Exception._
import scopt._
import org.w3c.dom._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.async.Async._
import scala.util._
import org.log4s._
import com.lucidchart.open.xtract.{ XmlReader, _ }
import XmlReader._
import play.api.libs.functional.syntax._
import cats._
import cats.data._
import cats.implicits._
import cats.instances._
import cats.syntax._
import better.files._
import java.io.{ File => JFile }
import fs2._
import scala.concurrent.ExecutionContext
import sdk.CrmAuth._
import sdk.httphelpers._
import sdk.streamhelpers._
import scala.util.matching._
import sdk.soapnamespaces.implicits._
import sdk.messages.soaprequestwriters._
import sdk.soapreaders._
import sdk.metadata.xmlreaders._
import sdk._
import sdk.discovery._
import sdk.metadata.soapwriters._
import sdk.discovery.soapwriters._
import sdk.metadata._
import scala.xml
import org.apache.ddlutils._
import org.apache.ddlutils.model._
import org.apache.ddlutils.io._
import _root_.io.circe._
import _root_.io.circe.syntax._

object Copy {
/*
  private[this] implicit val logger = getLogger

  def apply(config: Config): Unit = {
    config.copyAction match {
      case "listTargets" =>
        PlatformFactory.getSupportedPlatforms.toSeq.sorted.foreach(println)
      case "ddl" =>
        val metadatafile = config.copyMetadataFile.getOrElse("metadata.xml")
        val schema =
          try {
            val mxml = xml.XML.loadFile(metadatafile)
            schemaReader.read(mxml) match {
              case ParseSuccess(s) => s
              case PartialParseSuccess(s, msg) => s
              case ParseFailure(msg) => throw new RuntimeException("Parsing error while parsing CRM XML metadata.")
            }
          } catch {
            case scala.util.control.NonFatal(e) =>
              logger.error(e)("Error obtaining metadata.")
              throw new RuntimeException(s"Unable to open metadata file $metadatafile: ${e.getMessage}")
          }
        val (db, json, mapping) = createDdl(schema)
        println("rvalue: " + db)
        val platform = PlatformFactory.createNewPlatformInstance(config.copyDatabaseType)
        new DatabaseIO().write(db, config.copyDatabaseType + ".xml")
        val sql = platform.getCreateTablesSql(db, true, true)
        val enhancedJson = json.add("dbTarget", Json.fromString(config.copyDatabaseType))
        import better.files._
        (config.copyDatabaseType + ".ddl").toFile < sql
        (config.copyDatabaseType + ".mapping").toFile < enhancedJson.asJson.toString
        println(mapping)
    }
  }

  /** If GUIDs become string, this is the size. */
  val guidSizeStr = "36"

  case class GenerationConfig(
    tablename_prefix: Option[String] = None,
    tablename_postfix: Option[String] = None,
    schema: Option[String] = None)

  /**
   * Wide range of possible column types for internal processing. These may
   *  be translated into a RDBMS specific type depending on the database
   *  capabilities. Some of the ColTypes' are really just aliases for each other.
   *  The column types mostly assume a RDBMS like target and are not designed
   *  to represent targets such as document databases.
   */
  object AttrsType {
    // BIT, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, REAL, NUMERIC, DECIMAL, 
    // CHAR, VARCHAR, LONGVARCHAR, DATE, 
    // TIME, TIMESTAMP, BINARY, VARBINARY, LONGVARBINARY, NULL, OTHER, JAVA_OBJECT, 
    // DISTINCT, STRUCT, ARRAY, BLOB, CLOB, REF, BOOLEANINT, BOOLEANCHAR, DOUBLE
    sealed trait Type
    case class Char(size: Int, unicode: Boolean = false) extends Type
    case class String(maxLength: Int, minLength: Option[Int] = None, info: Map[String, String] = Map(), unicode: Boolean = false) extends Type
    case class Decimal(precision: Int = 0, scale: Option[Int] = None) extends Type
    case class Binary(maxLength: Option[Int]) extends Type
    case class VarBinary(maxLength: Option[Int]) extends Type
    /** Physical representation will depend on the target. */
    case object Auto extends Type
    /** Physical representation will depend on the target e.g. GUID or Long. */
    case object UniqueIdentifier extends Type
    case object Float extends Type
    case object Double extends Type
    case object Bit extends Type
    case object SmallInt extends Type
    case object Integer extends Type
    case object Long extends Type
    case object BigInt extends Type
    case object Money extends Type
    case object Clob extends Type
    case object Blob extends Type
    case object Timestamp extends Type
    case object Time extends Type
    case object Date extends Type
    case object Boolean extends Type
    case object BooleanChar extends Type
    case object BoleanInt extends Type
    /**
     * Within the target "system," a reference to another object e.g. a FK.
     * Generally, Ref will be target specific type that the rendering system
     * will infer based on defaults or user input. Ref is generally only used
     * on the target side.
     */
    case object Ref extends Type
    /** Extensible type that is not represented by `Type`. */
    case class Other(name: String, info: Map[String, String] = Map()) extends Type
    /** Embedded structure. */
    case class Object(name: String, attrs: Seq[Type]) extends Type
  }

  /** A slot is a source or target description. */
  sealed trait Slot

  /** An attribute specification: name and type. */
  case class Spec(name: String, atype: AttrsType.Type, required: Boolean = false, info: Map[String, String] = Map()) extends Slot

  /** Source that's a constant value. The constant value is expressed as a string. */
  case class Literal(value: String, atype: AttrsType.Type) extends Slot
  /**
   * A computed value of a specific type and a possible list of
   *  attributes that may be input into that computation.
   */
  case class Computed(atype: AttrsType.Type, inputs: Seq[String] = Nil) extends Slot

  /** No slot. */
  case object EmptySlot extends Slot

  /**
   * Mapping from one slot to another. Only certain types of slots are
   *  allowed to be a target while a source can be any type of slot.
   */
  case class Mapping(target: Spec, source: Slot, info: Map[String, String] = Map.empty)

  /**
   * Given a CRM schema, output a mapping objects.
   */
  def createDdl(schema: CRMSchema) = {
    //import scala.collection.mutable._

    println(s"CRM schema has ${schema.entities.size} entity descriptions.")

    val gconfig = GenerationConfig(schema = Option("crmuser"))

    val root = JsonObject.fromIterable(Seq(
      "verson" -> Json.fromLong(1),
      "description" -> Json.fromString("Mapping from CRM to RDBMS"),
      "createdOn" -> Json.fromString(httphelpers.instantString)))
    val jtables = scala.collection.mutable.ListBuffer[Json]()

    val mappings = scala.collection.mutable.Map[String, Seq[Mapping]]()

    val db = new Database()
    db.setName("crm")
    schema.entities.foreach { ent =>
      val attributes = scala.collection.mutable.ListBuffer[Json]()
      val emapping = scala.collection.mutable.ListBuffer[Mapping]()
      val table = new Table()
      table.setName(ent.logicalName)
      table.setSchema("crmuser")
      table.setDescription("MS CRM entity " + ent.logicalName)
      ent.retrievableAttributes.sortBy(_.logicalName).foreach { attr =>
        logger.info(s"${ent.logicalName}.${attr.logicalName} => ${attr.attributeType}")
        val cols = toColumn(attr)
        cols.foreach(c => table.addColumn(c))
        val as = cols.map(c => Json.obj(
          "target" -> Json.fromString(c.getName),
          "source" -> Json.fromString(attr.logicalName),
          "sourcetype" -> Json.fromString(attr.attributeType)))

        def direct(a: Attribute, at: AttrsType.Type) = Mapping(Spec(a.logicalName, at), Spec(a.logicalName, at))

        emapping ++= (attr match {
          case a: StringAttribute =>
            Seq(Mapping(Spec(attr.logicalName, AttrsType.String(a.maxLength)), Spec(attr.logicalName, AttrsType.String(a.maxLength))))
          case a: LookupAttribute =>
            val col1 = Mapping(Spec(attr.logicalName, AttrsType.UniqueIdentifier), Spec(attr.logicalName, AttrsType.UniqueIdentifier))
            val col2source =
              if (a.targets.size == 1)
                Literal(a.targets(0), AttrsType.String(100))
              else
                Spec(attr.logicalName + "_target", AttrsType.String(100))
            val col2 = Mapping(Spec(attr.logicalName + "_target", AttrsType.String(100)), col2source,
              Map("description" -> "Entity reference target entity name."))
            Seq(col1, col2)
          case a: PicklistAttribute =>
            val col1 = Mapping(Spec(a.logicalName, AttrsType.Integer), Spec(a.logicalName, AttrsType.Integer))
            val col2 = Mapping(Spec(a.logicalName + "_label", AttrsType.String(100)), Spec(a.logicalName + "_fv", AttrsType.String(100)))
            Seq(col1, col2)
          case a: StatusAttribute =>
            val col1 = Mapping(Spec(a.logicalName, AttrsType.Integer), Spec(a.logicalName, AttrsType.Integer))
            val col2 = Mapping(Spec(a.logicalName + "_label", AttrsType.String(100)), Spec(a.logicalName + "_fv", AttrsType.String(100)))
            Seq(col1, col2)
          case a: StateAttribute =>
            val col1 = Mapping(Spec(a.logicalName, AttrsType.Integer), Spec(a.logicalName, AttrsType.Integer))
            val col2 = Mapping(Spec(a.logicalName + "_label", AttrsType.String(100)), Spec(a.logicalName + "_fv", AttrsType.String(100)))
            Seq(col1, col2)
          case a: IntegerAttribute =>
            Seq(direct(a, AttrsType.Integer))
          case a: DateTimeAttribute =>
            Seq(direct(a, AttrsType.Timestamp))
          case a: EntityNameAttribute =>
            Seq(direct(a, AttrsType.String(100)))
          case a: MemoAttribute =>
            Seq(direct(a, AttrsType.String(a.maxLength)))
          case a: BooleanAttribute =>
            Seq(direct(a, AttrsType.Boolean))
          case a: DecimalAttribute =>
            Seq(direct(a, AttrsType.Decimal(a.precision.getOrElse(0))))
          case a: MoneyAttribute =>
            Seq(direct(a, AttrsType.Money))
          case a: DoubleAttribute =>
            Seq(direct(a, AttrsType.Double))
          case a: BigIntAttribute =>
            Seq(direct(a, AttrsType.BigInt))
          case a: BasicAttribute =>
            a.attributeType match {
              case "Uniqueidentifier" =>
                Seq(direct(a, AttrsType.UniqueIdentifier))
              case "ManagedProperty" =>
                Seq(direct(a, AttrsType.String(40)))
              case "Customer" =>
                Seq(direct(a, AttrsType.UniqueIdentifier))
              case "Owner" =>
                Seq(direct(a, AttrsType.UniqueIdentifier))
              case "Virtual" =>
                Nil
              case x@_ =>
                throw new RuntimeException(s"Attribute ${a.logicalName}: Unhandled attribute type $x")
            }
          //          case _ =>
          //            throw new Ill
          //            Seq(Mapping(Spec(attr.logicalName, AttrsType.Char(10)), Spec(attr.logicalName, AttrsType.Char(10))))
        })
        attributes ++= as
      }
      val col = new Column()
      col.setName("auto_id")
      col.setType(TypeMap.INTEGER)
      col.setAutoIncrement(true)
      table.addColumn(col)
      db.addTable(table)

      emapping += Mapping(Spec("auto_inc", AttrsType.Auto), EmptySlot)
      mappings += ent.logicalName -> emapping

      val anames = ent.retrievableAttributes.map(_.logicalName.toLowerCase)
      val hasCreatedOn = anames.find(_ == "createdon")
      val hasModifiedOn = anames.find(_ == "modifiedon")

      jtables += Json.obj(
        "entityname" -> Json.fromString(ent.logicalName),
        "tablename" -> Json.fromString(ent.logicalName),
        "mappings" -> Json.fromValues(attributes),
        "timestampAttributes" -> Json.fromValues(Seq(Json.fromString("createdOn"), Json.fromString("modifiedOn"))))
    }
    (db, root.add("tables", Json.fromValues(jtables)), mappings)
  }

  def toColumn(spec: Spec): Validated[String, Column] = {
    import AttrsType._
    spec.atype match {
      case Char(size, unicode) =>
        val col = new Column()
        col.setName(spec.name)
        col.setType(TypeMap.CHAR)
        col.setSize(size.toString)
        col.valid
      case String(maxLength, minLength, info, unicode) => null
      case Decimal(precision, scale) => null
      case Binary(maxLength) => null
      case VarBinary(maxLength) => null
      case Auto => null
      case UniqueIdentifier => null
      case Float => null
      case Double => null
      case Bit => null
      case SmallInt => null
      case Integer => null
      case Long => null
      case BigInt => null
      case Money => null
      case Clob => null
      case Blob => null
      case Timestamp => null
      case Time => null
      case Date => null
      case Boolean => null
      case BooleanChar => null
      case BoleanInt =>
        val col = new Column()
        col.setName(spec.name)
        col.setType(TypeMap.BOOLEAN)
        col.valid
     case t: Ref.type => "No conversion from Ref".invalid
      case t: Other => "No conversion from Other".invalid
      case t: Object => "No conversion from Object".invalid
    }
  }

  def toColumn(attr: Attribute): Seq[Column] = {

    def c(attr: Attribute) = {
      val c = new Column()
      c.setName(attr.logicalName)
      c.setDescription(attr.logicalName)
      c
    }

    attr match {
      case a: LookupAttribute =>
        val col = c(a)
        col.setType(TypeMap.CHAR)
        col.setSize(guidSizeStr)
        val col2 = new Column();
        col2.setName(a.logicalName + "_tgt")
        col2.setDescription("Entity reference target.")
        col2.setType(TypeMap.VARCHAR)
        col2.setSize("100")
        Seq(col, col2)
      case a: PicklistAttribute =>
        val col = c(a)
        col.setType(TypeMap.INTEGER)
        val col2 = new Column()
        col2.setName(a.logicalName + "_label")
        col2.setType(TypeMap.VARCHAR)
        col2.setSize("100")
        Seq(col, col2)
      case a: StatusAttribute =>
        val col = c(a)
        col.setType(TypeMap.INTEGER)
        val col2 = new Column()
        col2.setName(a.logicalName + "_label")
        col2.setType(TypeMap.VARCHAR)
        col2.setSize("100")
        Seq(col, col2)
      case a: StateAttribute =>
        val col = c(a)
        col.setType(TypeMap.INTEGER)
        val col2 = new Column()
        col2.setName(a.logicalName + "_label")
        col2.setType(TypeMap.VARCHAR)
        col2.setSize("100")
        Seq(col, col2)
      case a: StringAttribute =>
        val col = c(a)
        col.setType(TypeMap.VARCHAR)
        col.setSize(a.maxLength.toString)
        Seq(col)
      case a: IntegerAttribute =>
        val col = c(a)
        col.setType(TypeMap.INTEGER)
        Seq(col)
      case a: DateTimeAttribute =>
        val col = c(a)
        col.setType(TypeMap.TIMESTAMP)
        Seq(col)
      case a: EntityNameAttribute =>
        val col = c(a)
        col.setType(TypeMap.VARCHAR)
        col.setSize("100")
        Seq(col)
      case a: MemoAttribute =>
        val col = c(a)
        col.setType(TypeMap.VARCHAR)
        col.setSize(a.maxLength.toString)
        Seq(col)
      case a: BooleanAttribute =>
        val col = c(a)
        col.setType(TypeMap.BOOLEAN)
        Seq(col)
      case a: DecimalAttribute =>
        val col = c(a)
        col.setType(TypeMap.DECIMAL)
        a.precision.foreach(p => col.setPrecisionRadix(p))
        Seq(col)
      case a: MoneyAttribute =>
        // TODO handle precision better
        val col = c(a)
        col.setType(TypeMap.DECIMAL)
        col.setPrecisionRadix(2)
        Seq(col)
      case a: DoubleAttribute =>
        val col = c(a)
        col.setType(TypeMap.DOUBLE)
        Seq(col)
      case a: BigIntAttribute =>
        val col = c(a)
        col.setType(TypeMap.BIGINT)
        Seq(col)
      case a: BasicAttribute =>
        // bit of a catch-all...
        val col = c(a)
        a.attributeType match {
          case "BigInt" =>
            col.setType(TypeMap.BIGINT)
          case "Uniqueidentifier" =>
            col.setType(TypeMap.CHAR)
            col.setSize(guidSizeStr)
          case "Boolean" =>
            col.setType(TypeMap.BOOLEAN)
          case "ManagedProperty" =>
            col.setType(TypeMap.VARCHAR)
            col.setSize("40")
          case "Customer" =>
            col.setType(TypeMap.CHAR)
            col.setSize(guidSizeStr)
          case "Owner" =>
            col.setType(TypeMap.CHAR)
            col.setSize(guidSizeStr)
          case "Virtual" =>
            Nil
          case x@_ =>
            throw new RuntimeException(s"Attribute ${a.logicalName}: Unhandled attribute type $x")
        }
        Seq(col)
      case x@_ =>
        throw new RuntimeException(s"Unhandled attribute metadata $x")
    }
  }
*/
}
