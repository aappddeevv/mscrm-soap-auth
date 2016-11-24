package crm

import java.util.concurrent._

import scala.language._
import scala.util.control.Exception._
import scopt._
import org.w3c.dom._
import dispatch._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.async.Async._
import scala.util._
import org.log4s._
import cats._
import cats.data._
import cats.implicits._
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
import sdk.metadata.readers._
import sdk._
import sdk.metadata._

/**
 *  Run a "script" derived from JSON objects that creates, modifies or
 *  deletes objects in CRM. Entity scripts make it easier to run tests
 *  in an automated way or, depending on the source of the script,
 *  migrate data. The JSON format is greatly simplified compared
 *  to XML or even to the newer CRM REST APIs so a human being can
 *  type in the commands (albiet with a data dictionary handy to get
 *  the attribute names correct).
 *
 *  The script needs to fit into memory.
 *
 *  TODO: Streaming json reading and fs2 stream based support.
 */
object EntityScript {

  private[this] implicit val logger = getLogger

  import _root_.io.circe._
  import _root_.io.circe.parser._
  import _root_.io.circe.optics.JsonPath._
  import sdk.messages._

  /**
   * Get the schema from the cache or the online org.
   */
  def getSchema(config: Config): Either[String, CRMSchema] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val result = Query.orgAuthF(config).flatMap { orgAuth =>
      entityMetadata(Http, orgAuth, config.url)
    }
    Await.result(result, 300 seconds)
  }

  val _action = root.action.string
  val _entity = root.entity.string
  val _attrs = root.attributes.obj
  val _ignore = root.ignore.boolean
  val _refid = root.refid.string
  val _description = root.description.string
  val _id = root.id.string
  val _target = root.target.string

  /** Commands specific to running scripts. */
  sealed trait CrmCommand
  /** A command that involves entities. */
  trait CrudCommand extends CrmCommand {
    def entity: String
    def parameters: Map[String, Any]
    def context: Map[String, Any]
  }
  case class Create(entity: String, parameters: Map[String, Any], context: Map[String, Any]) extends CrudCommand
  case class Update(entity: String, id: Option[String], parameters: Map[String, Any], context: Map[String, Any]) extends CrudCommand
  //case class Delete(entity: String, id: Option[String]) extends CrudCommand
  case class Callback(context: Map[String, Any]) extends CrmCommand
  case class CommandError(messages: NonEmptyList[String], json: Json) extends CrmCommand

  import sdk.messages._

  def apply(config: Config): Unit = {
    config.entityCommand match {
      case "runCommands" =>
        println("Obtaining schema for this crm org...")
        val schema = getSchema(config).fold[CRMSchema](m => throw
          new RuntimeException(s"Unable to obtain schema: $m"), s => s)
        logger.debug(s"Schema for running entityscript: $schema")

        // entity:attribute:label - allows picklist labels to value lookups
        def mk(e: String, a: String, label: String) = e + ":" + a + ":" + label.trim
        val picklistRef = schema.entities.flatMap { e =>
          e.attributes.collect {
            //            case a: BasicAttribute if(a.attributeType == "Picklist") =>
            //              println(s"blah ${e.logicalName}, ${a.logicalName}")
            //              Seq(("",""))
            case p: PicklistAttribute =>
              //              println(s"found pick list ${e.logicalName}, ${p.logicalName}")
              p.options.options.map(o => (mk(e.logicalName, p.logicalName, o.value) -> o.label))
          }.flatten
        }.toMap

        println(s"Reading command file: ${config.commandFile}")
        val commandsAsString = config.commandFile.toFile.contentAsString
        val parseResult = parse(commandsAsString)
        logger.debug(s"Command file parsing result: $parseResult")

        val commands: Seq[CrmCommand] = parseResult.fold(
          failure =>
            {
              logger.error(failure.underlying)(s"Script file parsing error: ${failure.message}")
              Seq(CommandError(NonEmptyList.of(s"Script file parsing error: ${failure.message}."), Json.Null))
            },
          json => {
            val jcommands = json.cursor.focus.asArray.getOrElse(Nil)
            val commands = jcommands.zipWithIndex.map {
              case (c: Json, idx: Int) =>
                val action = _action.getOption(c)
                val entity = _entity.getOption(c)
                val attrs = _attrs.getOption(c)
                val refId = _refid.getOption(c)
                val id = _id.getOption(c)
                val ignore = (_ignore.getOption(c) orElse Option(false)).filterNot(x => x)

                val tmp = ignore.flatMap { _ =>
                  // must have entity name, description and attributes to continue
                  for {
                    e <- entity.map(_.trim)
                    ent <- schema.entities.find(_.logicalName == e)
                    a <- attrs
                  } yield {
                    val result = findAttributes(ent, a.fields)
                    result match {
                      case Validated.Valid(found) =>
                        // Process the script command to prep for execution
                        val p = a.fields.map { f =>
                          val attrDesc = found(f)
                          //println(s"BLAH: BLAH: BLAH: $f -> ${attrDesc.attributeType}")                          
                          val v = attrDesc.attributeType match {
                            case "String" =>
                              a(f).flatMap(_.asString).getOrElse(InvalidValue(s"Expecting string for $f."))
                            case "Int" =>
                              a(f).flatMap(_.asNumber).map(_.toInt).getOrElse(InvalidValue(s"Expecting integer for $f."))
                            case "Boolean" =>
                              a(f).flatMap(_.asBoolean).getOrElse(InvalidValue(s"Expecteding boolean for $f."))
                            case "Picklist" =>
                              // we either have an int or a string to lookup, or an int for a string
                              val wrongTypeValue = InvalidValue("Picklist value must be an integer or the picklist label.")
                              val j = a(f).map { v =>
                                v.fold(
                                  UnsetValue,
                                  _ => wrongTypeValue,
                                  n => n.toInt,
                                  s => {
                                    // lookup the value in the metadata
                                    //println(s"looking up $s in $attrDesc")
                                    val options = attrDesc.asInstanceOf[PicklistAttribute].options.options
                                    options.find(_.label == s)
                                      .map(ov => OptionSetValue(ov.value.toInt))
                                      .getOrElse(InvalidValue(s"""Unable to find label $s in option set for $f. Choices are '${options.map(_.label).mkString(",")}'"""))
                                  },
                                  _ => wrongTypeValue,
                                  _ => wrongTypeValue)
                              }
                              j.getOrElse(wrongTypeValue)
                            case "DateTime" =>
                              a(f).flatMap(_.asString)
                                .flatMap(d => catching(classOf[java.time.format.DateTimeParseException]) opt java.time.Instant.parse(d))
                                .getOrElse(InvalidValue(s"Expecting $f in datetime in format yyyy-mm-ddThh:mm:ss:Z."))
                            case "Lookup" =>
                              // must account for a possible entity reference signalled by "$id"
                              val wrongTypeValue = InvalidValue(s"Lookup must be a string with a GUID, a json object with a GUID/target or null.")
                              val r = a(f).map { v =>
                                v.fold(
                                  UnsetValue,
                                  _ => wrongTypeValue,
                                  _ => wrongTypeValue,
                                  s => s,
                                  _ => wrongTypeValue,
                                  jo => {
                                    // look for id and optionally id
                                    (_id.getOption(v), _target.getOption(v)) match {
                                      case (Some(id), Some(target)) =>
                                        EntityReference(id, Some(target))
                                      case (Some(id), None) =>
                                        EntityReference(id, None)
                                      case (None, None) => wrongTypeValue
                                      case (None, Some(target)) => wrongTypeValue
                                    }
                                  })
                              }
                              r
                            case "EntityName" =>
                              // just like a string
                              a(f).flatMap(_.asString).getOrElse(InvalidValue(s"Expecting entity name as a string for $f"))
                            case _ =>
                              InvalidValue(s"Unknown attribute type for $f.")
                          }
                          (f, v)
                        }.toMap

                        val invalidValues = p.values.collect { case InvalidValue(msg) => msg }
                        if (invalidValues.size > 0) {
                          CommandError(NonEmptyList.fromList(invalidValues.toList).getOrElse(NonEmptyList.of("Unable to identify parameter with error.")), c)
                        } else {
                          val context = Map() ++ refId.map(i => ("refid" -> i)).toMap
                          action match {
                            case Some("create") => Create(ent.logicalName, p, context)
                            case Some("update") => Update(ent.logicalName, id, p, context)
                            case _ =>
                              CommandError(NonEmptyList.of(s"No action specified or action command not recognized."), c)
                          }
                        }

                      case Validated.Invalid(m) =>
                        CommandError(NonEmptyList.of(s"Command $idx. Script command: $c") ++ m.toList, c)
                    }
                  }
                }
                tmp
            }
            commands.collect { case Some(c) => c }
          })

        val errors = commands.collect { case x: CommandError => x }
        val nonerrors = commands diff errors

        if (errors.size > 0) {
          println("Errors in script commands:")
          errors.foreach { e =>
            e.messages.toList.foreach(println)
          }
        } else {
          val engine = new SoapScriptExecutionEngine(config, nonerrors)
          val context = engine.EvalContextDef()
          import scala.concurrent.ExecutionContext.Implicits.global
          engine.run(context)
        }
      case _ =>
        println("Unknown entity script request.")
    }
  }
  
  /**
   * Script execution engine.
   *  Could use a free monad but did not have time to set that up.
   */
  abstract class ScriptExecutionEngine(config: Config, commands: Seq[CrmCommand]) {
    import dispatch._
    import Defaults._
    import CrmAuth._
    import httphelpers._
    import com.lucidchart.open.xtract._

    type EvalContext >: Null <: EvalContextDef

    /**
     * Evaluation context.
     *
     *  There are a few conventions:
     *  <ul>
     *  <li>lastEntity: GUID of last entity whether added, modified or deleted.</li>
     *  </ul>
     *
     */
    trait EvalContextDef {
      def state: Map[String, Any]
    }

    /** An action that runs before or after the entity command. */
    type EvalAction = EvalContext => EvalContext

    protected def fetch(reqBody: xml.Elem,
      reader: XmlReader[Envelope],
      logger: Logger) = {
      val q = Query.orgAuthF(config).flatMap { auth =>
        val request = soaprequestwriters.executeTemplate(reqBody, Some(auth))
        val headers = Map("SOAPAction" -> "http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute")
        val req = CrmAuth.createPost(config.url) <:< headers << request.toString
        logger.debug(s"fetch request: ${req.toRequest}")
        println("req: " + req.toRequest)
        Http(req).unwrapEx.either
          .map(r => processResponse(Option(request))(r)(reader, logger))
      }
      q
    }

    def run(context: EvalContext)(implicit ec: ExecutionContext, logger: Logger): Unit = {
      val requests = commands.foreach { c =>
        val result = c match {
          case Create(e, p, c) =>
            val reqBody = createRequestTemplate(e, p)
            println(s"request body: $reqBody")
            val q = fetch(reqBody, soapreaders.createRequestResponseReader, logger)
            val result = Await.result(q, Duration.Inf)
            result
          case Update(e, id, p, c) =>
            val reqBody = updateRequestTemplate(e, "", p)
            val q = fetch(reqBody, soapreaders.createRequestResponseReader, logger)
            val result = Await.result(q, Duration.Inf)            
            result
        }

        result match {
          case Left(t) =>
            println(s"Error during processing: ${t.getMessage}")
          case Right(e) =>
            println(s"Return envelope: $e")
        }
      }

    }
  }

  /** Engine that runs with a SOAP protocol. */
  class SoapScriptExecutionEngine(config: Config, commands: Seq[CrmCommand])
      extends ScriptExecutionEngine(config, commands) {
    type EvalContext = EvalContextDef
    case class EvalContextDef(state: Map[String, Any] = Map()) extends super.EvalContextDef
  }

}
