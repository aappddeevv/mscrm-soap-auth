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
import sdk.metadata.xmlreaders._
import sdk._
import sdk.metadata._
import sdk.metadata.soapwriters._
import sdk.driver._
import sdk.messages._

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
  private def getSchema(config: Config): Either[String, CRMSchema] = {
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
  val _context = root.context.obj

  /** Commands specific to running scripts. */
  sealed trait CrmCommand

  /** A command that involves entities. */
  trait CrudCommand extends CrmCommand {
    def entity: String
    def parameters: Map[String, Any]
    def context: Map[String, Any]
  }
  case class Create(entity: String, parameters: Map[String, Any], context: Map[String, Any]) extends CrudCommand
  case class Update(entity: String, id: String, parameters: Map[String, Any], context: Map[String, Any]) extends CrudCommand
  case class Delete(entity: String, id: String, parameters: Map[String, Any], context: Map[String, Any]) extends CrudCommand
  case class Callback(context: Map[String, Any] = Map()) extends CrmCommand
  case class CommandError(messages: NonEmptyList[String], json: Json) extends CrmCommand
  case class Ignore(msg: Option[String] = None) extends CrmCommand

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
            jcommands.zipWithIndex.map {
              case (c: Json, idx: Int) =>
                val action = _action.getOption(c).map(_.toLowerCase.trim)
                val entity = _entity.getOption(c).map(_.trim)
                val attrs = _attrs.getOption(c)
                val refId = _refid.getOption(c)
                val description = _description.getOption(c)
                val id = _id.getOption(c)
                val ignore = (_ignore.getOption(c) orElse Option(false)).getOrElse(false)
                val context = _context.getOption(c)
                var ent: Option[EntityDescription] = schema.entities.find(_.logicalName == entity.getOrElse("_"))

                val parameters: Option[Map[String, Any]] =
                  for {
                    e <- entity.map(_.trim)
                    ent_ <- schema.entities.find(_.logicalName == e)
                    a <- attrs
                  } yield {
                    findAttributes(ent_, a.fields) match {
                      case Validated.Valid(found) =>
                        a.fields.map { f =>
                          val attrDesc = found(f)
                          val v = toValue(a(f), attrDesc, f)
                          (f, v)
                        }.toMap
                      case Validated.Invalid(_) =>
                        Map()
                    }
                  }
                if (ignore) Ignore(Some(s"Ignoring command $idx"))
                else {
                  val invalidValues = parameters.map(_.values.collect { case InvalidValue(msg) => msg }).getOrElse(Seq())
                  if (invalidValues.size > 0) {
                    CommandError(NonEmptyList.fromList(invalidValues.toList).getOrElse(NonEmptyList.of("Invalid parameter but unable to identify the issues.")), c)
                  } else {
                    val context = Map() ++ refId.map(i => Map("refid" -> i)).getOrElse(Map()) ++ Map("description" -> description)
                    action match {
                      case Some("create") =>
                        if (ent.isEmpty) CommandError(NonEmptyList.of(s"Entity name needed for command $action"), c)
                        else Create(ent.get.logicalName, parameters.get, context)
                      case Some("update") =>
                        if (id.isEmpty || ent.isEmpty) CommandError(NonEmptyList.of(s"Update command requires an entity and id to be specified."), c)
                        else Update(ent.get.logicalName, id.get, parameters.get, context)
                      case Some("delete") =>
                        if (id.isEmpty || ent.isEmpty) CommandError(NonEmptyList.of(s"Update command requires an entity and id to be specified."), c)
                        else Delete(ent.get.logicalName, id.get, Map(), context)
                      case Some("callback") =>
                        Callback()
                      case Some(n) =>
                        Ignore(Some("Ignoring unknown command $n"))
                      case _ =>
                        CommandError(NonEmptyList.of(s"No action attribute provided for command $idx"), c)
                    }
                  }
                }
            }
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
          val context = engine.EvalContextCase()
          import scala.concurrent.ExecutionContext.Implicits.global
          try { engine.run(context) }
          catch {
            case x@scala.util.control.NonFatal(_) =>
              engine.shutdown
              throw x
          }
        }
      case _ =>
        println("Unknown entity script request.")
    }
  }

  /**
   * Convert a Json value to a value for sending messages using the SDK.
   */
  private def toValue(a: Option[Json], attrDesc: Attribute, f: String): Any = {
    attrDesc.attributeType match {
      case "String" =>
        a.flatMap(_.asString).getOrElse(InvalidValue(s"Expecting string for $f."))
      case "Int" =>
        a.flatMap(_.asNumber).map(_.toInt).getOrElse(InvalidValue(s"Expecting integer for $f."))
      case "Boolean" =>
        a.flatMap(_.asBoolean).getOrElse(InvalidValue(s"Expecteding boolean for $f."))
      case "Picklist" =>
        // we either have an int or a string to lookup, or an int for a string
        val wrongTypeValue = InvalidValue("Picklist value must be an integer or the picklist label.")
        val j = a.map { v =>
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
        a.flatMap(_.asString)
          .flatMap(d => catching(classOf[java.time.format.DateTimeParseException]) opt java.time.Instant.parse(d))
          .getOrElse(InvalidValue(s"Expecting $f in datetime in format yyyy-mm-ddThh:mm:ss:Z."))
      case "Lookup" =>
        // must account for a possible entity reference signalled by "$id"
        val wrongTypeValue = InvalidValue(s"Lookup must be a string with a GUID, a json object with a GUID/target or null.")
        val r = a.map { v =>
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
        a.flatMap(_.asString).getOrElse(InvalidValue(s"Expecting entity name as a string for $f"))
      case _ =>
        InvalidValue(s"Unknown attribute type for $f.")
    }
  }

}

/**
 * Script execution engine.
 *  Could use a free monad but did not have time to set that up.
 */
abstract class ScriptExecutionEngine(config: Config, commands: Seq[EntityScript.CrmCommand]) {

  import dispatch._
  import Defaults._
  import CrmAuth._
  import httphelpers._
  import com.lucidchart.open.xtract._
  import cats.syntax.either._
  import cats.implicits._
  import EntityScript._

  //type EvalContext >: Null <: EvalContextDef
  type EvalContext = EvalContextCase

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

  case class EvalContextCase(state: Map[String, Any] = Map()) extends EvalContextDef

  /** An action that runs before or after the entity command. */
  type EvalAction = EvalContext => EvalContext

  val client = DispatchClient.fromConfig(config)

  /** Get a typed value from a context only if the key starts with $ else None */
  def deref[A](key: String, context: Map[String, Any]) = {
    if (key.startsWith("$")) context.getAs[A](key.substring(1))
    else None
  }

  def shutdown: Unit = {
    client.shutdownNow
  }

  def callPre(c: CrmCommand, ctx: Map[String, Any]) = {
    config.commandPre(c, ctx)
  }

  def callPost(c: CrmCommand, ctx: Map[String, Any]) = {
    config.commandPost(c, ctx)
  }

  def run(initialContext: EvalContext)(implicit ec: ExecutionContext, logger: Logger): Unit = {
    var ccontext = initialContext
    for (c <- commands) {
      val (cc, ctxxx) = callPre(c, ccontext.state)
      ccontext = ccontext.copy(state = ctxxx)
      println(s"RUNNING COMMAND: $cc")
      val newcontext = cc match {
        case Create(e, p, c_) =>
          val cr = messages.CreateRequest(e, p)
          val cresponse = client.execute(cr).value.unsafeRun
          logger.debug(s"Create request response: $cresponse")
          cresponse match {
            case Right(r) =>
              val id = r.results.getAs[sdk.TypedServerValue]("id").map(_.text).getOrElse(s"No id of the created entity $e found.")
              //println(s"id: $id")
              val idkey = c_.getAs[String]("refid").getOrElse("lastecreatedid")
              //println(s"idkey: $idkey")
              ccontext.copy(state = ccontext.state ++ Seq(idkey -> id, "lastcreatedid" -> id))
            case Left(rerr) =>
              throw new RuntimeException(s"""${toUserMessage(rerr)}""")
          }
        case Update(e, id, p, c_) =>
          val uid = deref[String](id, ccontext.state).getOrElse(id)
          //println(s"uidq: $uid")
          val ur = messages.UpdateRequest(e, uid, p)
          val uresponse = client.execute(ur).value.unsafeRun
          logger.debug(s"Update response: $uresponse")
          uresponse match {
            case Right(r) =>
              ccontext.copy(state = ccontext.state ++ Seq("lastupdatedid" -> ""))
            case Left(rerr) =>
              throw new RuntimeException(s"""${toUserMessage(rerr)}""")
          }
        case Delete(e, id, p, c_) =>
          val uid = deref[String](id, ccontext.state).getOrElse(id)
          //println(s"did: $uid")
          val dr = messages.DeleteRequest(e, uid)
          val dresponse = client.execute(dr).value.unsafeRun
          logger.debug(s"Delete response: $dresponse")
          dresponse match {
            case Right(r) =>
              ccontext.copy(state = ccontext.state ++ Seq("lastdeletedid" -> uid))
            case Left(rerr) =>
              throw new RuntimeException(s"""${toUserMessage(rerr)}""")
          }
        case Callback(c_) =>
          val ctx: Map[String, Any] = config.commandCallback(ccontext.state, c_) match {
            case Right(m) => 
              m
            case Left(msg) => 
              throw new RuntimeException(s"Callback indicated an error and to stop the script: $msg")
          }
          ccontext.copy(state = ccontext.state ++ ctx)
        case Ignore(msgopt) =>
          msgopt.foreach { m => println(m) }
          ccontext
        case _ =>
          throw new RuntimeException(s"Unrecoginized command $c")
      }
      //println(s"Context at end of commands: $newcontext")
      ccontext = newcontext.copy(state = callPost(c, newcontext.state))
    }
  }
}

/** Engine that runs with a SOAP protocol. */
class SoapScriptExecutionEngine(config: Config, commands: Seq[EntityScript.CrmCommand])
    extends ScriptExecutionEngine(config, commands) {
  //    type EvalContext = EvalContextDef
  //    case class EvalContextDef(state: Map[String, Any] = Map()) extends super.EvalContextDef
}
