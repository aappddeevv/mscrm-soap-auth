package crm

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
import java.time._
import java.time.temporal._
import crm.sdk.client._

case class Config(
  /** Duration of an authorization in minutes. */
  leaseTimeInMin: Int = 120,
  /**
   * % of leasTime that an auth is renewed. This gives time for the auth
   *  to be generated because it takes time to generate and receive an auth.
   */
  leaseTimeRenewalFraction: Double = 0.9,
  /** Show help. */
  help: Boolean = false,
  /**
   * Web app url for CRM online.
   */
  url: String = "",
  servicesUrl: String = "",
  /**
   * Region for the org you want to connect to. You may have many
   * orgs within a region that you can connect to.
   */
  region: String = Option(System.getenv.get("REGION")).getOrElse("NA"),
  /** Username */
  username: String = Option(System.getenv.get("USERNAME")).getOrElse(""),
  /** Password */
  password: String = Option(System.getenv.get("PASSWORD")).getOrElse(""),
  /** Authentication model. */
  auth: AuthenticationProviderType = Online,
  /**
   * Timeout to wait for an individual remote request.
   */
  timeout: Int = 30 * 60, // seconds, wow that's long!
  mode: String = "auth",
  filterFilename: String = "filters.txt",
  objects: String = "Entity Relationships Attributes",
  output: String = "metadata.xml",
  discoveryAction: String = "listRegions",
  metadataAction: String = "dumpRawXml",
  wsdlFilename: String = "wsdl.xml",
  sdkVersion: Option[String] = None,
  queryType: String = "countEntities",
  queryEntity: String = "contact",
  queryCountFilterFilename: String = "count-filters.txt",
  queryFilters: Seq[String] = Seq(),
  keyfileChunkSize: Int = 2000000,
  keyfilePrefix: String = "",
  keyfileEntity: String = "",
  dump: Dump = Dump(),
  /** Comnand to run for entity scripts. */
  entityCommand: String = "runCommands",
  /** Sychronous callback. */
  commandCallback: (Map[String, Any], Map[String, Any]) => Either[String, Map[String, Any]] = (ctx, cctx) => Right(ctx)
  /** Hook prior to running a command. */
  //,commandPre: (EntityScript.CrmCommand, Map[String, Any]) => (EntityScript.CrmCommand, Map[String, Any]) = (x, y) => (x, y)
  /** Hook post to running a command. */
  //commandPost: (EntityScript.CrmCommand, Map[String, Any]) => Map[String, Any] = (x, y) => y,
  /** JSON command file. */
  ,commandFile: String = "commands.json",
  //connectionPoolIdelTimeoutInSec: Int = 5*60,
  /** Amount of concurrency e.g. # of operations to try at the same time. */
  concurrency: Int = 2,
  pconcurrency: Int = 5,
  /** Default parallelism. This is used to set the number of threads. */
  parallelism: Int = 8,
  /** Number of retrys if a HTTP request fails to execute correctly. */
  httpRetrys: Int = 5,
  /** Pause between retries. See httpRetrys. */
  pauseBetweenRetriesInSeconds: Int = 30,
  ignoreKeyFiles: Boolean = false,
  keyfileChunkFetchSize: Int = 5000,
  take: Option[Long] = None,
  drop: Option[Long] = None,
  outputFormattedValues: Boolean = true,
  formattedValuesSuffix: String = "_fV",
  outputExplodedValues: Boolean = true,
  copyDatabaseType: String = "oracle",
  copyMetadataFile: Option[String] = None,
  copyAction: String = "",
  copyFilterFilename: String = "")

/**
 *  Create a key file for a single entity. A key file contains the primary key
 *  and no other attributes. The name of the file, the prefix, should indicate
 *  the entity somehow although that is not a requirement.
 */
case class CreatePartitions(entity: String = "",
  /** List of attributes to dump, if None, dump just the primary key. */
  attributes: Option[Seq[String]] = Option(Nil),
  outputFilePrefix: String = "",
  chunkSize: Int = 2000000,
  attributeSeparator: String = ",",
  recordSeparator: String = "\n",
  pconcurrency: Int = 4)

/**
 * Dump an entity. This structure is not really used.
 */
case class Dump(entity: String = "",
  outputFilename: String = "",
  header: Boolean = true,
  batchSize: Int = 0, // usually cannot be greate than 5,000
  attributeList: Option[Seq[String]] = None,
  attributeListFilename: Option[String] = None,
  statusFrequency: Int = 100000,
  attributeSeparator: String = ",",
  recordSeparator: String = "\n",
  /**
   * Run an Entity (a record) through a processor that could
   *  transform it in some way.
   */
  recordProcessor: fs2.Pipe[Task, (sdk.Entity, Int), (sdk.Entity, Int)] = fs2.pipe.id
  /**
   * Convert a record from CRM (an Entity) into an output string suitable
   *  for whatever output format you want.
   */
  //,toOutputProcessor: fs2.Pipe[Task, sdk.Entity, String] = defaultMakeOutputRow // Traversable[String] => fs2.Pipe[Task, sdk.Entity, String] = defaultMakeOutputRow _)
  )

object program {

  import sdk.metadata._
  import sdk._

  private[this] implicit val logger = getLogger

  val defaultConfig = Config()
  val parser = new scopt.OptionParser[Config]("crmauth") {
    override def showUsageOnError = true

    val urlOpt = opt[String]('r', "url").optional().valueName("<url>").text("Organization service url.").
      action((x, c) => c.copy(url = x))
    val regionOpt = opt[String]("region").optional().valueName("<region abbrev>").text("Organization region abbreviation e.g. NA.").
      action((x, c) => c.copy(region = x))
    val servicesUrlOpt = opt[String]("services-url").optional().valueName("<services url>").text("Org services url (not the web app url)").
      action((x, c) => c.copy(servicesUrl = x))

    head("crmauth", "0.1.0")
    opt[String]('u', "userid").optional().valueName("<userid>").text("Userid")
      .action((x, c) => c.copy(username = x))
    opt[String]('p', "password").optional().valueName("<password>").text("Password")
      .action((x, c) => c.copy(password = x))
    opt[Int]('t', "timeout").valueName("<number>").text(s"Timeout in seconds for each request. Default is ${defaultConfig.timeout}")
      .action((x, c) => c.copy(timeout = x))
    opt[Int]("auth-lease-time").text(s"Auth lease time in minutes. Default is ${defaultConfig.leaseTimeInMin}")
      .action((x, c) => c.copy(leaseTimeInMin = x))
    opt[Double]("renewal-quantum").text(s"Renewal fraction of leaseTime. Default is ${defaultConfig.leaseTimeRenewalFraction}")
      .action((x, c) => c.copy(leaseTimeRenewalFraction = x))
    //    opt[Int]("connectionPoolIdelTimeoutInSec").text(s"If you get idle timeouts, make this larger. Default is ${defaultConfig.connectionPoolIdelTimeoutInSec}").
    //      action((x, c) => c.copy(connectionPoolIdelTimeoutInSec = x))

    opt[Unit]("authonline").text("CRM Online authentication.")
      .action((x, c) => c.copy(auth = Online))
    opt[Unit]("authifd").text("CRM IFD On Prem authentication.")
      .action((x, c) => c.copy(auth = Federation))
    opt[Int]("parallelism").text("Parallelism.").
      validate(con =>
        if (con < 1 || con > 32) failure("Parallelism must be between 1 and 32")
        else success).
      action((x, c) => c.copy(parallelism = x))
    opt[Int]("concurrency").text("Concurrency factor.").
      validate(con =>
        if (con < 1 || con > 16) failure("Concurrency must be between 1 and 32")
        else success).
      action((x, c) => c.copy(concurrency = x))
    opt[Int]("http-retries").text(s"# of http retries. Default is ${defaultConfig.httpRetrys}").
      action((x, c) => c.copy(httpRetrys = x))
    opt[Int]("retry-pause").text(s"Pause between retries in seconds. Default is ${defaultConfig.pauseBetweenRetriesInSeconds}").
      action((x, c) => c.copy(pauseBetweenRetriesInSeconds = x))

    help("help").text("Show help")
    note("Environment variables USERNAME, PASSWORD and REGION are used if they are not specified on the command line.")
    note("")

    cmd("create-test").action((_, c) => c.copy(mode = "create")).
      text("Create entity test.").
      children(
        urlOpt)
    note("You need a username/password and -r URL to run this command.")
    note("")

    cmd("metadata").action((_, c) => c.copy(mode = "metadata")).
      text("Obtain entity metadata from an organization").
      children(
        opt[String]("filter-file").text("File with one line per regex filter.").
          action((x, c) => c.copy(filterFilename = x)),
        opt[String]('r', "url").optional().valueName("<url>").text("Organization service url.").
          action((x, c) => c.copy(url = x)),
        opt[String]('o', "objects").valueName("<Entity Relationship Attributes>").text("What metadata to return. Space separated list. Use quotes in shell. All metadata is returned by default.").
          action((x, c) => c.copy(objects = x)),
        opt[String]("output").valueName("<filename>").text("Output file for metadata retrieved using -m. Default is '${config.objects}'").
          action((x, c) => c.copy(output = x)),
        opt[Unit]("generate-ddl").text("Generate DDL. Uses filter file to target specific entities.").
          action((x, c) => c.copy(metadataAction = "generateDdl")))
    note("You need a username/password and url to run this command.")
    note("The entire SOAP response envelope is output when dumping entity metadata. Individual entries are under EntityMetadata")
    note("")

    cmd("auth").action((_, c) => c.copy(mode = "auth")).
      text("Check that authentication works. This is the default command.").
      children(
        opt[String]('r', "url").optional().valueName("<url>").text("Organization service url.").
          action((x, c) => c.copy(url = x)))
    note("You need a username/password and -r URL to run the auth command.")
    note("")

    cmd("discovery").action((_, c) => c.copy(mode = "discovery")).
      text("Discovery and work with endpoints").
      children(
        opt[String]('r', "url").optional().valueName("<url>").text("Organization service url.").
          action((x, c) => c.copy(url = x)),
        opt[String]("services-url").valueName("<web app url>").optional().text("Find the org services url from a web app url. Can also specify a region abbrev")
          .action((x, c) => c.copy(discoveryAction = "findServicesUrl", url = x)),
        opt[String]("region").valueName("<region abbrev>").optional().text("The region abbrevation to use.")
          .action((x, c) => c.copy(region = x)),
        opt[String]("sdk-version").valueName("<sdk version e.g. 8.0>").optional().text("SDK version to use when obtaining WSDL.")
          .action((x, c) => c.copy(sdkVersion = Some(x))),
        opt[String]("save-org-svc-wsdl-to").valueName("<wsdl output file>").optional().text("Retrieve and save the organization service WSDL")
          .action((x, c) => c.copy(discoveryAction = "saveOrgSvcWsdl", wsdlFilename = x)),
        opt[String]("save-disc-wsdl-to").valueName("<wsdl output file>").optional().text("Retrieve and save the discovery WSDL")
          .action((x, c) => c.copy(discoveryAction = "saveDiscoveryWsdl", wsdlFilename = x)),
        opt[Unit]("list-regions").optional().text("List known (to this program) regions (abbrev and URLs) to use when finding endpoints. This is the default discovery action."),
        opt[String]("list-endpoints").valueName("<region abbrev>").optional().text("List the specific orgs given a specific region and username/pasword. Default region is NA.")
          .action((x, c) => c.copy(region = x, discoveryAction = "listEndpoints")))

    note("You only need a username/password and discovery URL or the region to run a discovery command.")
    note("")

    cmd("entity").action((_, c) => c.copy(mode = "entity")).
      text("Create or modify entities using a json command file.").
      children(
        opt[String]('r', "url").optional().valueName("<url>").text("Organization service url.").
          action((x, c) => c.copy(url = x)),
        opt[String]("region").optional().valueName("<region abbrev>").text("Organization region abbreviation e.g. NA.").
          action((x, c) => c.copy(region = x)),
        opt[String]("command-file").valueName("<command file name>").optional().text("Run commands in he form of a json file with specialized syntax.")
          .action((x, c) => c.copy(entityCommand = "runCommands", commandFile = x)))
    note("The results of the commands are output to the terminal and can be inspected.")
    note("")

    cmd("query").action((_, c) => c.copy(mode = "query")).
      text("Run a query. Entity names should be logical names e.g. contact").
      children(
        opt[String]('r', "url").optional().valueName("<url>").text("Organization service url.").
          action((x, c) => c.copy(url = x)),
        opt[String]("region").optional().valueName("<region abbrev>").text("Organization region abbreviation e.g. NA.").
          action((x, c) => c.copy(region = x)),
        opt[String]("query-filterfile").valueName("<filename>").text("Input regexp filter, one filter per line. No filters means accept everything.")
          .action((x, c) => c.copy(queryCountFilterFilename = x)).validate { filename =>
            if (File(filename).exists) success
            else failure(s"No filter file ${filename} found.")
          },
        opt[String]("qf").unbounded().valueName("<regex>").text("Regex used to identify which entities are included. Repeat option as needed. May need to be escaped or quoted on command line.")
          .action((x, c) => c.copy(queryFilters = c.queryFilters :+ x)),
        opt[Unit]("count").text("Count records for the given entity name expressed in the filters.")
          .action((x, c) => c.copy(queryType = "countEntities")),
        opt[Boolean]("output-formatted-values").text("Output formatted values in additition to the attribute values.")
          .action((x, c) => c.copy(outputFormattedValues = x)),
        opt[Boolean]("output-exploded-values").text("Output eploded values in additition to the attribute values.")
          .action((x, c) => c.copy(outputExplodedValues = x)),
        opt[String]("formatted-values-suffix").text("Suffix to put on the attribute names that represent formatted values.").
          action((x, c) => c.copy(formattedValuesSuffix = x)),
        opt[String]("create-partition").text("Create a set of persistent primary key partitions for entity.").
          action((x, c) => c.copy(keyfileEntity = x, queryType = "partition")),
        opt[Unit]("create-attribute-file").text("Create an attribute file that can be used for downloading. Download, edit then use --attribute-file. Output is to attributes.csv.").
          action((x, c) => c.copy(queryType = "createAttributeFile")),
        opt[Int]("keyfile-chunk-fetch-size").text("Fetch size when using a keyfile to dump records.").
          action((x, c) => c.copy(keyfileChunkFetchSize = x)),
        opt[Int]("take").text("Take n rows.").
          action((x, c) => c.copy(take = Option(x))),
        opt[Int]("drop").text("Drop leading n rows.").
          action((x, c) => c.copy(drop = Option(x))),
        opt[Unit]("ignore-keyfiles").text("Ignore keyfiles during a dump, if present for an entity.").
          action((x, c) => c.copy(ignoreKeyFiles = true)),
        opt[String]("dump").valueName("<entity name interpreted as a regex>").text("Dump entity data into file. Default output file is 'entityname'.csv.").
          action((x, c) => c.copy(queryType = "dump", dump = c.dump.copy(entity = x))),
        opt[String]("output-filename").valueName("<dump filename>").text("Dump file name. Default is entitityname.csv").
          action((x, c) => c.copy(dump = c.dump.copy(outputFilename = x))),
        opt[Unit]("header").text("Add a header to the output. Default is no header.").
          action((x, c) => c.copy(dump = c.dump.copy(header = true))),
        opt[Int]("statusfrequency").text("Report a status every n records.").
          action((x, c) => c.copy(dump = c.dump.copy(statusFrequency = x))),
        opt[Int]("batchsize").valueName("<batch size as int>").text("Number of records to retrieve for each server call.").
          action((x, c) => c.copy(dump = c.dump.copy(batchSize = x))),
        opt[String]("attribute-file").text("CSV file  of attributes, (entity logical name, attribute logical name, download y/n).").
          action((x, c) => c.copy(dump = c.dump.copy(attributeListFilename = Some(x)))),
        opt[String]("attributes").text("Comma separate list of attributes to dump.").
          action((x, c) => c.copy(dump = c.dump.copy(attributeList = Some(x.split(","))))))
    note("Attribute file content and attributes specified in --attributes are merged.")
    note("All double quotes and backslashes are removed from dumped values. Formatted values are not dumped.")
    note("")

    cmd("copy").action((_, c) => c.copy(mode = "copy")).text("Copy or update a copy of a local RDBMS from CRM online.")
      .children(
        opt[String]('r', "url").optional().valueName("<url>").text("Organization service url.").
          action((x, c) => c.copy(url = x)),
        opt[String]("region").optional().valueName("<region abbrev>").text("Organization region abbreviation e.g. NA.").
          action((x, c) => c.copy(region = x)),
        opt[String]("filterfile").valueName("<filename>").text("Input regexp filter, one filter per line. No filters means accept everything.")
          .action((x, c) => c.copy(copyFilterFilename = x)).validate { filename =>
            if (File(filename).exists) success
            else failure(s"No filter file ${filename} found.")
          },
        cmd("ddl").action((_, c) => c.copy(copyAction = "ddl")).text("DDL generation")
          .children(
            opt[String]("crm-metadata-file").optional().text("Filename holding the XML metadata. You can use this program to dump the metadata so it is locally accessible").
              action((x, c) => c.copy(copyMetadataFile = Option(x))),
            opt[String]("dbtype").optional().text("Generate ddl for a specific database type. Use list-targets to see possible target names.")
              action ((x, c) => c.copy(copyDatabaseType = x)),
            cmd("list-targets").action((_, c) => c.copy(copyAction = "listTargets")).text("List database target types.")))
    note("There are a few steps involved in using this command. See the documentation.")
    note("Metadata to create the local RDBMS schema can be generated from a local copy of the metadata infromation or dynamically retrieved from CRM online.")
    note("")

    cmd("test").action((_, c) => c.copy(mode = "test")).text("Run some tests.").
      children(
        urlOpt,
        regionOpt,
        servicesUrlOpt)
    note("")

    def emptyUOrP(c: Config): Boolean = c.username.trim.isEmpty || c.password.trim.isEmpty

    checkConfig { c =>
      c.mode match {
        case "discovery" =>
          if (c.discoveryAction == "listEndpoints" && emptyUOrP(c))
            failure("Listing endpoints requires an username and password.")
          else if (c.discoveryAction == "saveOrgSvcWsdl" && (emptyUOrP(c) || c.url.trim.isEmpty))
            failure("Saving organization service WSDL requires username password and url.")
          else if (c.discoveryAction == "saveDiscoveryWsdl" && (emptyUOrP(c) || c.url.trim.isEmpty))
            failure("Saving discoery WSDL requires username password and url.")
          else if (c.discoveryAction == "findServicesUrl" && (emptyUOrP(c) || c.region.trim.isEmpty))
            failure("Obtaining a services URL requires an username, password, web app url and region.")
          else
            success
        case "auth" =>
          if (emptyUOrP(c))
            failure("Auth check requires an username, password and url.")
          else success
        case "create-test" =>
          if (emptyUOrP(c))
            failure("Create test requires an username, password and url.")
          else success
        case "query" =>
          if (emptyUOrP(c) || c.url.trim.isEmpty) failure("Queries require a username, password and url.")
          else success
        case "metadata" =>
          if (emptyUOrP(c) || c.url.trim.isEmpty)
            failure("Metadata requires an username, password and url.")
          else success
        case "entity" =>
          if (emptyUOrP(c) || c.url.trim.isEmpty) failure("Entity commands require a username, password and url.")
          else success
        case "test" => success
        case "copy" => success
      }
    }

    note("")
    note("The organization service url can be obtained from the developer resources web page within your CRM org or using the discovery command.")
    note("This program only works with MS CRM Online.")
  }

  def main(args: Array[String]): Unit = {
    utils.mscrmConfigureLogback(Option("mscrm.log"))

    val config = parser.parse(args, defaultConfig) match {
      case Some(c) => c
      case None => return//Http.shutdown; return
    }
    
    
    val start = Instant.now
    println(s"Program start: ${instantString}")
    try {
      config.mode match {
//        case "discovery" => Discovery(config)
//        case "auth" => Auth(config)
//        case "metadata" => Metadata(config)
//        case "create" => Create(config)
//        case "query" => Query(config)
//        case "entity" => EntityScript(config)
        case "test" => Test(config)
//        case "copy" => Copy(config)
        case _ =>
          println("Unknown command.")
      }
    } catch {
      case scala.util.control.NonFatal(e) =>
        logger.error(e)("Error occurred at the top level.")
        println("An unexpected and non-recoverable error during processing. Processing has stopped.")
        println("Error: " + e.getMessage)
    } finally {
      //Http.shutdown
    }
    val stop = Instant.now
    println(s"Program stop: ${instantString}")
    val elapsed = java.time.Duration.between(start, stop)
    println(s"Program runtime in minutes: ${elapsed.toMinutes}")
    println(s"Program runtime in seconds: ${elapsed.toMillis / 1000}")
  }

  /**
   * Create a post request from a config object.
   */
  //def createPost(config: Config): Req = crm.sdk.httphelpers.createPost(config.url)
}

object utils {
  import java.nio.file._
  import org.slf4j._
  import ch.qos.logback.classic._
  import ch.qos.logback.classic.joran._
  import ch.qos.logback.core.joran.spi._

  val rootLoggerName = org.slf4j.Logger.ROOT_LOGGER_NAME

  def mscrmConfigureLogback(outputlogfile: Option[String] = None): Unit =
    configureLogback(None, outputlogfile, Option(getDefaultLogDir()), Option(getDefaultConfDir()))

  /**
   * Configure logback.
   *
   * @param configfile Config file name, not the full path prefix. Default is `logback.xml`.
   * @param outputlogfile Output log file name. Default is `app.log`.
   * @param logDir Directory for the log output. Default is `.`. LOG_DIRECTORY Can be used in the config file.
   * @param confDir Directory to look for the logging configuration file `configfile`. Default is `.`.
   * @param props Properties for the logging context that become available for variable substitution.
   */
  def configureLogback(
    configfile: Option[String] = None,
    outputlogfile: Option[String] = None,
    logDir: Option[String] = None,
    confDir: Option[String] = None,
    props: Map[String, String] = Map()): Unit = {

    val context = LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]
    context.reset()
    val configurator = new JoranConfigurator()
    context.putProperty("LOG_DIRECTORY", logDir getOrElse ".")
    props.foreach { case (k, v) => context.putProperty(k, v) }
    configurator.setContext(context)

    val loggingconfigfile = Paths.get(confDir getOrElse ".", configfile getOrElse "logback.xml")
    def default() = createDefaultRootLogger(outputlogfile.getOrElse("app.log"), context)

    if (Files.exists(loggingconfigfile))
      try {
        configurator.doConfigure(loggingconfigfile.toString)
      } catch {
        case j: JoranException =>
          default()
          throw new RuntimeException(j)
      }
    else
      default()
  }

  /**
   *  Create a default root loogger that logs errors to `file`.
   */
  def createDefaultRootLogger(file: String, context: LoggerContext) = {
    import org.slf4j.LoggerFactory
    import ch.qos.logback.classic._
    import ch.qos.logback.classic.encoder._
    import ch.qos.logback.classic.spi._
    import ch.qos.logback.core._

    val logger = context.getLogger(rootLoggerName)
    logger.setLevel(Level.ERROR)
    val encoder = new PatternLayoutEncoder()
    encoder.setContext(context)
    encoder.setPattern("%d{HH:mm:ss.SSS} [%thread] %-5level %logger{35} - %msg%n")
    encoder.start()

    val appender = new FileAppender[ILoggingEvent]()
    appender.setContext(context)
    appender.setName("FILE")
    appender.setAppend(false)
    appender.setEncoder(encoder)
    appender.setFile(file)
    appender.start()
    logger.addAppender(appender)
    appender
  }

  /** MSCRM default log dir. */
  def getDefaultLogDir(env: Map[String, String] = sys.env): String = {
    env.get("MSCRM_LOG_DIR").map(t => Paths.get(t))
      .orElse(env.get("MSCRM_HOME").map(t => Paths.get(t, "logs")))
      .map(_.toAbsolutePath().toString)
      .getOrElse(Paths.get(".").toAbsolutePath().toString)
  }

  /** MSCRM default conf dir. */
  def getDefaultConfDir(env: Map[String, String] = sys.env): String = {
    env.get("MSCRM_CONF_DIR").map(t => Paths.get(t))
      .orElse(env.get("MSCRM_HOME").map(t => Paths.get(t, "conf")))
      .filter(p => Files.isDirectory(p))
      .map(_.toAbsolutePath().toString)
      .getOrElse(Paths.get(".").toAbsolutePath().toString)
  }

}
