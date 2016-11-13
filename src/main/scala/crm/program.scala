package crm

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
import sdk.SoapHelpers._
import sdk.StreamHelpers._

case class Config(
  /** Duration of an authorization in minutes. */
  leaseTime: Int = 120,
  /**
   * % of leasTime that an auth is renewed. This gives time for the auth
   *  to be generated because it takes time to generate and receive an auth.
   */
  leaseTimeRenewalFraction: Double = 0.9,
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
  username: String = Option(System.getenv.get("USERNAME")).getOrElse(""),
  password: String = Option(System.getenv.get("PASSWORD")).getOrElse(""),
  /**
   * Timeout to wait for an individual remote request.
   */
  timeout: Int = 30 * 60, // seconds, wow that's long!
  mode: String = "auth",
  filterFilename: String = "filters.txt",
  objects: String = "Entity Relationships Attributes",
  output: String = "metadata.xml",
  discoveryAction: String = "listRegions",
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
  entityCommand: String = "runCommands",
  commandFile: String = "commands.json",
  //connectionPoolIdelTimeoutInSec: Int = 5*60,
  concurrency: Int = 2,
  pconcurrency: Int = 5,
  parallelism: Int = 16,
  httpRetrys: Int = 10,
  pauseBetweenRetriesInSeconds: Int = 30,
  ignoreKeyFiles: Boolean = false,
  keyfileChunkFetchSize: Int = 5000,
  take: Option[Long] = None,
  drop: Option[Long] = None,
  outputFormattedValues: Boolean = false)

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
  recordProcessor: fs2.Pipe[Task, (sdk.Entity, Int), (sdk.Entity, Int)] = fs2.pipe.id,
  /**
   * Convert a record from CRM (an Entity) into an output string suitable
   *  for whatever output format you want.
   */
  toOutputProcessor: fs2.Pipe[Task, sdk.Entity, String] = defaultMakeOutputRow // Traversable[String] => fs2.Pipe[Task, sdk.Entity, String] = defaultMakeOutputRow _)
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
    opt[Int]("auth-lease-time").text(s"Auth lease time in minutes. Default is ${defaultConfig.leaseTime}")
      .action((x, c) => c.copy(leaseTime = x))
    opt[Double]("renewal-quantum").text(s"Renewal fraction of leaseTime. Default is ${defaultConfig.leaseTimeRenewalFraction}")
      .action((x, c) => c.copy(leaseTimeRenewalFraction = x))
    //    opt[Int]("connectionPoolIdelTimeoutInSec").text(s"If you get idle timeouts, make this larger. Default is ${defaultConfig.connectionPoolIdelTimeoutInSec}").
    //      action((x, c) => c.copy(connectionPoolIdelTimeoutInSec = x))

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
      text("Obtain metadata from an organization").
      children(
        urlOpt.required(),
        opt[String]('f', "filter").valueName("<filename>").text(s"Input regexp inclusion filter, one filter per line. No filters means accept everything. Default reads ${defaultConfig.filterFilename} if present.").
          action((x, c) => c.copy(filterFilename = x)),
        opt[String]('o', "objects").valueName("<Entity Relationship Attributes>").text("What metadata to return. Space separated list. Use quotes in shell. All metadata is returned by default.").
          action((x, c) => c.copy(objects = x)),
        opt[String]("output").valueName("<filename>").text("Output file for metadata retrieved using -m. The entire SOAP envelope is output.").
          action((x, c) => c.copy(output = x)))
    note("You need a username/password and url to run this command.")
    note("")
    cmd("auth").action((_, c) => c.copy(mode = "auth")).
      text("Check that authentication works. This is the default command.")
    note("You need a username/password and -r URL to run the auth command.")
    note("")

    cmd("discovery").action((_, c) => c.copy(mode = "discovery")).
      text("Discovery and work with endpoints").
      children(
        urlOpt,
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

    note("You only need a username/password and discovery URL for the region to run a discovery command.")
    note("")

    cmd("entity").action((_, c) => c.copy(mode = "entity")).
      text("Create or modify entities using a specialized json file.").
      children(
        urlOpt,
        regionOpt,
        opt[String]("command-file").valueName("<command file name>").optional().text("Run commands in he form of a json file with specialized syntax.")
          .action((x, c) => c.copy(entityCommand = "runCommands", commandFile = x)))
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
        opt[Unit]("output-formatted-values").text("Output formatted values in additition to the attribute values.")
          .action((x, c) => c.copy(outputFormattedValues = true)),

        //        opt[Int]("keyfile-chunksize").text("Number of keys per key file.").
        //          action((x, c) => c.copy(keyfileChunkSize = x)),
        opt[String]("create-partition").text("Create a set of persistent primary key partitions for entity.").
          action((x, c) => c.copy(keyfileEntity = x, queryType = "partition")),
        //        opt[Int]("part-concurrency").text("Concurrency per partition.").
        //          action((x, c) => c.copy(pconcurrency = x)),
        opt[Int]("keyfile-chunk-fetch-size").text("Fetch size when using a keyfile to dump records.").
          action((x, c) => c.copy(keyfileChunkFetchSize = x)),
        opt[Int]("take").text("Take n rows.").
          action((x, c) => c.copy(take = Option(x))),
        opt[Int]("drop").text("Drop leading n rows.").
          action((x, c) => c.copy(drop = Option(x))),
        opt[Unit]("ignore-keyfiles").text("Ignore keyfiles during a dump, if present for an entity.").
          action((x, c) => c.copy(ignoreKeyFiles = true)),
        //        opt[String]("partition-prefix").text("Prefix for partitions files. Defaults to entity name.").
        //          action((x, c) => c.copy(keyfilePrefix = x)),
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
        opt[String]("attribute-file").text("File of attributes, one per line.").
          action((x, c) => c.copy(dump = c.dump.copy(attributeListFilename = Some(x)))),
        opt[String]("attributes").text("Comma separate list of attributes to dump.").
          action((x, c) => c.copy(dump = c.dump.copy(attributeList = Some(x.split(","))))))
    note("Attribute file content and attributes specified in --attributes are merged.")
    note("All double quotes and backslashes are removed from dumped values. Formatted values are not dumped.")

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
      }
    }

    note("")
    note("The organization service url can be obtained from the developer resources web page within your CRM org or using the discovery command.")
    note("This program only works with MS CRM Online.")
  }

  def main(args: Array[String]): Unit = {
    val config = parser.parse(args, defaultConfig) match {
      case Some(c) => c
      case None => Http.shutdown; return
    }
    import java.time._
    import java.time.temporal._

    val start = Instant.now
    println(s"Program start: ${instantString}")
    try {
      config.mode match {
        case "discovery" => discovery(config)
        case "auth" => auth(config)
        case "metadata" => metadata(config)
        case "create" => create(config)
        case "query" => query(config)
        case "entity" => entity(config)
        case "test" => test(config)
      }
    } catch {
      case scala.util.control.NonFatal(e) =>
        logger.error(e)("Error occurred at the top level.")
        println("An unexpected and non-recoverable error during processing. Processing has stopped.")
        println("Error: " + e.getMessage)
    } finally {
      Http.shutdown
    }
    val stop = Instant.now
    println(s"Program stop: ${instantString}")
    val elapsed = Duration.between(start, stop)
    println(s"Program runtime in minutes: ${elapsed.toMinutes}")
    println(s"Program runtime in seconds: ${elapsed.toMillis / 1000}")
  }

  def entity(config: Config): Unit = {
    config.entityCommand match {
      case "runCommands" =>
      // Read command file and parse into json values

      // Find a create command

      case _ =>
        println("Unknown entity command.")
    }
  }

  /**
   * Run some basic test.s
   */
  def test(config: Config): Unit = {

    import Defaults._

    println("Get discovery auth and URL")
    val f1 = discoveryAuth(Http, config.username, config.password, config.region).
      andThen { result =>
        result match {
          case Success(xor) => xor match {
            case Xor.Right(result) => println(s"Result: $result")
            case Xor.Left(err) => println(s"Error: $err")
          }
          case Failure(ex) => println(s"Failed: $ex")
        }
      }
    val discAuth = Await.result(f1, config.timeout seconds).getOrElse(CrmAuthenticationHeader())

    println("Find discovery URL given a region")
    val x = locationsToDiscoveryURL.get(config.region)
    x match {
      case Some(url) => println(s"Discovery URL for region ${config.region} is $url")
      case None => println(s"Unable to find discovery url for region abbrev ${config.region}")
    }
    val discoveryUrl = x.get

    println("Get org services auth and URL")
    val f2 = orgServicesAuth(Http, discAuth, config.username, config.password, config.url, config.region).
      andThen { result =>
        result match {
          case Success(xor) => xor match {
            case Xor.Right(result) => println(s"Result: $result")
            case Xor.Left(err) => println(s"Error: $err")
          }
          case Failure(ex) => println(s"Failed: $ex")
        }
      }
    val orgSvcAuth = Await.result(f2, config.timeout seconds).getOrElse(CrmAuthenticationHeader())

    import responseReaders._

    println("Get endpoints")
    val f3 = requestEndpoints(Http, discAuth).
      andThen { result =>
        result match {
          case Success(xor) => xor match {
            case Xor.Right(endpoints) => endpoints.foreach { ep =>
              println(s"Org: $ep")
            }
            case Xor.Left(err) => println(s"Error: $err")
          }
          case Failure(ex) => println(s"Failed: $ex")
        }
      }
    Await.ready(f3, config.timeout seconds)

    println("Get entity metadata")
    import crm.sdk.metadata._
    import crm.sdk.metadata.readers._
    val f4 = requestEntityMetadata(Http, orgSvcAuth).andThen { result =>
      result match {
        case Success(xor) => xor match {
          case Xor.Right(result) => println(s"# entities: ${result.entities.length}")
          case Xor.Left(err) => println(s"Error: $err")
        }
        case Failure(ex) => println(s"Failed: $ex")
      }
    }
    Await.ready(f4, 10 * config.timeout seconds)

    println("Find org services URL from web app url and region")
    val f5 = orgServicesUrl(Http, discAuth, config.url).andThen { result =>
      result match {
        case Success(xor) => xor match {
          case Xor.Right(result) => println(result)
          case Xor.Left(err) => println(s"Error: $err")
        }
        case Failure(ex) => println(s"Failed $ex")
      }
    }
    Await.ready(f5, config.timeout seconds)
  }

  /**
   *
   * Run query like requests.
   *
   * https://msdn.microsoft.com/en-us/library/hh547457.aspx
   *
   * https://msdn.microsoft.com/en-us/library/gg328300.aspx
   */
  def query(config: Config): Unit = {

    def stdout: Sink[Task, String] = pipe.lift((line: String) => Task.delay(println(line)))

    /** Collect entities out of the envelope. */
    val toEntities: Pipe[Task, Envelope, Seq[Entity]] = pipe.collect {
      _ match {
        case Envelope(_, EntityCollectionResult(_, entities, _, _, _, _)) => entities
      }
    }

    /** Acquire an http client inside an effect (a task). */
    def acquire = Task.delay(client(config.timeout))

    /** Return an effect that shutdown an http client. */
    def release = (client: HttpExecutor) => Task.delay(client.shutdown)

    config.queryType match {

      case "dump" =>
        val dump = config.dump
        val entity = config.dump.entity
        val outputFile = if (config.dump.outputFilename.isEmpty) s"$entity.csv" else config.dump.outputFilename

        import scala.xml._
        import crm.sdk.metadata._
        import crm.sdk.metadata.readers._
        import crm.sdk._
        import responseReaders._
        import CrmXmlWriter._
        import fs2.async._
        import fs2.async.immutable._
        import fs2.util._
        import java.util.concurrent.Executors
        import java.nio.file._

        //val tpool = Executors.newWorkStealingPool(config.parallelism + 1)
        implicit val tpool = DaemonThreads(config.parallelism + 1)
        implicit val ec = scala.concurrent.ExecutionContext.fromExecutor(tpool)
        implicit val strategy = Strategy.fromExecutionContext(ec)
        implicit val reader = retrieveMultipleRequestReader
        implicit val sheduler = Scheduler.fromFixedDaemonPool(2, "auth-getter")

        /**
         * Ensure attributes are present and return a new Entity.
         *  Invalid column names in `ensure` are ignored. By default,
         *  any columns that are not in the `Entity` but are
         *  in `ensure` are set to an empty value. This ensures that
         *  the column is present even though it may not have been retrieved
         *  from the server or the server did not send it.
         *
         *  @param ensure List of attributes to ensure are present in the `Entity`.
         */
        def fillInMissingAttributes(meta: EntityDescription, ensure: Set[String]): Pipe[Task, sdk.Entity, sdk.Entity] = pipe.lift { ent: sdk.Entity =>
          val merged = augment(ent.attributes, ensure, meta)
          ent.copy(attributes = merged)
        }

        /** Convenience function to get the auth. */
        def orgAuthF = {
          logger.info("Renewing org auth.")
          orgServicesAuthF(Http, config.username, config.password, config.url, config.region, config.leaseTime)
        }

        /** Create a stream that emits the single-line header or does not emit anything. */
        def headerStream(cols: Seq[String]): Stream[Task, String] =
          if (!cols.isEmpty) {
            val header = cols.toList.sorted.
              map(org.apache.commons.lang3.StringEscapeUtils.escapeCsv(_)).
              mkString(",") + "\n"
            Stream.emit(header)
          } else Stream.empty

        val authRenewalTimeInMin = (config.leaseTimeRenewalFraction * config.leaseTime).toInt
        val initialState = (Option(PagingInfo(page = 1, returnTotalRecordCount = true, count = 0)), true)

        def query(entity: String, columns: ColumnSet, pi: Option[PagingInfo]) = {
          QueryExpression(entity, columns, pageInfo = pi.getOrElse(EmptyPagingInfo))
        }

        /** For getting a specific list of entities based on guids */
        def fetchInIdList(entity: String, id: String, guids: Traversable[String], cols: Traversable[String]) =
          <fetch returntotalrecordcount='true' mapping='logical'>
            <entity name={ entity }>
              { cols.map { a => <attribute name={ a }/> } }
              <filter type='and'>
                <condition attribute={ id } operator='in'>
                  { guids.map(g => <value>{ g }</value>) }
                </condition>
              </filter>
            </entity>
          </fetch>

        /** Read ids from a file. */
        def readIds(idFilename: String): Stream[Task, String] =
          io.file.readAll[Task](Paths.get(idFilename), 5000)
            .through(text.utf8Decode)
            .through(text.lines)
            .filter(s => !s.trim.isEmpty && !s.startsWith("#"))

        /** Create a stream of output rows given an Envelope input. 
         *  flatMap this into a stream of Envelopes.
         */
        def envelopesToStringRows(entityDesc: EntityDescription, cols: Traversable[String])(e: Envelope) = {
          Stream.emit(e)
            .through(toEntities)
            .flatMap { Stream.emits }
            .drop(config.drop getOrElse 0L)
            .take(config.take getOrElse Long.MaxValue)
            .through(fillInMissingAttributes(entityDesc, cols.toSet))
            .zipWithIndex
            .through(dump.recordProcessor)
            .through(reportEveryN(dump.statusFrequency, (ent: sdk.Entity, index: Int) => {
              Task.delay(println(s"$instantString: ${entityDesc.logicalName}: Processed $index records."))
            }))
            .through(dump.toOutputProcessor)
        }

        /**
         *  Dump an entity. The dump will occur when the Stream is run.
         *  A dump can take a long time to run so the output may be
         *  delayed.
         *
         *  @return stream of output messages.
         */
        def dumpEntity(http: HttpExecutor, orgAuth: CrmAuthenticationHeader,
          entityDesc: EntityDescription, cols: Columns,
          outputFile: String, header: Boolean = true,
          envelopes: Stream[Task, Envelope]): Stream[Task, String] = {

          val entity = entityDesc.logicalName
          logger.info(s"$entity: Columns to dump: $cols")
          val rows = envelopes.flatMap(envelopesToStringRows(entityDesc, cols.names))

          val header: Stream[Task, String] =
            if (dump.header) headerStream(cols.names)
            else Stream.empty

          import java.nio.file.StandardOpenOption
          (header ++ rows).
            through(text.utf8Encode).
            to(fs2.io.file.writeAllAsync[Task](java.nio.file.Paths.get(outputFile),
              List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))).
            map(_ => s"$entity: Completed dumping records.")
        }

        /**
         * Dump entity data using a page by page, sequential walk through.
         *
         *  TODO: Change this to use a feed of ids into the same approach
         *  used in `dumpEntityUsingKeyfile` so that it runs fast by default.
         */
        def dumpEntityUsingPaging(http: HttpExecutor, orgAuth: CrmAuthenticationHeader,
          entityDesc: EntityDescription, cols: Seq[String],
          outputFile: String, header: Boolean = true): Stream[Task, String] = {
          val colSet = Columns(cols)

          val qGenerator = query(entityDesc.logicalName, colSet, _: Option[PagingInfo])

          dumpEntity(http, orgAuth,
            entityDesc, colSet,
            outputFile, header,
            envelopesStream(http, orgAuth, orgAuthF, authRenewalTimeInMin, initialState,
              qGenerator, config.httpRetrys, config.pauseBetweenRetriesInSeconds))
        }

        /** Dump entity data using a parallel process based on a key file. */
        def dumpEntityUsingKeyfile(http: HttpExecutor, orgAuth: CrmAuthenticationHeader,
          entityDesc: EntityDescription, cols: Seq[String],
          outputFile: String, header: Boolean = true, keyfile: String): Stream[Task, String] = {

          println(s"Dump for ${entityDesc.logicalName} using key file $keyfile")
          val colSet = Columns(cols)

          val idStream =
            readIds(keyfile).vectorChunkN(config.keyfileChunkFetchSize).through(Stream.emit)

          def mkQuery(ids: Traversable[String], ps: Option[PagingInfo]) =
            FetchExpression(fetchInIdList(entityDesc.logicalName,
              entityDesc.primaryId, ids, cols), ps.getOrElse(PagingInfo()))

          def toEnvStream(signal: Signal[Task, CrmAuthenticationHeader]) =
            envelopesFromInput[Envelope, Traversable[String], FetchExpression](http, signal,
              mkQuery _,
              config.httpRetrys, config.pauseBetweenRetriesInSeconds) _

          def estreams(signal: Signal[Task, CrmAuthenticationHeader]) =
            idStream.flatMap(_.map(toEnvStream(signal)))

          // Wrap up the streams of streams with a "renewing" auth
          val outer =
            Stream.eval(fs2.async.signalOf[Task, CrmAuthenticationHeader](orgAuth)).flatMap { authTokenSignal =>
              auths(orgAuthF, authRenewalTimeInMin).evalMap(newToken => authTokenSignal.set(newToken))
                .drain
                .mergeHaltBoth(estreams(authTokenSignal))
            }

          val joined = fs2.concurrent.join[Task, Envelope](config.pconcurrency)(outer)

          dumpEntity(http, orgAuth, entityDesc, colSet, outputFile, header, joined)
        }

        def mkKeysFileName(ename: String) = ename + ".keys"

        def output(http: HttpExecutor): Stream[Task, Stream[Task, String]] =
          fs2.Stream.eval(Task.fromFuture(orgAuthF)).flatMap { orgAuth =>
            val schemaTask = Task.fromFuture(entityMetadata(http, orgAuth, config.url))
            Stream.eval(schemaTask).flatMap { xor =>
              xor match {
                case Xor.Right(schema) =>
                  // If only one entity was specified we can use just that
                  // and we need to look for specified attributes. If more than
                  // one entity to dump was specified, we are dumping everything.
                  val specifiedAttributes = dump.attributeList.getOrElse(Seq()) ++
                    (dump.attributeListFilename map { _.toFile.lines.map(_.trim).filterNot(_.isEmpty).toSeq } getOrElse Seq())

                  val possibleFilters: Seq[String] = Seq(dump.entity) ++ config.queryFilters ++
                    (nonFatalCatch withApply (f => Seq()) apply { config.queryCountFilterFilename.toFile.lines.toSeq })

                  val spec = {
                    val _spec = entityAttributeSpec(possibleFilters, schema)
                    _spec
                  }

                  Stream.emits(spec.keySet.toSeq).map { entity =>
                    val cols = spec(entity)

                    val entityDesc = findEntity(entity, schema).
                      getOrElse(throw new RuntimeException(s"Unable to find metadata for entity $entity"))

                    val outputFile = s"$entity.csv"
                    println(s"Output file: $outputFile")

                    val keyFile = mkKeysFileName(entity)
                    val keyFileExists = Files.exists(Paths.get(keyFile))

                    val str =
                      if (keyFileExists && !config.ignoreKeyFiles)
                        dumpEntityUsingKeyfile(http, orgAuth, entityDesc, cols, outputFile, dump.header, keyFile)
                      else
                        dumpEntityUsingPaging(http, orgAuth, entityDesc, cols, outputFile, dump.header)

                    str.onError { t =>
                      logger.error(t)("Error processing")
                      Stream(s"Error processing: ${t.getMessage}")
                    }
                  }

                case Xor.Left(error) =>
                  Stream.emit(Stream.emit(s"Error during start of processing: $error}"))
              }
            }
          }

        val outputs = Stream.bracket(acquire)(http =>
          fs2.concurrent.join[Task, String](config.concurrency) { output(http) }.to(stdout),
          release)

        outputs.run.unsafeAttemptRun match {
          case Left(t) =>
            logger.error(t)("Error occurred and was not caught elsewhere.")
            println(s"Error occured during processing: ${t.getMessage}")
          case _ =>
        }
        tpool.shutdown
        tpool.awaitTermination(120, java.util.concurrent.TimeUnit.SECONDS)

      case "countEntities" =>

        //        def fetch(e: String) = s"""
        //            <fetch version="1.0" distinct='false' mapping='logical' aggregate='true'> 
        //                        <entity name='${e}'> 
        //                           <attribute name='${e}id' alias='count' aggregate='count' /> 
        //                        </entity> 
        //                    </fetch>"""
        //
        //        def fetchExpr() = <query i:type="b:FetchExpression" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
        //        <b:Query>{ fetch(config.queryEntity) }</b:Query>
        //        </ query >
        //                  <c:string>{config.queryEntity}id</c:string>
        //i:nil="true"

        import responseReaders._
        import sdk.metadata.readers._
        import crm.sdk._
        import java.util.concurrent.Executors

        val tpool = Executors.newWorkStealingPool(config.parallelism + 1)
        implicit val ec = scala.concurrent.ExecutionContext.fromExecutor(tpool)
        implicit val strategy = Strategy.fromExecutionContext(ec)
        implicit val reader = retrieveMultipleRequestReader
        implicit val sheduler = Scheduler.fromFixedDaemonPool(2, "auth-getter")

        def query(entity: String, id: String)(pi: Option[PagingInfo]) =
          QueryExpression(entity, ColumnSet(id), pageInfo = pi.getOrElse(EmptyPagingInfo))

        /**
         * Extract record count from an Entity Collection Result. Look for
         *  the total record count or count any entitis found (backup).
         */
        def getEntityCollectionResultRecordCount: Pipe[Task, Envelope, Int] =
          pipe.collect {
            _ match {
              case Envelope(_, EntityCollectionResult(_, entities, _, _, _, _)) => entities.length
            }
          }

        def orgAuthF = orgServicesAuthF(Http, config.username, config.password, config.url, config.region, config.leaseTime)
        val authRenewalTimeInMin = (config.leaseTimeRenewalFraction * config.leaseTime).toInt
        val initialState = (Option(PagingInfo(page = 1, returnTotalRecordCount = true, count = 0)), true)

        val output = orgAuthF.flatMap { orgAuth =>
          entityMetadata(Http, orgAuth, config.url).map {
            _ match {
              case Xor.Right(schema) =>
                //logger.info(s"schema: $schema")
                println(s"Found ${schema.entities.size} entities to consider.")
                val possibleFilters: Seq[String] = config.queryFilters ++
                  (nonFatalCatch withApply (f => Seq()) apply { config.queryCountFilterFilename.toFile.lines.toSeq })
                println(s"There are ${possibleFilters.size} filters.")
                val spec = entityAttributeSpec(possibleFilters, schema)
                val entitiesToProcess =
                  if (spec.keySet.size == 0 && possibleFilters.size == 0) schema.entities
                  else if (spec.keySet.size == 0 && possibleFilters.size != 0) {
                    println("Filters did not select any entities to count.")
                    Seq()
                  } else schema.entities.filter(ent => spec.keySet.contains(ent.logicalName))

                // Get the list of allowed entities using their schema name.
                val entityNamesToProcess = entitiesToProcess.map(_.logicalName).toList
                println(s"# entities to process: ${entitiesToProcess.size}")
                logger debug {
                  "Entities to process:\n" + entityNamesToProcess.mkString("\n")
                }
                println("Entities to process:\n" + entityNamesToProcess.mkString("\n"))
                println("Entries with -1 count result indicate an error occured during processing for that entity.")
                println("Record counts")

                val keypipe = pipe.id[Task, Envelope]

                Stream.bracket(acquire)(http =>
                  concurrent.join[Task, (String, Int)](config.concurrency) {
                    Stream.emits(entityNamesToProcess).map { ename =>
                      val id = primaryId(ename, schema) getOrElse s"${ename}id"
                      logger.debug(s"Getting counts for entity: $ename with $id")
                      envelopesStream(http, orgAuth, orgAuthF,
                        authRenewalTimeInMin, initialState, query(ename, id), config.httpRetrys, config.pauseBetweenRetriesInSeconds).
                        through(keypipe).
                        through(getEntityCollectionResultRecordCount).
                        through(pipe.sum).
                        through(pipe.lastOr(-1)).
                        map(s => (ename, s)).
                        onError { e =>
                          logger.error(e)(s"Error processing $ename")
                          println(s"Error processing $ename. Try rerunning just this entity.")
                          Stream.emit((ename, -1))
                        }
                    }
                  },
                  release).runLog.unsafeAttemptRun() match {
                    case Right(counts) =>
                      counts.sortBy(t => t._1).map(p => s"${p._1}, ${p._2}").mkString("\n")
                    case Left(ex) =>
                      logger.debug(ex)(s"Error running count queries.")
                      "Error running count queries."
                  }

              case Xor.Left(error) =>
                s"Error during processing: $error}"
            }
          }
        }
        val result = Await.result(output, Duration.Inf)
        tpool.shutdown
        println(result)
        tpool.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)

      case "partition" =>

        val cp = CreatePartitions(config.keyfileEntity, chunkSize = config.keyfileChunkSize,
          outputFilePrefix = config.keyfileEntity)
        println(s"Creating partition for ${cp.entity}")

        import responseReaders._
        import sdk.metadata.readers._
        import crm.sdk._
        import java.util.concurrent.Executors

        //val tpool = Executors.newWorkStealingPool(config.parallelism + 1)
        implicit val tpool = DaemonThreads(config.parallelism + 1)
        implicit val ec = scala.concurrent.ExecutionContext.fromExecutor(tpool)
        implicit val strategy = Strategy.fromExecutionContext(ec)
        implicit val reader = retrieveMultipleRequestReader
        implicit val sheduler = Scheduler.fromFixedDaemonPool(2, "auth-getter")

        def query(entity: String, id: String)(pi: Option[PagingInfo]) =
          QueryExpression(entity, ColumnSet(id), pageInfo = pi.getOrElse(EmptyPagingInfo))

        /**
         * Extract record count from an Entity Collection Result. Look for
         *  the total record count or count any entitis found (backup).
         */
        def getPartitionValue(attr: String): Pipe[Task, Envelope, Seq[String]] =
          pipe.collect {
            _ match {
              case Envelope(_, EntityCollectionResult(_, entities, _, _, _, _)) =>
                entities.map(_.attributes(attr).text)
            }
          }

        def orgAuthF = orgServicesAuthF(Http, config.username, config.password, config.url, config.region, config.leaseTime)
        val authRenewalTimeInMin = (config.leaseTimeRenewalFraction * config.leaseTime).toInt
        val initialState = (Option(PagingInfo(page = 1, returnTotalRecordCount = true, count = 0)), true)
        import java.nio.file._
        val bits = List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

        // For the moment this is overkill, but we want to be able to parallelise this. */
        def output(http: HttpExecutor): Stream[Task, Stream[Task, String]] =
          fs2.Stream.eval(Task.fromFuture(orgAuthF)).flatMap { orgAuth =>
            val schemaTask = Task.fromFuture(entityMetadata(http, orgAuth, config.url))
            Stream.eval(schemaTask).flatMap { xor =>
              xor match {
                case Xor.Right(schema) =>
                  println(s"Found ${schema.entities.size} entities to consider.")
                  val spec = entityAttributeSpec(Seq(cp.entity), schema)
                  val entityToProcess = schema.entities.filter(ent => spec.keySet.contains(ent.logicalName)).headOption

                  println(s"Entity to process: ${cp.entity}")
                  Stream.emits(Seq(cp.entity)).map { ename =>
                    val id = primaryId(ename, schema) getOrElse s"${ename}id"
                    logger.info(s"Partitioning entity: $ename using $id")

                    val str = envelopesStream(http, orgAuth, orgAuthF,
                      authRenewalTimeInMin, initialState, query(ename, id)).
                      through(getPartitionValue(id)).
                      flatMap(Stream.emits).
                      zipWithIndex.
                      through(reportEveryN(100000, (id: String, index: Int) => {
                        Task.delay(println(s"$instantString: ${ename}: Processed $index records."))
                      })).
                      map(_ + "\n").
                      through(text.utf8Encode).
                      to(fs2.io.file.writeAllAsync[Task](Paths.get(cp.outputFilePrefix + ".keys"), bits)).
                      map(_ => s"$ename Partitioning complete.")

                    str.onError { e =>
                      logger.error(e)(s"Error processing $ename")
                      Stream(s"Error processing $ename: ${e.getMessage}")
                    }
                  }
                case Xor.Left(error) =>
                  Stream.emit(Stream.emit(s"Error during start of processing: $error}"))
              }
            }
          }

        val outputs = Stream.bracket(acquire)(http =>
          fs2.concurrent.join[Task, String](config.concurrency) { output(http) }.to(stdout),
          release)

        outputs.run.unsafeAttemptRun match {
          case Left(t) =>
            logger.error(t)("Error occurred and was not caught elswhere.")
            println("Lowlevel error occurred and was not caught elsewhere: ${t.getMessage}")
          case _ =>
        }
        tpool.shutdown
        tpool.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)
    }
  }

  /**
   * Create some entities.
   */
  def create(config: Config): Unit = {

    import Defaults._

    println("Create test.")
    val fut = GetHeaderOnline(config.username, config.password, config.url).flatMap { header =>
      val body1 =
        <s:Body>
          <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services">
            <request i:type="b:ExecuteMultipleRequest" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
              <b:Parameters xmlns:c="http://schemas.datacontract.org/2004/07/System.Collections.Generic">
                <b:KeyValuePairOfstringanyType>
                  <c:key>Requests</c:key>
                  <c:value i:type="d:OrganizationRequestCollection" xmlns:d="http://schemas.microsoft.com/xrm/2012/Contracts">
                    <d:OrganizationRequest i:type="b:CreateRequest">
                      <b:Parameters>
                        <b:KeyValuePairOfstringanyType>
                          <c:key>Target</c:key>
                          <c:value i:type="b:Entity">
                            <b:Attributes>
                              <b:KeyValuePairOfstringanyType>
                                <c:key>name</c:key>
                                <c:value i:type="e:string" xmlns:e="http://www.w3.org/2001/XMLSchema">Example Account 1</c:value>
                              </b:KeyValuePairOfstringanyType>
                            </b:Attributes>
                            <b:EntityState i:nil="true"/>
                            <b:FormattedValues/>
                            <b:Id>00000000-0000-0000-0000-000000000000</b:Id>
                            <b:LogicalName>account</b:LogicalName>
                            <b:RelatedEntities/>
                          </c:value>
                        </b:KeyValuePairOfstringanyType>
                      </b:Parameters>
                      <b:RequestId i:nil="true"/>
                      <b:RequestName>Create</b:RequestName>
                    </d:OrganizationRequest>
                  </c:value>
                </b:KeyValuePairOfstringanyType>
                <b:KeyValuePairOfstringanyType>
                  <c:key>Settings</c:key>
                  <c:value i:type="d:ExecuteMultipleSettings" xmlns:d="http://schemas.microsoft.com/xrm/2012/Contracts">
                    <d:ContinueOnError>false</d:ContinueOnError>
                    <d:ReturnResponses>true</d:ReturnResponses>
                  </c:value>
                </b:KeyValuePairOfstringanyType>
              </b:Parameters>
              <b:RequestId i:nil="true"/>
              <b:RequestName>ExecuteMultiple</b:RequestName>
            </request>
          </Execute>
        </s:Body>

      val body2 =
        <s:Body>
          <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
            <request i:type="a:CreateRequest" xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts">
              <a:Parameters xmlns:b="http://schemas.datacontract.org/2004/07/System.Collections.Generic">
                <a:KeyValuePairOfstringanyType>
                  <b:key>Target</b:key>
                  <b:value i:type="a:Entity">
                    <a:Attributes>
                      <a:KeyValuePairOfstringanyType>
                        <b:key>name</b:key>
                        <b:value i:type="c:string" xmlns:c="http://www.w3.org/2001/XMLSchema">My New Account Code</b:value>
                      </a:KeyValuePairOfstringanyType>
                      <a:KeyValuePairOfstringanyType>
                        <b:key>address1_city</b:key>
                        <b:value i:type="c:string" xmlns:c="http://www.w3.org/2001/XMLSchema">Minneapolis</b:value>
                      </a:KeyValuePairOfstringanyType>
                    </a:Attributes>
                    <a:EntityState i:nil="true"/>
                    <a:FormattedValues/>
                    <a:Id>00000000-0000-0000-0000-000000000000</a:Id>
                    <a:LogicalName>account</a:LogicalName>
                    <a:RelatedEntities/>
                    <a:RowVersion i:nil="true"/>
                  </b:value>
                </a:KeyValuePairOfstringanyType>
              </a:Parameters>
              <a:RequestId i:nil="true"/>
              <a:RequestName>Create</a:RequestName>
            </request>
          </Execute>
        </s:Body>

      def h2part(orgUrl: String) =
        <a:Action s:mustUnderstand="1">http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute</a:Action>
        <SdkClientVersion xmlns="http://schemas.microsoft.com/xrm/2011/Contracts">7.0.0.3030</SdkClientVersion>
        <a:MessageID>urn:uuid:aa5ac821-ca34-4461-ba27-10899cd527eb</a:MessageID>
        <a:ReplyTo>
          <a:Address>http://www.w3.org/2005/08/addressing/anonymous</a:Address>
        </a:ReplyTo>
        <a:To s:mustUnderstand="1">{ orgUrl }</a:To>

      def xml2(orgUrl: String) =
        <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://www.w3.org/2005/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
          <s:Header>
            { h2part(orgUrl) }
            { CreateSOAPSecurityHeader(header.key, header.token1, header.token2) }
          </s:Header>
          { body2 }
        </s:Envelope>

      val headers = Map("SOAPAction" -> "http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute")

      val req = CrmAuth.createPost(config.url) <:< headers << xml2(header.url).toString
      println("req: " + req.toRequest)
      Http(req OKWithBody as.xml.Elem)
    }.map { resp =>
      println("response: " + resp)
      resp
    }.unwrapEx.recover {
      case x: ApiHttpError =>
        println(s"Exception during entity creation. Response code is: ${x.code}")
        println("Message: " + x.getMessage)
        println(s"Response body: ${x.response.getResponseBody}")
        logger.error(x)("Exception during entity creation processing.")
        null
      case scala.util.control.NonFatal(ex) =>
        println("Exception during entity creation (caught via NonFatal)")
        println("Message: " + ex.getMessage)
        logger.error(ex)("Exception during entity creation processing.")
        null
    }

    catchTimeout("create-test") {
      Await.ready(fut, config.timeout seconds)
    }
  }

  import scala.util.control.Exception._

  /**
   * Run discovery commands.
   */
  def discovery(config: Config): Unit = {

    import Defaults._

    println("Discovery mode")
    config.discoveryAction match {

      case "findServicesUrl" =>
        println(s"Find org services url for region abbrev ${config.region} and web app url ${config.url}")
        val output = discoveryAuth(Http, config.username, config.password, config.region).
          flatMap { discoveryAuth =>
            discoveryAuth match {
              case Xor.Right(auth) => orgServicesUrl(Http, auth, config.url)
              case Xor.Left(err) => Future.successful(Xor.left(s"Error: $err"))
            }
          }.andThen {
            case Success(result) => result match {
              case Xor.Right(url) => println(s"Organization services URL: $url")
              case Xor.Left(err) => println(s"Error: $err")
            }
            case Failure(ex) => println(s"Failed: $ex")
          }
        Await.ready(output, config.timeout seconds)

      case "saveDiscoveryWsdl" =>
        println(s"Retrieving discovery service wsdl from ${config.url}")
        val http = client(config) // in case there are redirects
        val qp = Map("wsdl" -> null)
        val req = url(endpoint(config.url)) <<? qp
        val fut = http(req OKWithBody as.xml.Elem).unwrapEx.
          recover {
            case x: ApiHttpError =>
              println(s"Exception during WSDL retrieval. Response code is: ${x.code}")
              if (x.response.getResponseBody.trim.isEmpty) println("Response body is empty")
              else println(s"Response body: ${x.response.getResponseBody}")
              logger.error(x)("Exception during WSDL retrieval processing.")
              "Unable to obtain WSDL"
            case scala.util.control.NonFatal(ex) =>
              println("Exception during WSDL retrieval")
              logger.error(ex)("Exception during WSDL retrieval processing.")
              "Unable to obtain WSDL"
          }.
          andThen { case _ => http.shutdown }.
          andThen {
            case Success(wsdl) =>
              // Side affect is writing to a file.
              println("Obtained WSDL.")
              println(s"WSDL written to file ${config.wsdlFilename}")
              config.wsdlFilename.toFile < wsdl.toString
              logger.debug("WSDL: " + wsdl.toString)
          }
        catchTimeout("wsdl") { Await.ready(fut, config.timeout seconds) }

      case "saveOrgSvcWsdl" =>
        println(s"Retrieving organization service wsdl from ${config.url}")
        // https://msdn.microsoft.com/en-us/library/gg309401.aspx
        val qp = config.sdkVersion.map(v => Map("singleWsdl" -> null, "sdkversion" -> v)) getOrElse Map("wsdl" -> "wsdl0")
        val req = url(endpoint(config.url)) <<? qp
        logger.debug("WSDL request: " + req.toRequest)
        val http = client(config) // in case there are redirects
        val fut = http(req OKWithBody as.xml.Elem).unwrapEx.
          recover {
            case x: ApiHttpError =>
              println(s"Exception during WSDL retrieval. Response code is: ${x.code}")
              if (x.response.getResponseBody.trim.isEmpty) println("Response body is empty")
              else println(s"Response body: ${x.response.getResponseBody}")
              logger.error(x)("Exception during WSDL retrieval processing.")
              "Unable to obtain WSDL"
            case scala.util.control.NonFatal(ex) =>
              println("Exception during WSDL retrieval")
              logger.error(ex)("Exception during WSDL retrieval processing.")
              "Unable to obtain WSDL"
          }.
          andThen { case _ => http.shutdown }.
          andThen {
            case Success(wsdl) =>
              // Side affect is writing to a file.
              println("Obtained WSDL.")
              println(s"WSDL written to file ${config.wsdlFilename}")
              config.wsdlFilename.toFile < wsdl.toString
              logger.debug("WSDL: " + wsdl.toString)
          }
        catchTimeout("wsdl") { Await.ready(fut, config.timeout seconds) }

      case "listEndpoints" =>
        println("List valid enpdoints for a given region and user.")
        println(s"User           : ${config.username}")
        println(s"Password       : (not displayed)")
        println(s"Discovey Region: ${config.region}")

        def output(org: OrganizationDetail) = {
          s"Org     : ${org.friendlyName} (${org.uniqueName}, ${org.guid})\n" +
            s"State   : ${org.state}\n" +
            s"URL Name: ${org.urlName}\n" +
            s"Version : ${org.version}\n" +
            "End points:\n" +
            org.endpoints.map { e =>
              f"\t${e.name}%-32s: ${e.url}\n"
            }.mkString
        }

        import responseReaders._

        // Endpoints come from the discovery URL locaton, not the actual org of course.
        val endpoints = discoveryAuth(Http, config.username, config.password, config.region).
          flatMap { discoveryAuth =>
            discoveryAuth match {
              case Xor.Right(auth) =>
                val orgs = requestEndpoints(Http, auth).map { presult =>
                  val sb = new StringBuilder()
                  presult match {
                    case Xor.Right(orgs) =>
                      sb.append(s"# of endpoints: ${orgs.length}\n")
                      orgs.foreach(org => sb.append(output(org) + "\n"))
                    case Xor.Left(error) =>
                      sb.append(s"Errors parsing results: $error\n")
                  }
                  sb.toString
                }
                orgs
              case Xor.Left(err) => Future.successful(err)
            }
          }
        val printableOutput = Await.result(endpoints, config.timeout seconds)
        println(printableOutput)

      case "listRegions" =>
        println("Known discovery endpoints")
        locationsToDiscoveryURL.foreach {
          case (k, v) => println(f"$k%-10s: $v")
        }
      case _ =>
    }
  }

  import scala.async.Async.{ async => sasync }

  def auth(config: Config): Unit = {
    import Defaults._

    println("Checking authentication by performing a WhoAmI")
    println("Authenticating...")
    val who = scala.async.Async.async {
      val header = await(GetHeaderOnline(config.username, config.password, config.url))
      val guid = await(CrmWhoAmI(header, config))
      println("User guid: " + guid)
      val name = await(CrmGetUserName(header, guid, config))
      println("Name: " + name)
    } recover {
      case scala.util.control.NonFatal(ex) =>
        println("Exception obtaining name")
        println("Message: " + ex.getMessage)
        logger.error(ex)("Exception during processing")
        "unable to obtain name"
    }
    // End of world, wait for the response.
    println("Obtaining WhoAmI...")
    catchTimeout("name") { Await.ready(who, config.timeout seconds) }
  }

  def metadata(config: Config): Unit = {
    import Defaults._

    println("Authenticating...")
    val header = GetHeaderOnline(config.username, config.password, config.url)

    // open filter filename if present
    val filters = nonFatalCatch withApply { _ => Seq() } apply config.filterFilename.toFile.lines
    println("# entity inclusion filters to use: " + filters.size)
    if (filters.size == 0) println("Accept all entities.")

    println("Obtaining metadata...")
    val metadata = sasync {
      val h = await(header)
      val req = createPost(config) << RetrieveAllEntities(h, config.objects).toString
      val m = await(Http(req OKWithBody as.xml.Elem).unwrapEx)
      println("Obtained metadata for entities: ")
      nonFatalCatch withApply { t =>
        println(s"Unable to write metadata output to ${config.output}")
      } apply config.output.toFile.printWriter(true).map(_.write(m.toString))
      (m.child \\ "EntityMetadata").map { em: xml.Node =>
        (em \\ "SchemaName").text
      }.filter(_.length > 0).sorted.foreach(println)
    } recover {
      case scala.util.control.NonFatal(ex) =>
        println("Exception obtaining metadata")
        println("Message: " + ex.getMessage)
        logger.error(ex)("Exception during processing")
    }
    catchTimeout("metadata") { Await.ready(metadata, config.timeout seconds) }
    Thread.sleep(10000)
  }

  /**
   * Create a post request from a config object.
   */
  def createPost(config: Config): Req = CrmAuth.createPost(config.url)

  /**
   * Wrap a raw XML request (should be the full <body> element) with a header and issue the request.
   * @return Request body as XML or exception thrown if the response is not OK.
   */
  def createPost(requestBody: xml.Elem, auth: CrmAuthenticationHeader, config: Config, url: Option[String] = None): Future[xml.Elem] = {
    import Defaults._

    val body = wrap(auth, requestBody)
    val req = CrmAuth.createPost(url getOrElse config.url) << body.toString
    Http(req OK as.xml.Elem)
  }

  /**
   * Issues a WhoAmI message versus just returning a SOAP request body.
   */
  def CrmWhoAmI(auth: CrmAuthenticationHeader, config: Config): Future[String] = {
    import Defaults._

    val xml3 = <s:Body>
                 <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services">
                   <request i:type="c:WhoAmIRequest" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:c="http://schemas.microsoft.com/crm/2011/Contracts">
                     <b:Parameters xmlns:d="http://schemas.datacontract.org/2004/07/System.Collections.Generic"/>
                     <b:RequestId i:nil="true"/>
                     <b:RequestName>WhoAmI</b:RequestName>
                   </request>
                 </Execute>
               </s:Body>

    val body = wrap(auth, xml3).toString
    val req = createPost(config) << body.toString
    Http(req OK as.xml.Elem) map { response =>
      val nodes = (response \\ "KeyValuePairOfstringanyType")
      nodes.collect {
        case <KeyValuePairOfstringanyType><key>UserId</key><value>{ id@_* }</value></KeyValuePairOfstringanyType> => id.text.trim
      }.headOption.get
    }
  }

  /**
   * Get the user name given the user id GUID. Issues the call to the endpoint versus just returning a SOAP request body.
   */
  def CrmGetUserName(authHeader: CrmAuthenticationHeader, id: String, config: Config): Future[String] = {
    import Defaults._

    val xml = <s:Body>
                <Execute xmlns="http://schemas.microsoft.com/xrm/2011/Contracts/Services" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
                  <request i:type="a:RetrieveRequest" xmlns:a="http://schemas.microsoft.com/xrm/2011/Contracts">
                    <a:Parameters xmlns:b="http://schemas.datacontract.org/2004/07/System.Collections.Generic">
                      <a:KeyValuePairOfstringanyType>
                        <b:key>Target</b:key>
                        <b:value i:type="a:EntityReference">
                          <a:Id>{ id }</a:Id>
                          <a:LogicalName>systemuser</a:LogicalName>
                          <a:Name i:nil="true"/>
                        </b:value>
                      </a:KeyValuePairOfstringanyType>
                      <a:KeyValuePairOfstringanyType>
                        <b:key>ColumnSet</b:key>
                        <b:value i:type="a:ColumnSet">
                          <a:AllColumns>false</a:AllColumns>
                          <a:Columns xmlns:c="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
                            <c:string>firstname</c:string>
                            <c:string>lastname</c:string>
                          </a:Columns>
                        </b:value>
                      </a:KeyValuePairOfstringanyType>
                    </a:Parameters>
                    <a:RequestId i:nil="true"/>
                    <a:RequestName>Retrieve</a:RequestName>
                  </request>
                </Execute>
              </s:Body>

    val body = wrap(authHeader, xml).toString
    Http(createPost(config) << body OK as.xml.Elem) map { response =>
      val nodes = (response \\ "KeyValuePairOfstringanyType")
      nodes.collect {
        case <KeyValuePairOfstringanyType><key>firstname</key><value>{ fname }</value></KeyValuePairOfstringanyType> => (1, fname.text)
        case <KeyValuePairOfstringanyType><key>lastname</key><value>{ lname }</value></KeyValuePairOfstringanyType> => (2, lname.text)
      }.sortBy(_._1).map(_._2).mkString(" ")
    }
  }
}
