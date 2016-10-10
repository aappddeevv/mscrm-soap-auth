package crm

import scala.language._
import scala.util.control.Exception._
import scopt._
import org.w3c.dom._
import dispatch._, Defaults._
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
import better.files._
import java.io.{ File => JFile }
import fs2._
import scala.concurrent.ExecutionContext

case class Config(
  help: Boolean = false,
  url: String = "", // web app url
  servicesUrl: String = "",
  region: String = "NA",
  username: String = "",
  password: String = "",
  timeout: Int = 30 * 60, // seconds
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
  dump: Dump = Dump(),
  entityCommand: String = "runCommands",
  commandFile: String = "commands.json")

case class Dump(entity: String = "",
  outputFilename: String = "",
  header: Boolean = false,
  batchSize: Int = 0)

object program extends sdk.CrmAuth with sdk.SoapHelpers {

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
    opt[String]('u', "userid").optional().valueName("<username>").text("Userid")
      .action((x, c) => c.copy(username = x))
    opt[String]('p', "password").optional().valueName("<password>").text("Password")
      .action((x, c) => c.copy(password = x))
    opt[Int]('t', "timeout").valueName("<number>").text(s"Timeout in seconds for each request. Default is ${defaultConfig.timeout}")
      .action((x, c) => c.copy(timeout = x))

    help("help").text("Show help")
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
      text("Run a query. Entity names should be schema names e.g. Contact").
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
        opt[String]("dump").valueName("<entity name>").text("Dump entity data into file. Default output file is 'entityname'.csv").
          action((x, c) => c.copy(queryType = "dump", dump = c.dump.copy(entity = x))),
        opt[String]("output-filename").valueName("<dump filename>").text("Dump file name").
          action((x, c) => c.copy(dump = c.dump.copy(outputFilename = x))),
        opt[Unit]("header").text("Add a header to the output. Default is no header.").
          action((x, c) => c.copy(dump = c.dump.copy(header = true))),
        opt[Int]("batchsize").valueName("<batch size as int>").text("Number of records to retrieve for each server call.").
          action((x, c) => c.copy(dump = c.dump.copy(batchSize = x))))
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
      }
    }

    note("")
    note("The orgcanization service url can be obtained from the developer resources web page within your CRM org or using the discovery command.")
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
    config.mode match {
      case "discovery" => discovery(config)
      case "auth" => auth(config)
      case "metadata" => metadata(config)
      case "create" => create(config)
      case "query" => query(config)
      case "entity" => entity(config)
      case "test" => test(config)
    }
    Http.shutdown
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
    config.queryType match {

      case "dump" =>
        val dump = config.dump
        val entity = config.dump.entity
        val outputFile = if (config.dump.outputFilename.isEmpty) s"$entity.csv" else config.dump.outputFilename
        println(s"Dump all retrievable entity attributes for entity ${entity} to file ${outputFile}")

        import crm.sdk.metadata._
        import crm.sdk.metadata.readers._
        import crm.sdk._
        import responseReaders._
        val ec = scala.concurrent.ExecutionContext.Implicits.global
        implicit val strategy = Strategy.fromExecutionContext(ec)

        /**
         * Get a page of entity results wrapped in a Task. Passing in pageInfo = None returns None
         * otherwise it returns either an optional tuple of (Envelope, (auth, pageInfoOpt)).
         * Returned PagingInfo is suitable for a subsequent calls to this function.
         *
         * TODO: Probably could convert this to use the state monad.
         */
        def getPage(entity: String, columns: ColumnSet, orgAuth: CrmAuthenticationHeader, pageInfo: Option[PagingInfo]): Task[Option[(Envelope, (CrmAuthenticationHeader, Option[PagingInfo]))]] = Task.fromFuture {
          pageInfo match {
            case Some(pi) =>
              import CrmXmlWriter._
              logger.info(s"Issuing query expression with page info: $pi")
              val q = QueryExpression(entity, columns = columns, pageInfo = pi)
              val xml = CrmXmlWriter.of[QueryExpression].write(q)

              val qxml = retrieveMultiple(orgAuth, xml.asInstanceOf[scala.xml.Elem])
              logger.debug("Query XML: " + qxml.toString)
              val headers = Map("SOAPAction" -> "http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute")
              val req = createPost(orgAuth.url) <:< headers << qxml.toString
              logger.debug("Query entity request: " + req.toRequest)

              Http(req).map { response =>
                logger.debug(s"Response: ${show(response)}")

                val body = response.getResponseBody
                val xmlBody = scala.xml.XML.loadString(body)
                val crmResponse = processXml(response)(retrieveMultipleRequestReader, logger)

                crmResponse match {
                  case Xor.Right(Envelope(_, Fault(n, msg))) =>
                    throw new RuntimeException(s"Server returned an error: $msg ($n)")
                  case Xor.Left(err) =>
                    throw new RuntimeException(toUserMessage(err))
                  case Xor.Right(e@Envelope(_, ec@EntityCollectionResult(name, entities, total, limit, more, cookieOpt))) =>
                    logger.trace(ec.toString)
                    logger.info(s"Iteration: ename=$name, totalRecordCount=$total, limitExceeded=>$limit, moreRecords=$more, cookie=$cookieOpt")
                    val newPagingInfo = cookieOpt.map(c => pi.copy(page = pi.page + 1, cookie = cookieOpt))
                    Some((e, (orgAuth, newPagingInfo)))
                }
              }
            case None =>
              logger.debug("Stopping iteration. No paging information.")
              Future.successful(None)
          }
        }

        /** Create a stream of SOAP envelopes that are the result of a request to the server. */
        def envelopes(orgAuth: CrmAuthenticationHeader, columns: ColumnSet) =
          Stream.unfoldEval((orgAuth, Option(PagingInfo(page = 1, count = config.dump.batchSize)))) { t =>
            getPage(entity, columns, t._1, t._2)
          }

        /** Collect entities out of the envelope. */
        val toEntities: Pipe[Task, Envelope, Seq[Entity]] = pipe.collect {
          _ match {
            case Envelope(_, EntityCollectionResult(_, entities, _, _, _, _)) => entities
          }
        }

        /** Fill in missing attributes and return a new Entity. */
        def fillInMissingAttributes(meta: EntityDescription, adds: Set[String]): Pipe[Task, sdk.Entity, sdk.Entity] = pipe.lift { ent: sdk.Entity =>
          val merged = augment(ent.attributes, adds, meta)
          ent.copy(attributes = merged)
        }

        val makeOutputRow: Pipe[Task, sdk.Entity, String] = pipe.lift { ent =>
          import org.apache.commons.lang3.StringEscapeUtils._
          val keys = ent.attributes.keys.toList.sorted
          keys.map(ent.attributes(_).text).map(escapeCsv(_)).mkString(",") + "\n"
        }

        val output = discoveryAuth(Http, config.username, config.password, config.region).flatMap { discovery =>
          discovery match {
            case Xor.Right(discoveryAuth) =>
              val fut = orgServicesAuth(Http, discoveryAuth, config.username, config.password, config.url, config.region).flatMap { services =>
                services match {
                  case Xor.Right(orgAuth) =>
                    (entityMetadataFromCache(config.url) recoverWith
                      { case _ => requestEntityMetadata(Http, orgAuth, Some(config.url)) }).flatMap {
                        _ match {
                          case Xor.Right(schema) =>

                            val entityDesc = findEntity(entity, schema).
                              getOrElse(throw new RuntimeException(s"Unable to find metadata for entity $entity"))                              
                            entityDesc.retrievableAttributes.foreach(a => logger.info(s"Attr: $a"))
                            
                            val n = 1000 // artificial limit
                            val primaryId = sdk.metadata.primaryId(entity, schema)
                            val cols = entityDesc.retrievableAttributes.take(n).map(_.logicalName)

                            val colSet = Columns(cols)
                            logger.info(s"Columns to retrieve: $colSet")

                            val rows = envelopes(orgAuth, colSet).
                              through(toEntities).
                              flatMap { seq => Stream.chunk(Chunk.seq(seq)) }.
                              through(fillInMissingAttributes(entityDesc, cols.toSet)).
                              through(makeOutputRow)

                            val header: Stream[Task, String] =
                              if (config.dump.header) {
                                val header = cols.toList.sorted.
                                  map(org.apache.commons.lang3.StringEscapeUtils.escapeCsv(_)).
                                  mkString(",") + "\n"
                                Stream.emit(header)
                              } else Stream.empty

                            val result = (header ++ rows).
                              through(text.utf8Encode).
                              to(fs2.io.file.writeAllAsync[Task](java.nio.file.Paths.get(outputFile))).
                              run.unsafeAttemptRun

                            val fut2 = result match {
                              case Right(_) =>
                                Future.successful("")
                              case Left(err) =>
                                Future.successful(err.getMessage)
                            }                              
                            fut2
                            
                          case Xor.Left(error) =>
                            Future.successful(s"Error during processing: $error}")
                        }
                      }
                  case Xor.Left(err) => Future.successful(err)
                }
              }
              fut
            case Xor.Left(err) => Future.successful(err)
          }
        }
        //val result = Await.result(output, 10 * config.timeout seconds)
        val result = Await.result(output, Duration.Inf)
        println(result)

      case "countEntities" =>
        def fetch(e: String) = s"""
            <fetch version="1.0" distinct='false' mapping='logical' aggregate='true'> 
                        <entity name='${e}'> 
                           <attribute name='${e}id' alias='count' aggregate='count' /> 
                        </entity> 
                    </fetch>"""

        def fetchExpr() = <query i:type="b:FetchExpression" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
                            <b:Query>{ fetch(config.queryEntity) }</b:Query>
                          </query>
        //                  <c:string>{config.queryEntity}id</c:string>
        //i:nil="true"

        //        val processQEResults: xml.Elem => (String, Int) = (results: scala.xml.Elem) => {
        //          val pcookie = (results \\ "PagingCookie").headOption.map(_.text)
        //          val moreRecords = (results \\ "MoreRecords").headOption.map(_.text.toBoolean)
        //          val numRecords = (results \\ "TotalRecordCount").headOption.map(_.text.toInt)
        //          val r = (pcookie, moreRecords, numRecords)
        //          println("qe result: " + r)
        //          ("count", numRecords.get)
        //        }

        import responseReaders._

        val processFEResults = (resp: scala.xml.Elem) => {
          val rvalues = (resp \\ "Attributes" \ "KeyValuePairOfstringanyType").map { kv =>
            val key = (kv \ "key").text
            val value = (kv \ "value" \ "Value").text.toInt
            (key, value)
          }
          val result = rvalues.find(_._1 == "count").headOption getOrElse ("count", -1)
          result
        }

        /**
         * Create a query expression from raw XML. *Not* very customizable.
         */
        def queryExpr(ename: String, page: Int = 1, cookie: Option[String] = None, enameId: Option[String] = None) =
          <query i:type="b:QueryExpression" xmlns:b="http://schemas.microsoft.com/xrm/2011/Contracts" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
            <b:ColumnSet>
              <b:AllColumns>false</b:AllColumns>
              <b:Columns xmlns:c="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
                <c:string>{ enameId.getOrElse(ename + "id") }</c:string>
              </b:Columns>
            </b:ColumnSet>
            <b:Criteria>
              <b:Conditions/>
              <b:FilterOperator>And</b:FilterOperator>
              <b:Filters/>
            </b:Criteria>
            <b:Distinct>false</b:Distinct>
            <b:EntityName>{ ename }</b:EntityName>
            <b:LinkEntities/>
            <b:Orders/>
            <b:PageInfo>
              <b:Count>0</b:Count>
              <b:PageNumber>{ page }</b:PageNumber>
              <b:PagingCookie>{ cookie.getOrElse("") }</b:PagingCookie>
              <b:ReturnTotalRecordCount>
                true
              </b:ReturnTotalRecordCount>
            </b:PageInfo>
          </query>

        // Wow, MS forces you to be synchronous to some degree.
        import scala.collection.{ immutable => sci }
        /**
         * Get the next element in a stream which represents a page result for an
         * entity query. The stream terminates when there are no more pages.
         */
        def syncPage(entity: String, auth: CrmAuthenticationHeader, hasMore: Boolean, page: Int, pagingCookie: Option[String],
          runningSum: Int, entityId: Option[String] = None): sci.Stream[Int] = {
          if (hasMore) {
            val q = retrieveMultiple(auth, queryExpr(entity, page, pagingCookie, entityId))
            logger.debug("Query XML: " + q.toString)
            val headers = Map("SOAPAction" -> "http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute")
            val req = createPost(auth.url) <:< headers << q.toString
            logger.debug("Query entity request: " + req.toRequest)

            val fut2 = Http(req OKThenParse (pagingCookieReader, logger)).
              map {
                _ match {
                  case x@Xor.Right((cookie, more, count)) =>
                    logger.debug(s"Iteration: more=$more, count=$count, cookie=$cookie")
                    sci.Stream.cons(count, syncPage(entity, auth, more, page + 1, cookie, runningSum + count, entityId))
                  case Xor.Left(error) =>
                    println(s"Error during processing counts for entity $entity: ${toUserMessage(error)}")
                    error.log(logger)
                    sci.Stream.empty
                  case _ =>
                    println(s"Data in server response was insufficient to continue counts for entity $entity.")
                    sci.Stream.empty
                }
              }
            Await.result(fut2, config.timeout seconds)
          } else sci.Stream.empty
        }

        import scala.util.matching._

        // get all entities then filter them
        val _allowed = config.queryFilters.map(r => new Regex("(?i)" + r)) ++
          (nonFatalCatch withApply { f => Seq() } apply {
            config.queryCountFilterFilename.toFile.lines.filterNot(_.isEmpty).map(pat => new Regex(("(?i)" + pat).trim)).toSeq
          }): Seq[Regex]

        val allowAll = _allowed.size == 0 // && _excludes.size == 0
        if (allowAll) println("Allowing all entities for processing")
        else println(s"Using ${_allowed.size} entity schema name filter patterns for processing.")

        def allowed(ename: String) =
          if (allowAll) true
          else {
            val yes = _allowed.filter(pat => ename match { case pat() => true; case _ => false; }).size > 0
            //val no = _excludes.filter(pat => ename match { case pat() => true; case _ => false }).size > 0
            //if (yes && !no) true
            if (yes) true
            else false
          }

        // Get entity logical names
        import sdk.metadata.readers._

        val output = discoveryAuth(Http, config.username, config.password, config.region).flatMap { discovery =>
          discovery match {
            case Xor.Right(discoveryAuth) =>
              val fut = orgServicesAuth(Http, discoveryAuth, config.username, config.password, config.url, config.region).flatMap { services =>
                services match {
                  case Xor.Right(orgAuth) =>
                    (entityMetadataFromCache(config.url) recoverWith
                      { case _ => requestEntityMetadata(Http, orgAuth, Some(config.url)) }).map {
                        _ match {
                          case Xor.Right(schema) =>
                            println(s"Found ${schema.entities.size} entities to consider.")
                            val entitiesToProcess = schema.entities.filter(ent => allowed(ent.schemaName))
                            // Get the list of allowed entities using their schema name.
                            val schemaEntityNamesToProcess = entitiesToProcess.map(_.logicalName).toList
                            println(s"# entities to process: ${entitiesToProcess.size}")
                            logger debug {
                              "Entities to process:\n" + schemaEntityNamesToProcess.mkString("\n")
                            }
                            println("Entities to process:\n" + schemaEntityNamesToProcess.mkString("\n"))
                            println("Record counts")

                            implicit val strategy = Strategy.fromExecutionContext(ExecutionContext.Implicits.global)
                            concurrent.join[Task, (String, Int)](2) {
                              Stream.emits(schemaEntityNamesToProcess).map { ename =>
                                val id = primaryId(ename, schema) orElse Option(s"${ename}id")
                                logger.debug(s"Getting counts for entity: $ename with $id")
                                Stream.eval(Task.delay {
                                  (ename, syncPage(ename, orgAuth, true, 1, None, 0, id).sum)
                                })
                              }
                            }.runLog.unsafeAttemptRun() match {
                              case Right(counts) =>
                                val sb = new StringBuilder()
                                counts.foreach(p => sb.append(s"${p._1}, ${p._2}\n"))
                                sb.toString
                              case Left(ex) =>
                                logger.debug(ex)(s"Error running count queries.")
                                "Error running count queries."
                            }
                          case Xor.Left(error) =>
                            s"Error during processing: $error}"
                        }
                      }
                  case Xor.Left(err) => Future.successful(err)
                }
              }
              fut
            case Xor.Left(err) => Future.successful(err)
          }
        }
        val result = Await.result(output, Duration.Inf)
        println(result)
    }
  }

  /**
   * Create some entities.
   */
  def create(config: Config): Unit = {
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

      val req = createPost(config.url) <:< headers << xml2(header.url).toString
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
    println("Checking authentication by performing a WhoAmI")
    println("Authenticating...")
    val who = sasync {
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
  def createPost(config: Config): Req = createPost(config.url)

  /**
   * Wrap a raw XML request (should be the full <body> element) with a header and issue the request.
   * @return Request body as XML or exception thrown if the response is not OK.
   */
  def createPost(requestBody: xml.Elem, auth: CrmAuthenticationHeader, config: Config, url: Option[String] = None): Future[xml.Elem] = {
    val body = wrap(auth, requestBody)
    val req = createPost(url getOrElse config.url) << body.toString
    Http(req OK as.xml.Elem)
  }

  /**
   * Issues a WhoAmI message versus just returning a SOAP request body.
   */
  def CrmWhoAmI(auth: CrmAuthenticationHeader, config: Config): Future[String] = {

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
