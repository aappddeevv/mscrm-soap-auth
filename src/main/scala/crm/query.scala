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
import sdk.httphelpers._
import sdk.streamhelpers._
import scala.util.matching._
import sdk.soapnamespaces.implicits._
import sdk.messages._
import sdk.messages.soaprequestwriters._
import sdk.soapreaders._
import sdk.metadata.readers._
import sdk._
import scala.xml._
import sdk.metadata._
import sdk.metadata.readers._
import sdk._
import soapreaders._
import CrmXmlWriter._
import fs2.async._
import fs2.async.immutable._
import fs2.util._
import java.util.concurrent.Executors
import java.nio.file._

object Query {

  import sdk.metadata._
  import sdk._

  private[this] implicit val logger = getLogger

  /** Return an effect that shutdown an http client. */
  def release = (client: HttpExecutor) => Task.delay(client.shutdown)

  /** Collect entities out of the envelope. */
  val toEntities: Pipe[Task, Envelope, Seq[Entity]] = pipe.collect {
    _ match {
      case Envelope(_, EntityCollectionResult(_, entities, _, _, _, _)) => entities
    }
  }

  /** Read ids from a file. */
  def readIds(idFilename: String): Stream[Task, String] =
    io.file.readAll[Task](Paths.get(idFilename), 5000)
      .through(text.utf8Decode)
      .through(text.lines)
      .filter(s => !s.trim.isEmpty && !s.startsWith("#"))

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

  def query(entity: String, id: String)(pi: Option[PagingInfo]) =
    QueryExpression(entity, ColumnSet(id), pageInfo = pi.getOrElse(EmptyPagingInfo))

  /**
   *  Create a QueryExpression.
   */
  def query(entity: String, columns: ColumnSet, pi: Option[PagingInfo]) = {
    QueryExpression(entity, columns, pageInfo = pi.getOrElse(EmptyPagingInfo))
  }

  /**
   * Create a stream of output rows given an Envelope input.
   *  flatMap this into a stream of Envelopes. The first
   *  output row is the column header which is derived from
   *  the keys of the Entity's attributes and formatted values.
   */
  def envelopesToStringRows(entityDesc: EntityDescription, config: Config)(e: Envelope)(implicit a: Async[Task]): Stream[Task, String] = {
    val s = Stream.emit(e)
      .through(toEntities)
      .flatMap { Stream.emits }
      .drop(config.drop.getOrElse(0))
      .take(config.take.getOrElse(Long.MaxValue))
      //.through(fillInMissingAttributes(entityDesc, cols.toSet))
      .zipWithIndex
      .through(config.dump.recordProcessor)
      .through(reportEveryN(config.dump.statusFrequency, (ent: sdk.Entity, index: Int) => {
        Task.delay(println(s"$instantString: ${entityDesc.logicalName}: Processed $index records."))
      }))

    def makeRow(e: sdk.Entity): String = {
      val avalues =
        if (config.outputExplodedValues) e.attributes.flatMap { case (k, v) => expanders.expand(k, v) }
        else e.attributes.mapValues(_.text)

      val fvalues =
        if (config.outputFormattedValues) e.formattedAttributes.map { case (k, v) => (k + config.formattedValuesSuffix, v) }
        else Map.empty[String, String]

      val attrs = avalues ++ fvalues
      val line = attrs.keys.toList.sorted.map { k => cleanString(attrs(k)) }.mkString(",") + "\n"
      line
    }

    def entityToColumnNames(e: Entity): Traversable[String] = {
      val akeys =
        if (config.outputExplodedValues) e.attributes.keys
        else e.attributes.flatMap { case (k, v) => expanders.expand(k, v).keys }

      val fkeys =
        if (config.outputFormattedValues) e.formattedAttributes.keys.map(_ + config.formattedValuesSuffix)
        else Seq()

      val attrs = akeys ++ fkeys
      attrs.toList.sorted
    }

    val headerAndRows = s.through(streamhelpers.deriveFromFirst(e => entityToColumnNames(e).mkString(",") + "\n", makeRow))
    headerAndRows
  }

  /** Convenience function to get the auth. */
  def orgAuthF(config: Config)(implicit ec: ExecutionContext) = {
    logger.info("Renewing org auth.")
    orgServicesAuthF(Http, config.username, config.password, config.url, config.region, config.leaseTime)
  }

  /**
   *  Dump an entity. The dump will occur when the Stream is run.
   *  A dump can take a long time to run so the output may be
   *  delayed. f is flatMapped into envelopes then composed
   *  with an output sink.
   *
   *  @return stream of output messages.
   */
  def dumpEntity(http: HttpExecutor, orgAuth: CrmAuthenticationHeader,
    entityDesc: EntityDescription,
    outputFile: String, header: Boolean = true,
    envelopes: Stream[Task, Envelope],
    f: Envelope => Stream[Task, String])(implicit a: Async[Task]): Stream[Task, String] = {

    val entity = entityDesc.logicalName
    val rows = envelopes.flatMap(f)

    import java.nio.file.StandardOpenOption
    rows.through(text.utf8Encode).
      to(fs2.io.file.writeAllAsync[Task](java.nio.file.Paths.get(outputFile),
        List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))).
      map(_ => s"$entity: Completed dumping records.")
  }

  /** Make a filename for the keys file from a filename prefix. */
  def mkKeysFileName(ename: String) = ename + ".keys"

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

  /**
   * Dump entity data using a page by page, sequential walk through.
   *
   *  TODO: Change this to use a feed of ids into the same approach
   *  used in `dumpEntityUsingKeyfile` so that it runs fast by default.
   */
  def dumpEntityUsingPaging(http: HttpExecutor, orgAuth: CrmAuthenticationHeader,
    entityDesc: EntityDescription, cols: Seq[String],
    outputFile: String, header: Boolean = true,
    initialState: PagingState,
    authRenewalTimeInMin: Int,
    config: Config)(implicit ec: ExecutionContext, strategy: Strategy, reader: XmlReader[Envelope], scheduler: Scheduler): Stream[Task, String] = {
    val colSet = Columns(cols)

    val qGenerator = query(entityDesc.logicalName, colSet, _: Option[PagingInfo])

    dumpEntity(http, orgAuth,
      entityDesc,
      outputFile, header,
      envelopesStream(http, orgAuth, orgAuthF(config), authRenewalTimeInMin, initialState,
        qGenerator, config.httpRetrys, config.pauseBetweenRetriesInSeconds),
      envelopesToStringRows(entityDesc, config))
  }

  /** Dump entity data using a parallel process based on a key file. */
  def dumpEntityUsingKeyfile(http: HttpExecutor, orgAuth: CrmAuthenticationHeader,
    entityDesc: EntityDescription, cols: Seq[String],
    outputFile: String, header: Boolean = true, keyfile: String,
    authRenewalTimeInMin: Int,
    config: Config)(
      implicit ec: ExecutionContext, strategy: Strategy, reader: XmlReader[Envelope], scheduler: Scheduler): Stream[Task, String] = {

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
        auths(orgAuthF(config), authRenewalTimeInMin).evalMap(newToken => authTokenSignal.set(newToken))
          .drain
          .mergeHaltBoth(estreams(authTokenSignal))
      }

    val joined = fs2.concurrent.join[Task, Envelope](config.pconcurrency)(outer)

    dumpEntity(http, orgAuth, entityDesc, outputFile, header, joined,
      envelopesToStringRows(entityDesc, config))
  }

  /**
   *
   * Run query like requests.
   *
   * https://msdn.microsoft.com/en-us/library/hh547457.aspx
   *
   * https://msdn.microsoft.com/en-us/library/gg328300.aspx
   */
  def apply(config: Config): Unit = {

    /** Acquire an http client inside an effect (a task). */
    def acquire = Task.delay(client(config.timeout))

    config.queryType match {

      case "dump" =>
        val dump = config.dump
        val entity = config.dump.entity
        val outputFile = if (config.dump.outputFilename.isEmpty) s"$entity.csv" else config.dump.outputFilename

        //val tpool = Executors.newWorkStealingPool(config.parallelism + 1)
        implicit val tpool = DaemonThreads(config.parallelism + 1)
        implicit val ec = scala.concurrent.ExecutionContext.fromExecutor(tpool)
        implicit val strategy = Strategy.fromExecutionContext(ec)
        implicit val reader = retrieveMultipleRequestReader
        implicit val sheduler = Scheduler.fromFixedDaemonPool(2, "auth-getter")

        //        /** Create a stream that emits the single-line header or does not emit anything. */
        //        def headerStream(cols: Seq[String]): Stream[Task, String] =
        //          if (!cols.isEmpty) {
        //            val header = cols.toList.sorted.
        //              map(org.apache.commons.lang3.StringEscapeUtils.escapeCsv(_)).
        //              mkString(",") + "\n"
        //            Stream.emit(header)
        //          } else Stream.empty

        val authRenewalTimeInMin = (config.leaseTimeRenewalFraction * config.leaseTime).toInt
        val initialState = (Option(PagingInfo(page = 1, returnTotalRecordCount = true, count = 0)), true)

        // read attributes file...
        // ...        

        def output(http: HttpExecutor): Stream[Task, Stream[Task, String]] =
          fs2.Stream.eval(Task.fromFuture(orgAuthF(config))).flatMap { orgAuth =>
            val schemaTask = Task.fromFuture(entityMetadata(http, orgAuth, config.url))
            Stream.eval(schemaTask).flatMap { xor =>
              xor match {
                case Right(schema) =>
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
                        dumpEntityUsingKeyfile(http, orgAuth, entityDesc, cols, outputFile, dump.header, keyFile,
                          authRenewalTimeInMin, config)
                      else
                        dumpEntityUsingPaging(http, orgAuth, entityDesc, cols, outputFile, dump.header,
                          initialState, authRenewalTimeInMin, config)

                    str.onError { t =>
                      logger.error(t)("Error processing")
                      Stream(s"Error processing: ${t.getMessage}")
                    }
                  }

                case Left(error) =>
                  Stream.emit(Stream.emit(s"Error during start of processing: $error"))
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

        val tpool = Executors.newWorkStealingPool(config.parallelism + 1)
        implicit val ec = scala.concurrent.ExecutionContext.fromExecutor(tpool)
        implicit val strategy = Strategy.fromExecutionContext(ec)
        implicit val reader = retrieveMultipleRequestReader
        implicit val sheduler = Scheduler.fromFixedDaemonPool(2, "auth-getter")

        def orgAuthF = orgServicesAuthF(Http, config.username, config.password, config.url, config.region, config.leaseTime)
        val authRenewalTimeInMin = (config.leaseTimeRenewalFraction * config.leaseTime).toInt
        val initialState = (Option(PagingInfo(page = 1, returnTotalRecordCount = true, count = 0)), true)

        val output = orgAuthF.flatMap { orgAuth =>
          entityMetadata(Http, orgAuth, config.url).map {
            _ match {
              case Right(schema) =>
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

              case Left(error) =>
                s"Error during processing: $error"
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
        val bits = List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

        // For the moment this is overkill, but we want to be able to parallelise this. */
        def output(http: HttpExecutor): Stream[Task, Stream[Task, String]] =
          fs2.Stream.eval(Task.fromFuture(orgAuthF)).flatMap { orgAuth =>
            val schemaTask = Task.fromFuture(entityMetadata(http, orgAuth, config.url))
            Stream.eval(schemaTask).flatMap { xor =>
              xor match {
                case Right(schema) =>
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
                case Left(error) =>
                  Stream.emit(Stream.emit(s"Error during start of processing: $error"))
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

      case "createAttributeFile" =>
        println(s"Creating attribute file attributes.csv. Edit with a spreadsheet then use for downloading.")

        val tpool = Executors.newWorkStealingPool(config.parallelism + 1)
        implicit val ec = scala.concurrent.ExecutionContext.fromExecutor(tpool)

        def orgAuthF = orgServicesAuthF(Http, config.username, config.password, config.url, config.region, config.leaseTime)

        val output = orgAuthF.flatMap { orgAuth =>
          entityMetadata(Http, orgAuth, config.url).map {
            _ match {
              case Right(schema) =>
                logger.debug(s"Using schema: $schema")
                println(s"Found ${schema.entities.size} entities to consider.")
                val f = "attributes.csv".toFile
                f < "entity, attribute, download\n"
                val sb = new StringBuilder
                var counter: Int = 0
                schema.entities.foreach { e =>
                  e.retrievableAttributes.foreach { a =>
                    val line = s"${e.logicalName},${a.logicalName},y\n"
                    sb.append(line)
                    counter += 1
                  }
                }
                f << sb.toString
                s"Output ${counter} lines to attributes.csv"
              case Left(error) => s"Error during processing: $error"

            }
          }
        }
        val result = Await.result(output, Duration.Inf)
        tpool.shutdown
        println(result)
        tpool.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)
    }
  }
}
