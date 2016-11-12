package crm
package sdk

import org.scalatest._

class readerspec extends FlatSpec with Matchers {

  import scala.xml._
  import com.lucidchart.open.xtract._
  import com.lucidchart.open.xtract.{ XmlReader, __ }
  import com.lucidchart.open.xtract.XmlReader._
  import play.api.libs.functional.syntax._

  import responseReaders._

  val faultXml =
    <Fault>
      <ErrorCode>1</ErrorCode>
      <Message>Error</Message>
    </Fault>

  val faultInXml =
    <Response>
      <Body>
        <MultipleResponse>
          { faultXml }
        </MultipleResponse>
      </Body>
    </Response>

  val noFaultInXml =
    <Response>
      <Body>
        <MultipleResponse>
          <Data><key>k</key><value>1</value></Data>
        </MultipleResponse>
      </Body>
    </Response>

  val responseFaultXml =
    <Envelope>
      { faultInXml }
    </Envelope>

  val responseDataXml =
    <Envelope>
      { noFaultInXml }
    </Envelope>

  val bodyXml =
    <Envelope>
      <Flag>false</Flag>
      <Response>
        <Body>
          <MultipleResponse>
            <Data><key>k</key><value>1</value></Data>
          </MultipleResponse>
        </Body>
      </Response>
    </Envelope>

  sealed trait ResponseBody
  case class IFault(i: Int, e: String) extends ResponseBody
  case class IData(k: String, v: Int) extends ResponseBody

  case class IBody(flag: Boolean, r: ResponseBody)

  implicit val dataReader = ((__ \ "key").read[String] and (__ \ "value").read[Int])(IData.apply _)
  implicit val faultReader = ((__ \ "ErrorCode").read[Int] and (__ \ "Message").read[String])(IFault.apply _)

  val responsePathReader: XmlReader[NodeSeq] = (__ \ "Response").read
  val bodyPathReader: XmlReader[NodeSeq] = (__ \ "Body").read
  val faultPathReader = (__ \ "Fault").read[NodeSeq]
  val multipleResponsePathReader: XmlReader[NodeSeq] = (__ \ "MultipleResponse").read
  val multipleResponsePathReaderJump: XmlReader[NodeSeq] = (__ \\ "MultipleResponse").read // find anywhere in children

  "responseReaders" should "read a Fault" in {
    val r = faultReader.read(faultXml)
    r.toOption shouldBe Some(IFault(1, "Error"))
  }

  it should "find a fault or None inside a response body using a piece by piece XPath traversal" in {
    val bpath = (__ \\ "Body")
    val rpath = (__ \\ "MultipleResponse")
    val fpath = (__ \\ "Fault")
    val r: XmlReader[NodeSeq] = (bpath ++ rpath ++ fpath).read
    withClue("has fault:") { (r andThen faultReader).read(faultInXml).toOption shouldBe Some(IFault(1, "Error")) }
    withClue("no fault:") { (r andThen faultReader).read(noFaultInXml).toOption shouldBe None }
  }

  it should "read a constant using pure" in {
    val r = (XmlReader.pure(-1) and XmlReader.pure("Not an error"))(IFault.apply _)
    r.read(faultXml).toOption shouldBe Option(IFault(-1, "Not an error"))
  }

  it should "read something with a direct path" in {
    val r = (__ \\ "Data").read[IData]
    r.read(noFaultInXml).toOption shouldBe Option(IData("k", 1))
  }

  it should "find a fault inside a response body using a composition of readers traversal" in {
    val r = (responsePathReader andThen bodyPathReader andThen multipleResponsePathReader andThen faultPathReader andThen faultReader)
    r.read(responseFaultXml).toOption shouldBe Option(IFault(1, "Error"))
  }

  it should "not find a fault inside a response body using a composition of readers traversal that are not correct" in {
    val r = (responsePathReader andThen bodyPathReader andThen /*multipleResponsePathReader andThen*/ faultPathReader andThen faultReader)
    r.read(responseFaultXml).toOption shouldBe None
  }

  it should "follow a path using composition starting with XmlReaders" in {
    // We use map because we are starting with readers that read NodeSeq, see the example below
    // Shows off all combinators or embedding an XPath in the middle as well but as a reader of nodeseq.
    val tmp: XmlReader[ResponseBody] = (responsePathReader andThen bodyPathReader andThen multipleResponsePathReader andThen
      ((faultPathReader andThen faultReader) or ((__ \ "Data").read[NodeSeq] andThen dataReader)))
    val r = tmp.map(IBody(true, _))

    withClue("fault:") { r.read(responseFaultXml).toOption shouldBe Option(IBody(true, IFault(1, "Error"))) }
    withClue("data:") { r.read(responseDataXml).toOption shouldBe Option(IBody(true, IData("k", 1))) }
  }

  it should "follow a path using composition with builders" in {
    // Since the expression starts with a Builder 'and' Builder, we can use apply instead of map like above
    val tmp = ((__ \ "Flag").read[Boolean] and
      (responsePathReader andThen bodyPathReader andThen multipleResponsePathReader andThen
        ((faultPathReader andThen faultReader) or ((__ \ "Data").read[NodeSeq] andThen dataReader))))
    val r = tmp(IBody.apply _)

    withClue("data:") { r.read(bodyXml).toOption shouldBe Option(IBody(false, IData("k", 1))) }
  }

  it should "like the previous test but jump to the location using an XPath then compose" in {
    // Since the expression starts with a Builder 'and' Builder, we can use apply instead of map like above
    val r = ((__ \ "Flag").read[Boolean] and (multipleResponsePathReaderJump andThen
      ((faultPathReader andThen faultReader)
        or
        ((__ \ "Data").read[NodeSeq] andThen dataReader))))(IBody.apply _)

    withClue("data:") { r.read(bodyXml).toOption shouldBe Option(IBody(false, IData("k", 1))) }
  }

  val prefixattr =
    <kvs xmlns:i="mynamespace">
      <el i:type="string">
        theel
      </el>
    </kvs>

  "attribute reader" should "read a prefixed attribute" in {
    val areader = XmlReader.attribute[String]("{mynamespace}type")
    val r = (__ \ "el")(prefixattr)
    val prefixResult = areader.read(r)
    assert(prefixResult.isSuccessful)
    prefixResult.toOption shouldBe Some("string")

    // combine this together for a real read

    val elAndAttrReader = (
      (__ \ "el").read[String].map(_.trim) and
      (__ \ "el").read(areader))((_, _))
    elAndAttrReader.read(prefixattr).toOption shouldBe Some(("theel", "string"))
  }

  val f =
    <FormattedValues>
      <b:KeyValuePairOfstringstring>
        <c:key>creditonhold</c:key>
        <c:value>No</c:value>
      </b:KeyValuePairOfstringstring>
      <b:KeyValuePairOfstringstring>
        <c:key>donotcontact</c:key>
        <c:value>Allow</c:value>
      </b:KeyValuePairOfstringstring>
      <b:KeyValuePairOfstringstring>
        <c:key>bstate</c:key>
        <c:value>1</c:value>
      </b:KeyValuePairOfstringstring>
    </FormattedValues>

  val kvpreader = (
    (__ \ "key").read[String] and
    (__ \ "value").read[String])((_, _))

  "reading kv pairs" should "read a sequency correctly" in {
    val r = (__ \\ "KeyValuePairOfstringstring").read(seq(kvpreader))
    val tmp = r.read(f)
    r.map(v => v should contain inOrderOnly (("creditonhold", "No"), ("donotcontact", "Allow"), ("bstate", "1")))
  }

}
