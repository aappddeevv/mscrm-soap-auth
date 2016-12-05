package crm
package sdk

import org.scalatest._

class crmreaderspec extends FlatSpec with Matchers {

  import scala.xml._
  import com.lucidchart.open.xtract._
  import com.lucidchart.open.xtract.{ XmlReader, __ }
  import com.lucidchart.open.xtract.XmlReader._
  import play.api.libs.functional.syntax._

  import crm.sdk.metadata._
  import crm.sdk.metadata.xmlreaders._
  import crm.sdk.soapreaders._

  def loadXml(f: String) = XML.load(getClass.getResource(f))

  "removeprefix" should "remove prefix" in { 
    val s = soapreaders.removePrefix("x:int")
    s shouldBe ("int")  
  }
  
  it should "not remove a prefix that does not exist" in { 
    val s = soapreaders.removePrefix("int")
    s shouldBe ("int")
  }
  
  
  "picklistattribute reader" should "read an option attribute" in {
    val picklistAttribute = loadXml("am.xml")

    withClue("type and description:") {
      val plreader = (
        (__ \ "AttributeType").read[String] and
        (__ \ "Description" \ "UserLocalizedLabel" \ "Label").read[String])((_, _))
      val result = plreader.read(picklistAttribute)
      result.foreach(r => r._1 == "PickList" && r._2 == "Specifies the state of the form.")
    }

    withClue("option attribute:") {
      optionSetReader.read(picklistAttribute).foreach { x =>
        x.name shouldBe ("systemform_formactivationstate")
        x.displayName shouldBe ("Form State")
        x.description shouldBe ("Specifies the state of the form.")
        x.options should contain inOrderOnly (OptionMetadata("Inactive", "0"), OptionMetadata("Active", "1"))
      }
    }

    withClue("the entire attribute:") {
      pickListAttributeReader.read(picklistAttribute).foreach { x =>
        x.attributeType shouldBe ("Picklist")
        x.logicalName shouldBe ("formactivationstate")
      }
    }
  }

  "schema reader" should "read the schema" in {
    val ed = loadXml("ed.xml")
    val result = entityDescriptionReader.read(ed)
    result.foreach { s =>
      s.logicalName shouldBe ("subscriptionsyncentryoffline")
      s.attributes.size shouldBe (5)
    }
  }

  val valueER =
    <value i:type="EntityReference">
      <Id>x</Id>
      <LogicalName>BigX</LogicalName>
    </value>

  val valueX = <value xmlns:i="http://www.w3.org/2001/XMLSchema-instance" i:type="Int">10</value>

  val valueValue = <value i:type="OptionSetValue"><Value>1</Value></value>

  "attribute value reader" should "read an entityreference value" in {
    entityReferenceValueReader.read(valueER).foreach { v =>
      v.text shouldBe ("x")
      v.logicalName shouldBe ("BigX")
    }
  }

  it should "return a bad parse result if the i:Type value does not match" in {
    val el = <value xmlns:i="i" i:type="BADVALUE"></value>
    val r = nodeReader.filter(WrongTypeError("EntityReference"))(n =>
      XmlReader.attribute[String]("i").read(n).getOrElse("") == "EntityReference")
    val result = r.read(el)
    assert(!result.isSuccessful)
    result shouldBe (ParseFailure(WrongTypeError("EntityReference")))
  }

  it should "read a typed value" in {
    typedValueReader.read(valueX).foreach { v =>
      v.text shouldBe ("10")
      v.t shouldBe ("Int")
    }
  }

  it should "read an OptionSetValue value" in {
    optionSetValueReader.read(valueValue).foreach { v =>
      v.text shouldBe ("1")
    }
  }

  it should "read a value using a composed reader" in {
    valueReader.read(valueER).foreach { v =>
      v.text shouldBe ("x")
    }
  }
  
  
}
