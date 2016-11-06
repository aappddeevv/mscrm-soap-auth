package crm
package sdk

import org.scalatest._

class streamhelpersspec extends FlatSpec with Matchers {

  "A string cleaner" should "remove all quotes" in {
    val s = """this is a string with "embedded\"" bad chars\""""  
    val t = StreamHelpers.cleanString(s)
    t shouldBe ("this is a string with embedded bad chars")  
  }
  
  it should "remove newlines of all kinds" in {
    val s = "please remove\nme"
    val t = StreamHelpers.cleanString(s)
    t shouldBe ("please remove\\nme")
  }

}
