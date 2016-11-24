package crm
package sdk

/** Two way lookup from abbrevs to URI or back again. */
trait NamespaceLookup {
  def fromAbbrev(a: String): Option[String]
  def fromURI(u: String): Option[String]
}

/**
 *  Default namespaces when dealing with CRM SOAP messages. Note that
 *  the use of the URL form for the namespace is a convention and any
 *  URI could be used e.g. a URN `urn:somechars:morechars`.
 */
trait soapnamespaces extends NamespaceLookup {

  val NSEnvelope = "http://www.w3.org/2003/05/soap-envelope" //"http://schemas.xmlsoap.org/soap/envelope/"
  val NSContracts2011 = "http://schemas.microsoft.com/xrm/2011/Contracts"
  val NSServices = "http://schemas.microsoft.com/xrm/2011/Contracts/Services"
  val NSSchemaInstance = "http://www.w3.org/2001/XMLSchema-instance"
  val NSSchema = "http://www.w3.org/2001/XMLSchema"
  val NSCollectionsGeneric = "http://schemas.datacontract.org/2004/07/System.Collections.Generic"
  val NSMetadata = "http://schemas.microsoft.com/xrm/2011/Metadata"
  val NSSerialization = "http://schemas.microsoft.com/2003/10/Serialization"

  val NSSerializationArrays = "http://schemas.microsoft.com/2003/10/Serialization/Arrays"
  val NSXrmContracts = "http://schemas.microsoft.com/crm/2011/Contracts"
  val NSXrmMetadata2011 = "http://schemas.microsoft.com/xrm/2011/Metadata"
  val NSXrmMetadataQuery = "http://schemas.microsoft.com/xrm/2011/Metadata/Query"
  val NSXrmMetadata2013 = "http://schemas.microsoft.com/xrm/2013/Metadata"
  val NSXrmContracts2012 = "http://schemas.microsoft.com/xrm/2012/Contracts"

  /** Namespace lookup using some standard default NS abbrevs. */
  val DefaultNS: collection.Map[String, String] = collection.Map(
    "s" -> NSEnvelope,
    "a" -> NSContracts2011,
    "i" -> NSSchemaInstance,
    "b" -> NSCollectionsGeneric,
    "c" -> NSSchema,
    "e" -> NSSerialization,
    "f" -> NSSerializationArrays,
    "g" -> NSXrmContracts,
    "h" -> NSXrmMetadata2011,
    "j" -> NSXrmMetadataQuery,
    "k" -> NSXrmMetadata2013,
    "l" -> NSXrmContracts2012)

  /* From XrmTSToolkit
     var ns = {
                "s": "http://schemas.xmlsoap.org/soap/envelope/",
                "a": "http://schemas.microsoft.com/xrm/2011/Contracts",
                "i": "http://www.w3.org/2001/XMLSchema-instance",
                "b": "http://schemas.datacontract.org/2004/07/System.Collections.Generic",
                "c": "http://www.w3.org/2001/XMLSchema",
                //"d": "http://schemas.microsoft.com/xrm/2011/Contracts/Services",
                "e": "http://schemas.microsoft.com/2003/10/Serialization/",
                "f": "http://schemas.microsoft.com/2003/10/Serialization/Arrays",
                "g": "http://schemas.microsoft.com/crm/2011/Contracts",
                "h": "http://schemas.microsoft.com/xrm/2011/Metadata",
                "j": "http://schemas.microsoft.com/xrm/2011/Metadata/Query",
                "k": "http://schemas.microsoft.com/xrm/2013/Metadata",
                "l": "http://schemas.microsoft.com/xrm/2012/Contracts"
            };
     */

  /** Namespace URI to default abbreviation. */
  val DefaultNSR = DefaultNS.map { case (k, v) => (v, k) }

  /** Given an abbrev, find the URI. */
  def fromAbbrev(abbrev: String) = DefaultNS.get(abbrev)

  /** Given a URI, return the abbvrevation. */
  def fromURI(ns: String) = DefaultNSR.get(ns)
}

object soapnamespaces extends soapnamespaces { self =>
  object implicits {
    implicit val namespaceLookup = self
  }
}
