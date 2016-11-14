package crm
package sdk

import org.scalatest._

class crmreaderspec extends FlatSpec with Matchers {

  import scala.xml._
  import com.lucidchart.open.xtract._
  import com.lucidchart.open.xtract.{ XmlReader, __ }
  import com.lucidchart.open.xtract.XmlReader._
  import play.api.libs.functional.syntax._

  import metadata._
  import readers._

  val picklistAttribute =
    <d:AttributeMetadata i:type="d:PicklistAttributeMetadata">
      <d:MetadataId>56c9667d-3091-4ab1-811b-237cd305ce9b</d:MetadataId>
      <d:HasChanged i:nil="true"/>
      <d:AttributeOf i:nil="true"/>
      <d:AttributeType>Picklist</d:AttributeType>
      <d:CanBeSecuredForCreate>false</d:CanBeSecuredForCreate>
      <d:CanBeSecuredForRead>false</d:CanBeSecuredForRead>
      <d:CanBeSecuredForUpdate>false</d:CanBeSecuredForUpdate>
      <d:CanModifyAdditionalSettings>
        <b:CanBeChanged>false</b:CanBeChanged>
        <b:ManagedPropertyLogicalName>canmodifyadditionalsettings</b:ManagedPropertyLogicalName>
        <b:Value>true</b:Value>
      </d:CanModifyAdditionalSettings>
      <d:ColumnNumber>31</d:ColumnNumber>
      <d:DeprecatedVersion i:nil="true"/>
      <d:Description>
        <b:LocalizedLabels>
          <b:LocalizedLabel>
            <d:MetadataId>5c991770-c718-4b81-8c53-e3f2cd220b6e</d:MetadataId>
            <d:HasChanged i:nil="true"/>
            <b:IsManaged>true</b:IsManaged>
            <b:Label>Specifies the state of the form.</b:Label>
            <b:LanguageCode>1033</b:LanguageCode>
          </b:LocalizedLabel>
        </b:LocalizedLabels>
        <b:UserLocalizedLabel>
          <d:MetadataId>5c991770-c718-4b81-8c53-e3f2cd220b6e</d:MetadataId>
          <d:HasChanged i:nil="true"/>
          <b:IsManaged>true</b:IsManaged>
          <b:Label>Specifies the state of the form.</b:Label>
          <b:LanguageCode>1033</b:LanguageCode>
        </b:UserLocalizedLabel>
      </d:Description>
      <d:DisplayName>
        <b:LocalizedLabels>
          <b:LocalizedLabel>
            <d:MetadataId>223b9d85-9625-4a2d-afbd-7537edf81aaa</d:MetadataId>
            <d:HasChanged i:nil="true"/>
            <b:IsManaged>true</b:IsManaged>
            <b:Label>Form State</b:Label>
            <b:LanguageCode>1033</b:LanguageCode>
          </b:LocalizedLabel>
        </b:LocalizedLabels>
        <b:UserLocalizedLabel>
          <d:MetadataId>223b9d85-9625-4a2d-afbd-7537edf81aaa</d:MetadataId>
          <d:HasChanged i:nil="true"/>
          <b:IsManaged>true</b:IsManaged>
          <b:Label>Form State</b:Label>
          <b:LanguageCode>1033</b:LanguageCode>
        </b:UserLocalizedLabel>
      </d:DisplayName>
      <d:EntityLogicalName>systemform</d:EntityLogicalName>
      <d:InheritsFrom i:nil="true"/>
      <d:IsAuditEnabled>
        <b:CanBeChanged>true</b:CanBeChanged>
        <b:ManagedPropertyLogicalName>canmodifyauditsettings</b:ManagedPropertyLogicalName>
        <b:Value>true</b:Value>
      </d:IsAuditEnabled>
      <d:IsCustomAttribute>false</d:IsCustomAttribute>
      <d:IsCustomizable>
        <b:CanBeChanged>false</b:CanBeChanged>
        <b:ManagedPropertyLogicalName>iscustomizable</b:ManagedPropertyLogicalName>
        <b:Value>false</b:Value>
      </d:IsCustomizable>
      <d:IsFilterable>false</d:IsFilterable>
      <d:IsGlobalFilterEnabled>
        <b:CanBeChanged>true</b:CanBeChanged>
        <b:ManagedPropertyLogicalName>canmodifyglobalfiltersettings</b:ManagedPropertyLogicalName>
        <b:Value>false</b:Value>
      </d:IsGlobalFilterEnabled>
      <d:IsManaged>true</d:IsManaged>
      <d:IsPrimaryId>false</d:IsPrimaryId>
      <d:IsPrimaryName>false</d:IsPrimaryName>
      <d:IsRenameable>
        <b:CanBeChanged>false</b:CanBeChanged>
        <b:ManagedPropertyLogicalName>isrenameable</b:ManagedPropertyLogicalName>
        <b:Value>false</b:Value>
      </d:IsRenameable>
      <d:IsRetrievable>false</d:IsRetrievable>
      <d:IsSearchable>false</d:IsSearchable>
      <d:IsSecured>false</d:IsSecured>
      <d:IsSortableEnabled>
        <b:CanBeChanged>true</b:CanBeChanged>
        <b:ManagedPropertyLogicalName>canmodifyissortablesettings</b:ManagedPropertyLogicalName>
        <b:Value>false</b:Value>
      </d:IsSortableEnabled>
      <d:IsValidForAdvancedFind>
        <b:CanBeChanged>false</b:CanBeChanged>
        <b:ManagedPropertyLogicalName>canmodifysearchsettings</b:ManagedPropertyLogicalName>
        <b:Value>false</b:Value>
      </d:IsValidForAdvancedFind>
      <d:IsValidForCreate>true</d:IsValidForCreate>
      <d:IsValidForRead>true</d:IsValidForRead>
      <d:IsValidForUpdate>true</d:IsValidForUpdate>
      <d:LinkedAttributeId i:nil="true"/>
      <d:LogicalName>formactivationstate</d:LogicalName>
      <d:RequiredLevel>
        <b:CanBeChanged>false</b:CanBeChanged>
        <b:ManagedPropertyLogicalName>canmodifyrequirementlevelsettings</b:ManagedPropertyLogicalName>
        <b:Value>SystemRequired</b:Value>
      </d:RequiredLevel>
      <d:SchemaName>FormActivationState</d:SchemaName>
      <d:AttributeTypeName xmlns:e="http://schemas.microsoft.com/xrm/2013/Metadata">
        <e:Value>PicklistType</e:Value>
      </d:AttributeTypeName>
      <d:IntroducedVersion>6.0.0.0</d:IntroducedVersion>
      <d:IsLogical>false</d:IsLogical>
      <d:SourceType>0</d:SourceType>
      <d:DefaultFormValue>1</d:DefaultFormValue>
      <d:OptionSet>
        <d:MetadataId>9525475f-0a70-4d40-801d-0ee0454e83d9</d:MetadataId>
        <d:HasChanged i:nil="true"/>
        <d:Description>
          <b:LocalizedLabels>
            <b:LocalizedLabel>
              <d:MetadataId>b6a00a3a-f2c8-4898-8554-b2cb4fdf59ac</d:MetadataId>
              <d:HasChanged i:nil="true"/>
              <b:IsManaged>true</b:IsManaged>
              <b:Label>Indicates the form state that is Active\Inactive.</b:Label>
              <b:LanguageCode>1033</b:LanguageCode>
            </b:LocalizedLabel>
          </b:LocalizedLabels>
          <b:UserLocalizedLabel>
            <d:MetadataId>b6a00a3a-f2c8-4898-8554-b2cb4fdf59ac</d:MetadataId>
            <d:HasChanged i:nil="true"/>
            <b:IsManaged>true</b:IsManaged>
            <b:Label>Indicates the form state that is Active\Inactive.</b:Label>
            <b:LanguageCode>1033</b:LanguageCode>
          </b:UserLocalizedLabel>
        </d:Description>
        <d:DisplayName>
          <b:LocalizedLabels>
            <b:LocalizedLabel>
              <d:MetadataId>eed9985c-588f-4eb0-af87-654b2a421dfb</d:MetadataId>
              <d:HasChanged i:nil="true"/>
              <b:IsManaged>true</b:IsManaged>
              <b:Label>Form State</b:Label>
              <b:LanguageCode>1033</b:LanguageCode>
            </b:LocalizedLabel>
          </b:LocalizedLabels>
          <b:UserLocalizedLabel>
            <d:MetadataId>eed9985c-588f-4eb0-af87-654b2a421dfb</d:MetadataId>
            <d:HasChanged i:nil="true"/>
            <b:IsManaged>true</b:IsManaged>
            <b:Label>Form State</b:Label>
            <b:LanguageCode>1033</b:LanguageCode>
          </b:UserLocalizedLabel>
        </d:DisplayName>
        <d:IsCustomOptionSet>false</d:IsCustomOptionSet>
        <d:IsCustomizable>
          <b:CanBeChanged>false</b:CanBeChanged>
          <b:ManagedPropertyLogicalName>iscustomizable</b:ManagedPropertyLogicalName>
          <b:Value>false</b:Value>
        </d:IsCustomizable>
        <d:IsGlobal>false</d:IsGlobal>
        <d:IsManaged>true</d:IsManaged>
        <d:Name>systemform_formactivationstate</d:Name>
        <d:OptionSetType>Picklist</d:OptionSetType>
        <d:IntroducedVersion i:nil="true"/>
        <d:Options>
          <d:OptionMetadata>
            <d:MetadataId i:nil="true"/>
            <d:HasChanged i:nil="true"/>
            <d:Color i:nil="true"/>
            <d:Description>
              <b:LocalizedLabels/>
              <b:UserLocalizedLabel i:nil="true"/>
            </d:Description>
            <d:IsManaged>true</d:IsManaged>
            <d:Label>
              <b:LocalizedLabels>
                <b:LocalizedLabel>
                  <d:MetadataId>28865f5d-23fd-42a6-b006-af563392c51a</d:MetadataId>
                  <d:HasChanged i:nil="true"/>
                  <b:IsManaged>true</b:IsManaged>
                  <b:Label>Inactive</b:Label>
                  <b:LanguageCode>1033</b:LanguageCode>
                </b:LocalizedLabel>
              </b:LocalizedLabels>
              <b:UserLocalizedLabel>
                <d:MetadataId>28865f5d-23fd-42a6-b006-af563392c51a</d:MetadataId>
                <d:HasChanged i:nil="true"/>
                <b:IsManaged>true</b:IsManaged>
                <b:Label>Inactive</b:Label>
                <b:LanguageCode>1033</b:LanguageCode>
              </b:UserLocalizedLabel>
            </d:Label>
            <d:Value>0</d:Value>
          </d:OptionMetadata>
          <d:OptionMetadata>
            <d:MetadataId i:nil="true"/>
            <d:HasChanged i:nil="true"/>
            <d:Color i:nil="true"/>
            <d:Description>
              <b:LocalizedLabels/>
              <b:UserLocalizedLabel i:nil="true"/>
            </d:Description>
            <d:IsManaged>true</d:IsManaged>
            <d:Label>
              <b:LocalizedLabels>
                <b:LocalizedLabel>
                  <d:MetadataId>76b419a0-cea0-48d9-8298-12a38e1973f0</d:MetadataId>
                  <d:HasChanged i:nil="true"/>
                  <b:IsManaged>true</b:IsManaged>
                  <b:Label>Active</b:Label>
                  <b:LanguageCode>1033</b:LanguageCode>
                </b:LocalizedLabel>
              </b:LocalizedLabels>
              <b:UserLocalizedLabel>
                <d:MetadataId>76b419a0-cea0-48d9-8298-12a38e1973f0</d:MetadataId>
                <d:HasChanged i:nil="true"/>
                <b:IsManaged>true</b:IsManaged>
                <b:Label>Active</b:Label>
                <b:LanguageCode>1033</b:LanguageCode>
              </b:UserLocalizedLabel>
            </d:Label>
            <d:Value>1</d:Value>
          </d:OptionMetadata>
        </d:Options>
      </d:OptionSet>
      <d:FormulaDefinition i:nil="true"/>
      <d:SourceTypeMask>0</d:SourceTypeMask>
    </d:AttributeMetadata>

  "picklistattribute reader" should "read an option attribute" in {

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
        x.attributeType shouldBe ("PickList")
        x.logicalName shouldBe ("Form State")
      }
    }
  }

}
