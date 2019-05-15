/*
* Copyright 2015 herd contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.finra.catalog

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.herd.HerdApi
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar

import org.finra.herd.sdk.model._

@RunWith(classOf[JUnitRunner])
class DataCatalogTest extends FunSuite with MockitoSugar {

  private val spark: SparkSession = SparkSession
  .builder()
  .appName("catalog-test")
  .master("local[*]")
  .getOrCreate()

  private val namespace = "testNamespace"
  private val objectName = "testObject"
  private val formatUsage = "testFormatUsage"
  private val formatType = "testFormatType"
  private val partitionKey = "testPartitonKey"
  private val partitonValue = "2019-01-01"
  private val dataVersion = 0
  private val formatVersion = 0


  test("getPassword should return correct password from credStash when component is not null") {
    val dataCatalog = new DataCatalog(spark, "test.com")

    // Create mock credStash wrapper
    val mockCredStash = mock[CredStashWrapper]
    // Inject the mock object
    dataCatalog.credStash = mockCredStash
    val stringCaptor = ArgumentCaptor.forClass(classOf[String])
    val mapCaptor = ArgumentCaptor.forClass(classOf[util.HashMap[String, String]])

    when(mockCredStash.getSecret(stringCaptor.capture, mapCaptor.capture)).thenReturn("testPassword")
    val secret = dataCatalog.getPassword(spark, "testUser", "ags", "dev", "catalog")
    // Verify the secret
    assertEquals("testPassword", secret)

    // Verify prefixed credential name
    assertEquals("ags.catalog.dev.testUser", stringCaptor.getValue)

    // Verify context
    val context : util.HashMap[String, String] = mapCaptor.getValue
    assertEquals(3, context.size)
    assertEquals("ags", context.get("AGS"))
    assertEquals("dev", context.get("SDLC"))
    assertEquals("catalog", context.get("Component"))
  }

  test("getPassword should return correct password from credStash when component is null") {
    val dataCatalog = new DataCatalog(spark, "test.com")

    // Create mock credStash wrapper
    val mockCredStash = mock[CredStashWrapper]
    // Inject the mock object
    dataCatalog.credStash = mockCredStash
    val stringCaptor = ArgumentCaptor.forClass(classOf[String])
    val mapCaptor = ArgumentCaptor.forClass(classOf[util.HashMap[String, String]])

    when(mockCredStash.getSecret(stringCaptor.capture, mapCaptor.capture)).thenReturn("testPassword")
    val secret = dataCatalog.getPassword(spark, "testUser", "ags", "dev", null)
    // Verify the secret
    assertEquals("testPassword", secret)

    // Verify prefixed credential name(component should not appear)
    assertEquals("ags.dev.testUser", stringCaptor.getValue)

    // Verify context
    val context : util.HashMap[String, String] = mapCaptor.getValue
    assertEquals(2, context.size)
    assertEquals("ags", context.get("AGS"))
    assertEquals("dev", context.get("SDLC"))
  }

  test("dmAllObjectsInNamespace should return the business object definition keys in List format") {
    val dataCatalog = new DataCatalog(spark, "test.com")
    val mockHerdApi = mock[HerdApi]
    // Inject the herd api mock
    dataCatalog.herdApi = mockHerdApi

    var businessObjectDefinitionKeys = new BusinessObjectDefinitionKeys
    businessObjectDefinitionKeys.setBusinessObjectDefinitionKeys(new util.ArrayList[BusinessObjectDefinitionKey])

    var businessObjectDefinitionKey = new BusinessObjectDefinitionKey
    businessObjectDefinitionKey.setBusinessObjectDefinitionName("bdef1")
    businessObjectDefinitionKeys.getBusinessObjectDefinitionKeys.add(businessObjectDefinitionKey)

    businessObjectDefinitionKey = new BusinessObjectDefinitionKey
    businessObjectDefinitionKey.setBusinessObjectDefinitionName("bdef2")
    businessObjectDefinitionKeys.getBusinessObjectDefinitionKeys.add(businessObjectDefinitionKey)
    when(mockHerdApi.getBusinessObjectsByNamespace(namespace)).thenReturn(businessObjectDefinitionKeys)

    // Test the method
    val objectList = dataCatalog.dmAllObjectsInNamespace(namespace)
    assertEquals(List("bdef1", "bdef2"), objectList)

  }

  test("getNamespaces should return a list of namespaces") {
    val dataCatalog = new DataCatalog(spark, "test.com")
    val mockHerdApi = mock[HerdApi]
    // Inject the herd api mock
    dataCatalog.herdApi = mockHerdApi

    var businessObjectDefinitionKeys = new BusinessObjectDefinitionKeys
    businessObjectDefinitionKeys.setBusinessObjectDefinitionKeys(new util.ArrayList[BusinessObjectDefinitionKey])

    var businessObjectDefinitionKey = new BusinessObjectDefinitionKey
    businessObjectDefinitionKey.setNamespace("testNamespace1")
    businessObjectDefinitionKeys.addBusinessObjectDefinitionKeysItem(businessObjectDefinitionKey)

    businessObjectDefinitionKey.setNamespace("testNamespace2")
    businessObjectDefinitionKeys.addBusinessObjectDefinitionKeysItem(businessObjectDefinitionKey)

    var namespaceKeys = new NamespaceKeys
    namespaceKeys.setNamespaceKeys(new util.ArrayList[NamespaceKey]())

    var namespaceKey = new NamespaceKey

    namespaceKey.setNamespaceCode("testNamespace1")
    namespaceKeys.addNamespaceKeysItem(namespaceKey)

    var namespaceKey1 = new NamespaceKey
    namespaceKey1.setNamespaceCode("testNamespace2")
    namespaceKeys.addNamespaceKeysItem(namespaceKey1)

    when(mockHerdApi.getAllNamespaces).thenReturn(namespaceKeys)

    val objectList = dataCatalog.getNamespaces()
    assertEquals(List("testNamespace1", "testNamespace2"), objectList)

  }

  test("getBusinessObjectDefinitions return should data frame containing business object definitions") {
    val dataCatalog = new DataCatalog(spark, "test.com")
    val mockHerdApi = mock[HerdApi]
    // Inject the herd api mock
    dataCatalog.herdApi = mockHerdApi

    var businessObjectDefinitionKey1 = new BusinessObjectDefinitionKey
    businessObjectDefinitionKey1.setBusinessObjectDefinitionName("object1")
    businessObjectDefinitionKey1.setNamespace(namespace)

    var businessObjectDefinitionKey2 = new BusinessObjectDefinitionKey
    businessObjectDefinitionKey2.setBusinessObjectDefinitionName("object2")
    businessObjectDefinitionKey2.setNamespace(namespace)

    var businessObjectDefinitionKeys = new BusinessObjectDefinitionKeys
    businessObjectDefinitionKeys.setBusinessObjectDefinitionKeys(new util.ArrayList[BusinessObjectDefinitionKey]())

    businessObjectDefinitionKeys.getBusinessObjectDefinitionKeys.add(businessObjectDefinitionKey1)
    businessObjectDefinitionKeys.getBusinessObjectDefinitionKeys.add(businessObjectDefinitionKey2)

    when(mockHerdApi.getBusinessObjectsByNamespace(namespace)).thenReturn(businessObjectDefinitionKeys)
    val actualDF = dataCatalog.getBusinessObjectDefinitions(namespace)
    import spark.implicits._
    val expectedDF = List((namespace, "object1"), (namespace, "object2")).toDF("namespace", "definitionName")
    assertEquals(expectedDF.except(actualDF).count, 0)
  }

  test("getBusinessObjectFormats should return a business object formats in a data frame") {
    val dataCatalog = new DataCatalog(spark, "test.com")
    val mockHerdApi = mock[HerdApi]
    // Inject the herd api mock
    dataCatalog.herdApi = mockHerdApi

    var businessObjectFormatKeys = new BusinessObjectFormatKeys
    businessObjectFormatKeys.setBusinessObjectFormatKeys(new util.ArrayList[BusinessObjectFormatKey]())

    var businessObjectFormatKey = new BusinessObjectFormatKey
    businessObjectFormatKey.setBusinessObjectDefinitionName(objectName)
    businessObjectFormatKey.setNamespace(namespace)
    businessObjectFormatKey.setBusinessObjectFormatFileType(formatType)
    businessObjectFormatKey.setBusinessObjectFormatUsage(formatUsage)
    businessObjectFormatKey.setBusinessObjectFormatVersion(formatVersion)

    businessObjectFormatKeys.addBusinessObjectFormatKeysItem(businessObjectFormatKey)

    when(mockHerdApi.getBusinessObjectFormats(namespace, objectName, true)).thenReturn(businessObjectFormatKeys)
    val businessObjectFormatDataFrame = dataCatalog.getBusinessObjectFormats(namespace, objectName)

    import spark.implicits._
    val expectedDF = List((namespace, objectName, formatUsage, formatType, "0"))
      .toDF("namespace", "definitionName", "formatUsage", "formatFileType", "formatVersion")
    assertEquals(expectedDF.except(businessObjectFormatDataFrame).count, 0)

  }

  test("getDataAvailabilityRange should return data availability") {
    val dataCatalog = new DataCatalog(spark, "test.com")
    val mockHerdApi = mock[HerdApi]
    // Inject the herd api mock
    dataCatalog.herdApi = mockHerdApi

    val businesObjectDataAvailability = new BusinessObjectDataAvailability
    businesObjectDataAvailability.setNamespace(namespace)
    businesObjectDataAvailability.setBusinessObjectDefinitionName(objectName)
    businesObjectDataAvailability.setBusinessObjectFormatUsage(formatUsage)
    businesObjectDataAvailability.setBusinessObjectFormatFileType(partitionKey)

    val businessObjectDataStatusList = new util.ArrayList[BusinessObjectDataStatus]
    businesObjectDataAvailability.setAvailableStatuses(businessObjectDataStatusList)

    var businessObjectDataStatus1 = new BusinessObjectDataStatus
    businessObjectDataStatus1.setBusinessObjectDataVersion(0)
    businessObjectDataStatus1.setBusinessObjectFormatVersion(0)
    businessObjectDataStatus1.setReason("object1")
    businessObjectDataStatus1.setPartitionValue("2019-01-01")

    var businessObjectDataStatus2 = new BusinessObjectDataStatus
    businessObjectDataStatus2.setBusinessObjectDataVersion(0)
    businessObjectDataStatus2.setBusinessObjectFormatVersion(0)
    businessObjectDataStatus2.setReason("object2")
    businessObjectDataStatus2.partitionValue("2019-02-01")

    businessObjectDataStatusList.add(businessObjectDataStatus1)
    businessObjectDataStatusList.add(businessObjectDataStatus2)

    businesObjectDataAvailability.setAvailableStatuses(businessObjectDataStatusList)

    when(mockHerdApi.getBusinessObjectDataAvailability(namespace, objectName, formatUsage, formatType, partitionKey, "2019-01-01", "2099-12-31"))
      .thenReturn(businesObjectDataAvailability)

    val dataAvailabilityDataFrame = dataCatalog
      .getDataAvailabilityRange(namespace, objectName, formatUsage, formatType, partitionKey, "2019-01-01", "2099-12-31", formatVersion)
    dataAvailabilityDataFrame.show
    import spark.implicits._
    val expectedDF = List((namespace, objectName, formatUsage, partitionKey, "0", "0", "object1", "2019-01-01"),
      (namespace, objectName, formatUsage, partitionKey, "0", "0", "object2", "2019-02-01"))
      .toDF("Namespace", "ObjectName", "Usage", "FileFormat", "FormatVersion", "DataVersion", "Reason", "")

    assertEquals(dataAvailabilityDataFrame.except(expectedDF).count, 0)

  }

  test("getDataAvailability should return data availability") {
    val dataCatalog = new DataCatalog(spark, "test.com")
    val mockHerdApi = mock[HerdApi]
    // Inject the herd api mock
    dataCatalog.herdApi = mockHerdApi

    val businesObjectDataAvailability = new BusinessObjectDataAvailability
    businesObjectDataAvailability.setNamespace(namespace)
    businesObjectDataAvailability.setBusinessObjectDefinitionName(objectName)
    businesObjectDataAvailability.setBusinessObjectFormatUsage(formatUsage)
    businesObjectDataAvailability.setBusinessObjectFormatFileType(partitionKey)

    val businessObjectDataStatusList = new util.ArrayList[BusinessObjectDataStatus]
    businesObjectDataAvailability.setAvailableStatuses(businessObjectDataStatusList)

    var businessObjectDataStatus1 = new BusinessObjectDataStatus
    businessObjectDataStatus1.setBusinessObjectDataVersion(0)
    businessObjectDataStatus1.setBusinessObjectFormatVersion(0)
    businessObjectDataStatus1.setReason("object1")
    businessObjectDataStatus1.setPartitionValue("2019-01-01")

    var businessObjectDataStatus2 = new BusinessObjectDataStatus
    businessObjectDataStatus2.setBusinessObjectDataVersion(0)
    businessObjectDataStatus2.setBusinessObjectFormatVersion(0)
    businessObjectDataStatus2.setReason("object2")
    businessObjectDataStatus2.partitionValue("2019-02-01")

    businessObjectDataStatusList.add(businessObjectDataStatus1)
    businessObjectDataStatusList.add(businessObjectDataStatus2)

    businesObjectDataAvailability.setAvailableStatuses(businessObjectDataStatusList)

    when(mockHerdApi.getBusinessObjectDataAvailability(namespace, objectName, formatUsage, formatType, partitionKey, "2019-01-01", "2099-12-31"))
      .thenReturn(businesObjectDataAvailability)

    val dataAvailabilityDataFrame = dataCatalog
      .getDataAvailabilityRange(namespace, objectName, formatUsage, formatType, partitionKey, "2019-01-01", "2099-12-31", formatVersion)
    dataAvailabilityDataFrame.show
    import spark.implicits._
    val expectedDF = List((namespace, objectName, formatUsage, partitionKey, "0", "0", "object1", "2019-01-01"),
      (namespace, objectName, formatUsage, partitionKey, "0", "0", "object2", "2019-02-01"))
      .toDF("Namespace", "ObjectName", "Usage", "FileFormat", "FormatVersion", "DataVersion", "Reason", "")

    assertEquals(dataAvailabilityDataFrame.except(expectedDF).count, 0)

  }

  test("queryPath should return a business object data XML") {
    val dataCatalog = new DataCatalog(spark, "test.com")
    val mockHerdApi = mock[HerdApi]
    // Inject the herd api mock
    dataCatalog.herdApi = mockHerdApi

    var businessObjectData = new BusinessObjectData
    businessObjectData.setNamespace(namespace)
    businessObjectData.setBusinessObjectDefinitionName(objectName)
    businessObjectData.setBusinessObjectFormatUsage(formatUsage)
    businessObjectData.setBusinessObjectFormatFileType(formatType)
    businessObjectData.setBusinessObjectFormatVersion(formatVersion)
    businessObjectData.setVersion(dataVersion)
    businessObjectData.setPartitionKey(partitionKey)
    businessObjectData.setPartitionValue(partitonValue)

    val partitionValue = Array(partitonValue)

    when(mockHerdApi.getBusinessObjectData(namespace, objectName, formatUsage, formatType, 0, partitionKey, partitionValue(0), partitionValue.drop(1), 0))
      .thenReturn(businessObjectData)

    val businessObjectDataXML = dataCatalog.queryPath(namespace, objectName, formatUsage, formatType, partitionKey, partitionValue, 0, 0)

    val expectedDataXML = "<BusinessObjectData><id/><namespace>" + namespace + "</namespace><businessObjectDefinitionName>" +
      objectName + "</businessObjectDefinitionName><businessObjectFormatUsage>" + formatUsage + "</businessObjectFormatUsage><businessObjectFormatFileType>" +
      formatType + "</businessObjectFormatFileType><businessObjectFormatVersion>" + formatVersion + "</businessObjectFormatVersion><partitionKey>" +
      partitionKey + "</partitionKey><partitionValue>" + partitonValue + "</partitionValue><version>" +
      formatVersion + "</version><latestVersion/><status/><retentionExpirationDate/></BusinessObjectData>"

    assertEquals(expectedDataXML, businessObjectDataXML)
  }

  ignore("callBusinessObjectFormatQuery should return a business object format XML") {

    val dataCatalog = new DataCatalog(spark, "test.com")
    val mockHerdApi = mock[HerdApi]
    // Inject the herd api mock
    dataCatalog.herdApi = mockHerdApi

    var businessObjectFormat = new org.finra.herd.sdk.model.BusinessObjectFormat
    businessObjectFormat.setNamespace(namespace)
    businessObjectFormat.setBusinessObjectDefinitionName(objectName)
    businessObjectFormat.setBusinessObjectFormatUsage(formatUsage)
    businessObjectFormat.setBusinessObjectFormatFileType(formatType)
    businessObjectFormat.setBusinessObjectFormatVersion(formatVersion)

    var s = new Schema
    var schemaColumn = new SchemaColumn
    schemaColumn.setName("name")
    schemaColumn.setType("String")
    schemaColumn.setRequired(true)
    schemaColumn.setDescription("name column")
    schemaColumn.setSize("10")

    s.addColumnsItem(schemaColumn)
    var schemaColumns = new util.ArrayList[SchemaColumn]()
    schemaColumns.add(schemaColumn)

    businessObjectFormat.setSchema(s)

    when(mockHerdApi.getBusinessObjectFormat(namespace, objectName, formatUsage, formatType, formatVersion)).thenReturn(businessObjectFormat)

    val businessObjectFormatXML = dataCatalog.callBusinessObjectFormatQuery(namespace, objectName, formatUsage, formatType, formatVersion)
    val schema = "<schema><columns><column><name>name</name><type>String</type><size>10</size><required>true</required><defaultValue/>" +
      "<description>name column</description></column></columns><nullValue/><delimiter/><collectionItemsDelimiter/><mapKeysDelimiter/><escapeCharacter/>"  +
      "<partitionKeyGroup/></schema>"
    val expectedXML = "<BusinessObjectFormat><id/><namespace>" + namespace + "</namespace><businessObjectDefinitionName>" + objectName +
      "</businessObjectDefinitionName><businessObjectFormatUsage>" + formatUsage +
      "</businessObjectFormatUsage><businessObjectFormatFileType>" + formatType + "</businessObjectFormatFileType><businessObjectFormatVersion>"
     + formatVersion + "</businessObjectFormatVersion><latestVersion/><partitionKey/><description/><documentSchema/><documentSchemaUrl/>" +
       schema + "<recordFlag/><retentionPeriodInDays/><retentionType/><allowNonBackwardsCompatibleChanges/></BusinessObjectFormat>"

    assertEquals(expectedXML, businessObjectFormatXML)

  }

  test("dmSearch should return a list of tuples containing business object definition name and partition value") {

    val dataCatalog = new DataCatalog(spark, "test.com")
    val mockHerdApi = mock[HerdApi]
    // Inject the herd api mock
    dataCatalog.herdApi = mockHerdApi

    var businessObjectDataSearchKey = new BusinessObjectDataSearchKey()
    businessObjectDataSearchKey.setNamespace(namespace)
    businessObjectDataSearchKey.setBusinessObjectDefinitionName(objectName)
    var businessObjectDataSearchFilter = new BusinessObjectDataSearchFilter()
    businessObjectDataSearchFilter.addBusinessObjectDataSearchKeysItem(businessObjectDataSearchKey)
    var businessObjectDataSearchRequest = new BusinessObjectDataSearchRequest()
    businessObjectDataSearchRequest.addBusinessObjectDataSearchFiltersItem(businessObjectDataSearchFilter)

    var businessObjectDataSearchResult = new BusinessObjectDataSearchResult
    var businessObjectData = new BusinessObjectData
    businessObjectData.setNamespace(namespace)
    businessObjectData.setBusinessObjectDefinitionName(objectName)
    businessObjectData.setBusinessObjectFormatUsage(formatUsage)
    businessObjectData.setBusinessObjectFormatFileType(formatType)
    businessObjectData.setBusinessObjectFormatVersion(formatVersion)
    businessObjectData.setVersion(dataVersion)
    businessObjectData.setPartitionKey(partitionKey)
    businessObjectData.setPartitionValue(partitonValue)

    var businessObjectDataElements = new util.ArrayList[BusinessObjectData]()
    businessObjectDataElements.add(businessObjectData)
    businessObjectDataSearchResult.setBusinessObjectDataElements(businessObjectDataElements)

    when(mockHerdApi.searchBusinessObjectData(businessObjectDataSearchRequest)).thenReturn(businessObjectDataSearchResult)
    val dmSearchResults = dataCatalog.dmSearch(namespace, objectName)

    assertEquals(List((objectName, partitonValue)), dmSearchResults)
  }

  ignore("dmWipeNamespace should delete registered format for an object in DM")
  {
    val dataCatalog = new DataCatalog(spark, "test.com")
    val mockHerdApi = mock[HerdApi]
    // Inject the herd api mock
    dataCatalog.herdApi = mockHerdApi

    var businessObjectDefinitionKey1 = new BusinessObjectDefinitionKey
    businessObjectDefinitionKey1.setBusinessObjectDefinitionName(objectName)
    businessObjectDefinitionKey1.setNamespace(namespace)

    var businessObjectDefinitionKeys = new BusinessObjectDefinitionKeys
    businessObjectDefinitionKeys.setBusinessObjectDefinitionKeys(new util.ArrayList[BusinessObjectDefinitionKey]())

    businessObjectDefinitionKeys.getBusinessObjectDefinitionKeys.add(businessObjectDefinitionKey1)

    var businessObjectDataSearchKey = new BusinessObjectDataSearchKey()
    businessObjectDataSearchKey.setNamespace(namespace)
    businessObjectDataSearchKey.setBusinessObjectDefinitionName(objectName)
    var businessObjectDataSearchFilter = new BusinessObjectDataSearchFilter()
    businessObjectDataSearchFilter.addBusinessObjectDataSearchKeysItem(businessObjectDataSearchKey)
    var businessObjectDataSearchRequest = new BusinessObjectDataSearchRequest()
    businessObjectDataSearchRequest.addBusinessObjectDataSearchFiltersItem(businessObjectDataSearchFilter)

    var businessObjectDataSearchResult = new BusinessObjectDataSearchResult
    var businessObjectData = new BusinessObjectData
    businessObjectData.setNamespace(namespace)
    businessObjectData.setBusinessObjectDefinitionName(objectName)
    businessObjectData.setBusinessObjectFormatUsage(formatUsage)
    businessObjectData.setBusinessObjectFormatFileType(formatType)
    businessObjectData.setBusinessObjectFormatVersion(formatVersion)
    businessObjectData.setVersion(dataVersion)
    businessObjectData.setPartitionKey(partitionKey)
    businessObjectData.setPartitionValue(partitonValue)

    var businessObjectDataElements = new util.ArrayList[BusinessObjectData]()
    businessObjectDataElements.add(businessObjectData)
    businessObjectDataSearchResult.setBusinessObjectDataElements(businessObjectDataElements)

    when(mockHerdApi.searchBusinessObjectData(businessObjectDataSearchRequest)).thenReturn(businessObjectDataSearchResult)
    when(mockHerdApi.getBusinessObjectsByNamespace(namespace)).thenReturn(businessObjectDefinitionKeys)
    when(mockHerdApi.removeBusinessObjectFormat(namespace, objectName, formatUsage, formatType, formatVersion))
      .thenThrow(new IllegalStateException("method was called"))
    when(mockHerdApi
      .removeBusinessObjectData(namespace, objectName, formatUsage, formatType, formatVersion, partitionKey, partitonValue, Seq(), dataVersion))
      .thenThrow(new IllegalStateException("method was called"))

    val thrown = intercept[Throwable]{
      dataCatalog.dmWipeNamespace(namespace)
    }

    assert(thrown.getMessage == "method was called")
  }

  test("getStructType should return the Spark SQL Structure type")
  {
      val dataCatalog = new DataCatalog(spark, "test.com")
      assertEquals(org.apache.spark.sql.types.StringType, dataCatalog.getStructType("String", "10"))

  }

  test("registerNewFormat should register a new formatVersion in Herd and returns a new format version")
  {
    val dataCatalog = new DataCatalog(spark, "test.com")
    val mockHerdApi = mock[HerdApi]
    // Inject the herd api mock
    dataCatalog.herdApi = mockHerdApi

    when(mockHerdApi.registerBusinessObjectFormat(namespace, objectName, formatUsage, formatType, partitionKey, None)).thenReturn(1)

    assertEquals(0, dataCatalog.registerNewFormat(namespace, objectName, formatUsage, formatType))
  }

  ignore("unionUnionSchema should union the DataFrame with a unioned schema, missing columns get null values")
  {
    val dataCatalog = new DataCatalog(spark, "test.com")
    import spark.implicits._
    val DF1 = List((namespace, objectName, formatUsage, partitionKey, "0", "0", "object1", "2019-01-01"),
      (namespace, objectName, formatUsage, partitionKey, "0", "0", "object2", "2019-02-01"))
      .toDF("Namespace", "ObjectName", "Usage", "partitionKey", "FormatVersion", "DataVersion", "Reason", "date")
    val DF2 = List((namespace, objectName, formatUsage, "0", "0", "object3", "2019-03-01"),
      (namespace, objectName, formatUsage, "0", "0", "object4", "2019-04-01"))
      .toDF("Namespace", "ObjectName", "Usage", "FormatVersion", "DataVersion", "Reason", "date")

    val outputDF = dataCatalog.unionUnionSchema(DF1, DF2)

    val expectedDF = List(
      (namespace, objectName, formatUsage, partitionKey, "0", "0", "object1", "2019-01-01"),
      (namespace, objectName, formatUsage, partitionKey, "0", "0", "object2", "2019-02-01"),
      (namespace, objectName, formatUsage, null, "0", "0", "object3", "2019-03-01"),
      (namespace, objectName, formatUsage, null, "0", "0", "object4", "2019-04-01")
       ).toDF("Namespace", "ObjectName", "Usage", "partitionKey", "FormatVersion", "DataVersion", "Reason", "date")

    assertEquals(0, expectedDF.except(outputDF).count)
  }

  test("stop spark")
  {
    spark.stop()
  }

}

