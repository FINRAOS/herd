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
import org.finra.herd.sdk.model._
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar


@RunWith(classOf[JUnitRunner])
class DataCatalogTest extends FunSuite with MockitoSugar {

      val spark: SparkSession = SparkSession
      .builder()
      .appName("catalog-test")
      .master("local[*]")
      .getOrCreate()


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

      test("Mock: dmAllObjectsInNamespace should return the business object definition keys in List format") {
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
        when(mockHerdApi.getBusinessObjectsByNamespace("testNamespace")).thenReturn(businessObjectDefinitionKeys)

        // Test the method
        val objectList = dataCatalog.dmAllObjectsInNamespace("testNamespace")
        assertEquals(List("bdef1","bdef2"),objectList)

      }

  test("Mock: getNamespaces should return a list of namespaces") {
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

    var namespaceKeys=new NamespaceKeys
    namespaceKeys.setNamespaceKeys(new util.ArrayList[NamespaceKey]())

    var namespaceKey=new NamespaceKey

    namespaceKey.setNamespaceCode("testNamespace1")
    namespaceKeys.addNamespaceKeysItem(namespaceKey)

    var namespaceKey1=new NamespaceKey
    namespaceKey1.setNamespaceCode("testNamespace2")
    namespaceKeys.addNamespaceKeysItem(namespaceKey1)

    when(mockHerdApi.getAllNamespaces).thenReturn(namespaceKeys)

    val objectList=dataCatalog.getNamespaces()
    assertEquals(List("testNamespace1","testNamespace2"),objectList)

  }

  test("Mock: getBusinessObjectDefinitions return should data frame containing business object definitions") {
    val dataCatalog = new DataCatalog(spark, "test.com")
    val mockHerdApi = mock[HerdApi]
    // Inject the herd api mock
    dataCatalog.herdApi = mockHerdApi

    var businessObjectDefinitionKey1=new BusinessObjectDefinitionKey
    businessObjectDefinitionKey1.setBusinessObjectDefinitionName("object1")
    businessObjectDefinitionKey1.setNamespace("testNamespace")

    var businessObjectDefinitionKey2=new BusinessObjectDefinitionKey
    businessObjectDefinitionKey2.setBusinessObjectDefinitionName("object2")
    businessObjectDefinitionKey2.setNamespace("testNamespace")

    var businessObjectDefinitionKeys=new BusinessObjectDefinitionKeys
    businessObjectDefinitionKeys.setBusinessObjectDefinitionKeys(new util.ArrayList[BusinessObjectDefinitionKey]())

    businessObjectDefinitionKeys.getBusinessObjectDefinitionKeys.add(businessObjectDefinitionKey1)
    businessObjectDefinitionKeys.getBusinessObjectDefinitionKeys.add(businessObjectDefinitionKey2)

    when(mockHerdApi.getBusinessObjectsByNamespace("testNamespace")).thenReturn(businessObjectDefinitionKeys)
    val actualDF=dataCatalog.getBusinessObjectDefinitions("testNamespace")
    import spark.implicits._
    val expectedDF=List(("testNamespace","object1"),("testNamespace","object2")).toDF("namespace","definitionName")
    assertEquals(expectedDF.except(actualDF).count,0)
    }

      test("Mock: getBusinessObjectFormats should return a business object formats in a data frame") {
        val dataCatalog = new DataCatalog(spark, "test.com")
        val mockHerdApi = mock[HerdApi]
        // Inject the herd api mock
        dataCatalog.herdApi = mockHerdApi

        var businessObjectFormatKeys=new BusinessObjectFormatKeys
        businessObjectFormatKeys.setBusinessObjectFormatKeys(new util.ArrayList[BusinessObjectFormatKey]())

        var businessObjectFormatKey=new BusinessObjectFormatKey
        businessObjectFormatKey.setBusinessObjectDefinitionName("testObject")
        businessObjectFormatKey.setNamespace("testNamespace")
        businessObjectFormatKey.setBusinessObjectFormatFileType("testFileType")
        businessObjectFormatKey.setBusinessObjectFormatUsage("testUsage")
        businessObjectFormatKey.setBusinessObjectFormatVersion(0)

        businessObjectFormatKeys.addBusinessObjectFormatKeysItem(businessObjectFormatKey)

        when(mockHerdApi.getBusinessObjectFormats("testNamespace","testObject",true)).thenReturn(businessObjectFormatKeys)
        val businessObjectFormatDataFrame=dataCatalog.getBusinessObjectFormats("testNamespace","testObject")

        import spark.implicits._
        val expectedDF=List(("testNamespace","testObject","testUsage","testFileType","0")).toDF("namespace","definitionName","formatUsage","formatFileType","formatVersion")
        assertEquals(expectedDF.except(businessObjectFormatDataFrame).count,0)

      }

    test("Mock: getDataAvailabilityRange should return data availability") {
      val dataCatalog = new DataCatalog(spark, "test.com")
      val mockHerdApi = mock[HerdApi]
      // Inject the herd api mock
      dataCatalog.herdApi = mockHerdApi

      val businesObjectDataAvailability=new BusinessObjectDataAvailability
      businesObjectDataAvailability.setNamespace("testNamespace")
      businesObjectDataAvailability.setBusinessObjectDefinitionName("testObject")
      businesObjectDataAvailability.setBusinessObjectFormatUsage("testUsage")
      businesObjectDataAvailability.setBusinessObjectFormatFileType("testPartitionKey")

      val businessObjectDataStatusList=new util.ArrayList[BusinessObjectDataStatus]
      businesObjectDataAvailability.setAvailableStatuses(businessObjectDataStatusList)

      var businessObjectDataStatus1= new BusinessObjectDataStatus
      businessObjectDataStatus1.setBusinessObjectDataVersion(0)
      businessObjectDataStatus1.setBusinessObjectFormatVersion(0)
      businessObjectDataStatus1.setReason("object1")
      businessObjectDataStatus1.setPartitionValue("2019-01-01")

      var businessObjectDataStatus2= new BusinessObjectDataStatus
      businessObjectDataStatus2.setBusinessObjectDataVersion(0)
      businessObjectDataStatus2.setBusinessObjectFormatVersion(0)
      businessObjectDataStatus2.setReason("object2")
      businessObjectDataStatus2.partitionValue("2019-02-01")

//      var businessObjectDataStatus3= new BusinessObjectDataStatus
//      businessObjectDataStatus3.setBusinessObjectDataVersion(0)
//      businessObjectDataStatus3.setBusinessObjectFormatVersion(0)
//      businessObjectDataStatus3.setReason("object3")
//      businessObjectDataStatus3.partitionValue("2018-12-01")

      businessObjectDataStatusList.add(businessObjectDataStatus1)
      businessObjectDataStatusList.add(businessObjectDataStatus2)
//      businessObjectDataStatusList.add(businessObjectDataStatus3)

      businesObjectDataAvailability.setAvailableStatuses(businessObjectDataStatusList)

      when(mockHerdApi.getBusinessObjectDataAvailability("testNamespace","testObject","testUsage","testFileType","testPartitionKey","2019-01-01","2099-12-31")).thenReturn(businesObjectDataAvailability)

      val dataAvailabilityDataFrame = dataCatalog.getDataAvailabilityRange("testNamespace","testObject","testUsage","testFileType","testPartitionKey","2019-01-01","2099-12-31", 0)
      dataAvailabilityDataFrame.show
      import spark.implicits._
      val expectedDF=List(("testNamespace","testObject","testUsage","testPartitionKey","0","0","object1","2019-01-01"),
                          ("testNamespace","testObject","testUsage","testPartitionKey","0","0","object2","2019-02-01")).toDF("Namespace","ObjectName","Usage","FileFormat","FormatVersion","DataVersion","Reason","")

      assertEquals(dataAvailabilityDataFrame.except(expectedDF).count,0)
      spark.stop()
    }

  test("queryPath should return a business object data XML") {

//     val businessObjectDataXML = mockDataCatalog.getDataCatalog(spark, properties.getProperty("host"), properties.getProperty("username"), properties.getProperty("password")).queryPath("DATAMGT", "datamgt_bdef", "formatUsage", "RELATIONAL_TABLE", "partition", Array("2018-12-07"), 0, 0)
//      print(businessObjectDataXML)
  }

  test("callBusinessObjectFormatQuery should return a business object format XML") {

//    val businessObjectFormatXML = mockDataCatalog.getDataCatalog(spark, properties.getProperty("host"), properties.getProperty("username"), properties.getProperty("password")).callBusinessObjectFormatQuery("DATAMGT", "datamgt_bdef", "formatUsage", "RELATIONAL_TABLE", 0)
//    assertEquals(businessObjectFormatXML.contains("BusinessObjectFormat"),true)
  }


//
//  test("dmSearch should return a list of tuples containing business object definition name and partition value") {
//    // @TODO: Create a unit test
//    // val dataCatalog = new DataCatalog(spark, "test.com")
//    // val dmSearchResults = dataCatalog.dmSearch("FOO", "BAR")
//  }
//
//  test("queryPathFromGenerateDdl should return a list of S3 key prefixes") {
//    // @TODO: Create a unit test
//    // val dataCatalog = new DataCatalog(spark, "test.com")
//    // val dmSearchResults = dataCatalog.queryPathFromGenerateDdl("FOO", "BAR", "PRC", "ORC", "DATE", Array("2018-12-06"), 0, 0)
//  }
//

}

