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
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.mockito.{Mockito, ArgumentCaptor}
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

    val properties=new Properties();
    val mockDataCatalog=mock[DataCatalogWrapper]
    val mockDC=mock[DataCatalog]

    def init(): Unit ={
      properties.load(ClassLoader.getSystemResourceAsStream("dataCatalog.properties"))

      Mockito.doCallRealMethod().when(mockDataCatalog).getDataCatalog(spark, properties.getProperty("host"), properties.getProperty("username"), properties.getProperty("password"))
//      when(mockDataCatalog.getDataCatalog(spark, properties.getProperty("host"), properties.getProperty("username"), properties.getProperty("password"))).thenReturn(mockDC)
    }


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
      init()
      val objectList = mockDataCatalog.getDataCatalog(spark, properties.getProperty("host"), properties.getProperty("username"), properties.getProperty("password")).dmAllObjectsInNamespace("DATAMGT")
      assertEquals(List("datamgt_bdef","datamgt_bdef2"),objectList)

    }

    test("getNamespaces should return a list of namespaces") {
      init()
      val namespaces = mockDataCatalog.getDataCatalog(spark, properties.getProperty("host"), properties.getProperty("username"), properties.getProperty("password")).getNamespaces()
      assertEquals(namespaces.contains("DATAMGT"),true)
    }

    test("getBusinessObjectDefinitions return should data frame containing business object definitions") {
      init()
      val businessObjectDefinitions = mockDataCatalog.getDataCatalog(spark, properties.getProperty("host"), properties.getProperty("username"), properties.getProperty("password")).getBusinessObjectDefinitions("DATAMGT")
      businessObjectDefinitions.show()
      import spark.implicits._
      val expectedDF=List(("DATAMGT","datamgt_bdef"),("DATAMGT","datamgt_bdef2")).toDF("namespace","definitionName")
      assertEquals(expectedDF.except(businessObjectDefinitions).count,0)
    }

    test("getBusinessObjectFormats should return a business object formats in a data frame") {
      init()
      val businessObjectFormatDataFrame = mockDataCatalog.getDataCatalog(spark, properties.getProperty("host"), properties.getProperty("username"), properties.getProperty("password")).getBusinessObjectFormats("DATAMGT", "datamgt_bdef")
      import spark.implicits._
      val expectedDF=List(("DATAMGT","datamgt_bdef","formatUsage","RELATIONAL_TABLE","0")).toDF("namespace","definitionName","formatUsage","formatFileType","formatVersion")
      assertEquals(expectedDF.except(businessObjectFormatDataFrame).count,0)
    }

    test("saveDataframe stores a new object in S3 by registering it in DM"){
      import spark.implicits._
      val df01 = List(
        ("2019-01-10", 1, 93051.234544f: Float, 93051.234544: Double, BigDecimal(3.3), "one", true),
        ("2019-01-10", 4, 93052.234544f: Float, 93052.234544: Double, BigDecimal(6.6), "two", false),
        ("2019-01-10", 7, 93053.234544f: Float, 93053.234544: Double, BigDecimal(9.9), "three", true)
      ).toDF("tdate", "int", "float", "double", "bigdecimal", "string", "boolean")

      val df02 = List(
        ("2019-02-10", 1, 93054.234544f: Float, 93054.234544: Double, BigDecimal(3.3), "one", true),
        ("2019-02-10", 4, 93055.234544f: Float, 93055.234544: Double, BigDecimal(6.6), "two", false),
        ("2019-02-10", 7, 93056.234544f: Float, 93056.234544: Double, BigDecimal(9.9), "three", true)
      ).toDF("tdate", "int", "float", "double", "bigdecimal", "string", "boolean")

      val nameSpace = "DATAMGT"
      val objName = "datamgt_bdef_partition"
      val partitionKey = "tdate"
      val partitionValue01 = "2019-01-10"
      val partitionValue02 = "2019-02-10"

      mockDataCatalog.getDataCatalog(spark, properties.getProperty("host"), properties.getProperty("username"), properties.getProperty("password")).saveDataFrame(df01, nameSpace, objName, partitionKey, partitionValue01)
      mockDataCatalog.getDataCatalog(spark, properties.getProperty("host"), properties.getProperty("username"), properties.getProperty("password")).saveDataFrame(df02, nameSpace, objName, partitionKey, partitionValue02)

    }

    test("getDataAvailabilityRange should return data availability") {
      init()
      val dataAvailabilityDataFrame = mockDataCatalog.getDataCatalog(spark, properties.getProperty("host"), properties.getProperty("username"), properties.getProperty("password")).getDataAvailabilityRange("DATAMGT", "datamgt_bdef_partition", "PRC", "PARQUET", "tdate", "2019-01-01", "2099-12-31", 0)
      dataAvailabilityDataFrame.show()
    }
//
//  test("queryPath should return a business object data XML") {
//    // @TODO: Create a unit test
//    // val dataCatalog = new DataCatalog(spark, "test.com")
//    // val businessObjectDataXML = dataCatalog.queryPath("FOO", "BAR", "PRC", "BZ", "DATE", Array("2018-12-06"), 0, 0)
//  }
//
//  test("callBusinessObjectFormatQuery should return a business object format XML") {
//    // @TODO: Create a unit test
//    // val dataCatalog = new DataCatalog(spark, "test.com")
//    // val businessObjectFormatXML = dataCatalog.callBusinessObjectFormatQuery("FOO", "BAR", "PRC", "BZ", 0)
//  }
//

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

