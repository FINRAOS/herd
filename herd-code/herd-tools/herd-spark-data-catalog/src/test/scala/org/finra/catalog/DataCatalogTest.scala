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

  test("dmAllObjectsInNamespaceXML should return the business object definition keys in XML format") {
    // @TODO: Create a unit test
    // val dataCatalog = new DataCatalog(spark, "test.com")
    // val businessObjectDefinitions = dataCatalog.dmAllObjectsInNamespace("FOO")
  }

  test("getNamespaces should return a list of namespaces") {
    // @TODO: Create a unit test
    // val dataCatalog = new DataCatalog(spark, "test.com")
    // val namespaces = dataCatalog.getNamespaces()
  }

  test("getBusinessObjectDefinitions return should data frame containing business object definitions") {
    // @TODO: Create a unit test
    // val dataCatalog = new DataCatalog(spark, "test.com")
    // val businessObjectDefinitions = dataCatalog.getBusinessObjectDefinitions("FOO")
  }

  test("queryPath should return a business object data XML") {
    // @TODO: Create a unit test
    // val dataCatalog = new DataCatalog(spark, "test.com")
    // val businessObjectDataXML = dataCatalog.queryPath("FOO", "BAR", "PRC", "BZ", "DATE", Array("2018-12-06"), 0, 0)
  }

  test("callBusinessObjectFormatQuery should return a business object format XML") {
    // @TODO: Create a unit test
    // val dataCatalog = new DataCatalog(spark, "test.com")
    // val businessObjectFormatXML = dataCatalog.callBusinessObjectFormatQuery("FOO", "BAR", "PRC", "BZ", 0)
  }

  test("getBusinessObjectFormats should return a business object formats in a data frame") {
    // @TODO: Create a unit test
    // val dataCatalog = new DataCatalog(spark, "test.com")
    // val businessObjectFormatDataFrame = dataCatalog.getBusinessObjectFormats("FOO", "BAR")
  }

  test("dmSearch should return a list of tuples containing business object definition name and partition value") {
    // @TODO: Create a unit test
    // val dataCatalog = new DataCatalog(spark, "test.com")
    // val dmSearchResults = dataCatalog.dmSearch("FOO", "BAR")
  }

  test("queryPathFromGenerateDdl should return a list of S3 key prefixes") {
    // @TODO: Create a unit test
    // val dataCatalog = new DataCatalog(spark, "test.com")
    // val dmSearchResults = dataCatalog.queryPathFromGenerateDdl("FOO", "BAR", "PRC", "ORC", "DATE", Array("2018-12-06"), 0, 0)
  }

  test("getDataAvailabilityRange should return data availability") {
    // @TODO: Create a unit test
    // val dataCatalog = new DataCatalog(spark, "test.com")
    // val dataAvailabilityDataFrame = dataCatalog.getDataAvailabilityRange("FOO", "BAR", "PRC", "ORC", "DATE", "2018-12-06", "2099-12-31", 0)
  }
}

