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
package org.apache.spark.sql.herd

import java.util

import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyString}
import org.mockito.Mockito.{spy, times, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import org.finra.herd.sdk.api._
import org.finra.herd.sdk.invoker.ApiClient
import org.finra.herd.sdk.model.{BusinessObjectDataAvailabilityRequest, BusinessObjectDataCreateRequest, BusinessObjectDataStorageFilesCreateRequest, BusinessObjectFormat, BusinessObjectFormatCreateRequest, BusinessObjectFormatKeys, _}

@RunWith(classOf[JUnitRunner])
class DefaultHerdApiSuite extends FunSuite with MockitoSugar with BeforeAndAfterEach {
  val DATA_PROVIDER = "testDataProvider"
  val NAMESPACE = "testNamesapce"
  val BUSINESS_OBJECT_DEFINITION = "testBusinessObject"
  val FORMAT_USAGE = "testUsage"
  val FILE_TYPE = "testFileType"
  val FORMAT_VERSION = 0
  val PARTITION_KEY = "testPartitionKey"
  val PARTITION_VALUE = "testPartitionValue"
  val PARTITION_VALUES = Seq(PARTITION_VALUE)
  val DATA_VERSION = 0
  val STORAGE_NAME = "testStorageName"
  val STORAGE_DIRECTORY = "testDirectory"

  var mockApiClient : ApiClient = null
  var defaultHerdApi : DefaultHerdApi = null
  var mockBusinessObjectDefinitionApi : BusinessObjectDefinitionApi = null
  var mockBusinessObjectFormatApi : BusinessObjectFormatApi = null
  var mockBusinessObjectDataApi : BusinessObjectDataApi = null
  var mockBusinessObjectDataStatusApi : BusinessObjectDataStatusApi = null
  var mockBusinessObjectDataStorageFileApi : BusinessObjectDataStorageFileApi = null

  // set up
  override def beforeEach(): Unit = {
    mockApiClient = mock[ApiClient]
    mockBusinessObjectDefinitionApi = mock[BusinessObjectDefinitionApi]
    mockBusinessObjectFormatApi = mock[BusinessObjectFormatApi]
    mockBusinessObjectDataApi = mock[BusinessObjectDataApi]
    mockBusinessObjectDataStatusApi = mock[BusinessObjectDataStatusApi]
    mockBusinessObjectDataStorageFileApi = mock[BusinessObjectDataStorageFileApi]
    defaultHerdApi = spy(new DefaultHerdApi(mockApiClient))
  }

  test("Test HerdApi.retry") {
    val namespaceCaptor = ArgumentCaptor.forClass(classOf[String])
    val businessObjectDefinitionCaptor = ArgumentCaptor.forClass(classOf[String])
    val includeUpdateHistoryCaptor = ArgumentCaptor.forClass((classOf[Boolean]))
    val businessObjectDefinition = new BusinessObjectDefinition
    when(mockBusinessObjectDefinitionApi.businessObjectDefinitionGetBusinessObjectDefinition(namespaceCaptor.capture(),
      businessObjectDefinitionCaptor.capture(), includeUpdateHistoryCaptor.capture())).thenThrow(new RuntimeException("Boom!"))
    when(defaultHerdApi.getBusinessObjectDefinitionApi(mockApiClient)).thenReturn(mockBusinessObjectDefinitionApi)

    val thrown = intercept[RuntimeException] {
      defaultHerdApi.getBusinessObjectByName(NAMESPACE, BUSINESS_OBJECT_DEFINITION)
    }
    assert(thrown.getMessage === "Boom!")
    // verify the method has been tried 4 times
    verify(mockBusinessObjectDefinitionApi, times(4)).businessObjectDefinitionGetBusinessObjectDefinition(anyString(), anyString(), anyBoolean())
    verify(defaultHerdApi).getBusinessObjectDefinitionApi(mockApiClient)

    assertEquals(NAMESPACE, namespaceCaptor.getValue)
    assertEquals(BUSINESS_OBJECT_DEFINITION, businessObjectDefinitionCaptor.getValue)
    assertEquals(false, includeUpdateHistoryCaptor.getValue)
  }

  test("Test HerdApi.getBusinessObjectByName") {
    val namespaceCaptor = ArgumentCaptor.forClass(classOf[String])
    val businessObjectDefinitionCaptor = ArgumentCaptor.forClass(classOf[String])
    val includeUpdateHistoryCaptor = ArgumentCaptor.forClass((classOf[Boolean]))
    val businessObjectDefinition = new BusinessObjectDefinition
    when(mockBusinessObjectDefinitionApi.businessObjectDefinitionGetBusinessObjectDefinition(namespaceCaptor.capture(),
      businessObjectDefinitionCaptor.capture(), includeUpdateHistoryCaptor.capture())).thenReturn(businessObjectDefinition)
    when(defaultHerdApi.getBusinessObjectDefinitionApi(mockApiClient)).thenReturn(mockBusinessObjectDefinitionApi)

    assertEquals(businessObjectDefinition, defaultHerdApi.getBusinessObjectByName(NAMESPACE, BUSINESS_OBJECT_DEFINITION))
    verify(mockBusinessObjectDefinitionApi).businessObjectDefinitionGetBusinessObjectDefinition(anyString(), anyString(), anyBoolean())
    verify(defaultHerdApi).getBusinessObjectDefinitionApi(mockApiClient)

    assertEquals(NAMESPACE, namespaceCaptor.getValue)
    assertEquals(BUSINESS_OBJECT_DEFINITION, businessObjectDefinitionCaptor.getValue)
    assertEquals(false, includeUpdateHistoryCaptor.getValue)
  }

  test("Test HerdApi.getBusinessObjectsByNamespace") {
    val namespaceCaptor = ArgumentCaptor.forClass(classOf[String])
    val businessObjectDefinitionKeys = new BusinessObjectDefinitionKeys
    when(mockBusinessObjectDefinitionApi.businessObjectDefinitionGetBusinessObjectDefinitions1(namespaceCaptor.capture()))
      .thenReturn(businessObjectDefinitionKeys)
    when(defaultHerdApi.getBusinessObjectDefinitionApi(mockApiClient)).thenReturn(mockBusinessObjectDefinitionApi)

    assertEquals(businessObjectDefinitionKeys, defaultHerdApi.getBusinessObjectsByNamespace(NAMESPACE))
    verify(mockBusinessObjectDefinitionApi).businessObjectDefinitionGetBusinessObjectDefinitions1(anyString())
    verify(defaultHerdApi).getBusinessObjectDefinitionApi(mockApiClient)

    assertEquals(NAMESPACE, namespaceCaptor.getValue)
  }

  test("Test HerdApi.registerBusinessObject") {
    val createRequestCaptor = ArgumentCaptor.forClass(classOf[BusinessObjectDefinitionCreateRequest])
    when(mockBusinessObjectDefinitionApi.businessObjectDefinitionCreateBusinessObjectDefinition(createRequestCaptor.capture())).thenReturn(null)
    when(defaultHerdApi.getBusinessObjectDefinitionApi(mockApiClient)).thenReturn(mockBusinessObjectDefinitionApi)

    defaultHerdApi.registerBusinessObject(NAMESPACE, BUSINESS_OBJECT_DEFINITION, DATA_PROVIDER)
    verify(mockBusinessObjectDefinitionApi).businessObjectDefinitionCreateBusinessObjectDefinition(createRequestCaptor.capture())
    verify(defaultHerdApi).getBusinessObjectDefinitionApi(mockApiClient)

    val createRequest = createRequestCaptor.getValue.asInstanceOf[BusinessObjectDefinitionCreateRequest]
    assertEquals(NAMESPACE, createRequest.getNamespace)
    assertEquals(BUSINESS_OBJECT_DEFINITION, createRequest.getBusinessObjectDefinitionName)
    assertEquals(DATA_PROVIDER, createRequest.getDataProviderName)
  }

  test("Test HerdApi.getBusinessObjectFormats") {
    val namespaceCaptor = ArgumentCaptor.forClass(classOf[String])
    val businessObjectDefinitionCaptor = ArgumentCaptor.forClass(classOf[String])
    val formatVersionCatpor = ArgumentCaptor.forClass(classOf[Boolean])
    val businessObjectFormatKeys = new BusinessObjectFormatKeys
    when(mockBusinessObjectFormatApi.businessObjectFormatGetBusinessObjectFormats(namespaceCaptor.capture(), businessObjectDefinitionCaptor.capture(),
      formatVersionCatpor.capture())).thenReturn(businessObjectFormatKeys)
    when(defaultHerdApi.getBusinessObjectFormatApi(mockApiClient)).thenReturn(mockBusinessObjectFormatApi)

    assertEquals(businessObjectFormatKeys, defaultHerdApi.getBusinessObjectFormats(NAMESPACE, BUSINESS_OBJECT_DEFINITION, true))
    verify(mockBusinessObjectFormatApi).businessObjectFormatGetBusinessObjectFormats(anyString(), anyString(), anyBoolean())
    verify(defaultHerdApi).getBusinessObjectFormatApi(mockApiClient)

    assertEquals(NAMESPACE, namespaceCaptor.getValue)
    assertEquals(BUSINESS_OBJECT_DEFINITION, businessObjectDefinitionCaptor.getValue)
    assertEquals(true, formatVersionCatpor.getValue)
  }

  test("Test HerdApi.getBusinessObjectFormat") {
    val namespaceCaptor = ArgumentCaptor.forClass(classOf[String])
    val businessObjectDefinitionCaptor = ArgumentCaptor.forClass(classOf[String])
    val formatUsageCaptor = ArgumentCaptor.forClass(classOf[String])
    val fileTypeCaptor = ArgumentCaptor.forClass(classOf[String])
    val formatVersionCatpor = ArgumentCaptor.forClass(classOf[Int])
    val businessObjectFormat = new BusinessObjectFormat
    when(mockBusinessObjectFormatApi.businessObjectFormatGetBusinessObjectFormat(namespaceCaptor.capture(), businessObjectDefinitionCaptor.capture(),
      formatUsageCaptor.capture(), fileTypeCaptor.capture(), formatVersionCatpor.capture())).thenReturn(businessObjectFormat)
    when(defaultHerdApi.getBusinessObjectFormatApi(mockApiClient)).thenReturn(mockBusinessObjectFormatApi)

    assertEquals(businessObjectFormat, defaultHerdApi.getBusinessObjectFormat(NAMESPACE, BUSINESS_OBJECT_DEFINITION, FORMAT_USAGE, FILE_TYPE, FORMAT_VERSION))
    verify(mockBusinessObjectFormatApi).businessObjectFormatGetBusinessObjectFormat(anyString(), anyString(), anyString(), anyString(), anyInt())
    verify(defaultHerdApi).getBusinessObjectFormatApi(mockApiClient)

    assertEquals(NAMESPACE, namespaceCaptor.getValue)
    assertEquals(BUSINESS_OBJECT_DEFINITION, businessObjectDefinitionCaptor.getValue)
    assertEquals(FORMAT_USAGE, formatUsageCaptor.getValue)
    assertEquals(FILE_TYPE, fileTypeCaptor.getValue)
    assertEquals(FORMAT_VERSION, formatVersionCatpor.getValue)
  }

  test("Test HerdApi.registerBusinessObjectFormat") {
    val createRequestCaptor = ArgumentCaptor.forClass(classOf[BusinessObjectFormatCreateRequest])
    val businessObjectFormat = new BusinessObjectFormat
    val schema = new Schema
    businessObjectFormat.setBusinessObjectFormatVersion(FORMAT_VERSION)
    when(mockBusinessObjectFormatApi.businessObjectFormatCreateBusinessObjectFormat(createRequestCaptor.capture())).thenReturn(businessObjectFormat)
    when(defaultHerdApi.getBusinessObjectFormatApi(mockApiClient)).thenReturn(mockBusinessObjectFormatApi)

    assertEquals(FORMAT_VERSION, defaultHerdApi.registerBusinessObjectFormat(NAMESPACE, BUSINESS_OBJECT_DEFINITION,
      FORMAT_USAGE, FILE_TYPE, PARTITION_KEY, Some(schema)))
    verify(mockBusinessObjectFormatApi).businessObjectFormatCreateBusinessObjectFormat(any())
    verify(defaultHerdApi).getBusinessObjectFormatApi(mockApiClient)

    val createRequest = createRequestCaptor.getValue.asInstanceOf[BusinessObjectFormatCreateRequest]
    assertEquals(NAMESPACE, createRequest.getNamespace)
    assertEquals(BUSINESS_OBJECT_DEFINITION, createRequest.getBusinessObjectDefinitionName)
    assertEquals(FORMAT_USAGE, createRequest.getBusinessObjectFormatUsage)
    assertEquals(FILE_TYPE, createRequest.getBusinessObjectFormatFileType)
    assertEquals(PARTITION_KEY, createRequest.getPartitionKey)
    assertEquals(schema, createRequest.getSchema)
  }

  test("Test HerdApi.getBusinessObjectDataAvailability") {
    val businessObjectDataAvailability = new BusinessObjectDataAvailability
    val businessObjectDataAvailabilityRequestCaptor = ArgumentCaptor.forClass(classOf[BusinessObjectDataAvailabilityRequest])
    when(mockBusinessObjectDataApi.businessObjectDataCheckBusinessObjectDataAvailability(businessObjectDataAvailabilityRequestCaptor.capture()))
        .thenReturn(businessObjectDataAvailability)
    when(defaultHerdApi.getBusinessObjectDataApi(mockApiClient)).thenReturn(mockBusinessObjectDataApi)

    val actualBusinessObjectDataAvailability = defaultHerdApi.getBusinessObjectDataAvailability(NAMESPACE, BUSINESS_OBJECT_DEFINITION, FORMAT_USAGE,
      FILE_TYPE, PARTITION_KEY, "firstPartition", "lastPartition")
    verify(defaultHerdApi).getBusinessObjectDataApi(mockApiClient)
    verify(mockBusinessObjectDataApi).businessObjectDataCheckBusinessObjectDataAvailability(any())

    val businessObjectDataAvailabilityRequest = businessObjectDataAvailabilityRequestCaptor.getValue.asInstanceOf[BusinessObjectDataAvailabilityRequest]
    assertEquals(NAMESPACE, businessObjectDataAvailabilityRequest.getNamespace)
    assertEquals(BUSINESS_OBJECT_DEFINITION, businessObjectDataAvailabilityRequest.getBusinessObjectDefinitionName)
    assertEquals(FORMAT_USAGE, businessObjectDataAvailabilityRequest.getBusinessObjectFormatUsage)
    assertEquals(FILE_TYPE, businessObjectDataAvailabilityRequest.getBusinessObjectFormatFileType)
    val partitionValueFilter = businessObjectDataAvailabilityRequest.getPartitionValueFilter
    assertEquals(PARTITION_KEY, partitionValueFilter.getPartitionKey)
    assertEquals("firstPartition", partitionValueFilter.getPartitionValueRange.getStartPartitionValue)
    assertEquals("lastPartition", partitionValueFilter.getPartitionValueRange.getEndPartitionValue)
  }

  test("Test HerdApi.getBusinessObjectDataGenerateDdl") {
    val businessObjectDataDdlRequestCaptor = ArgumentCaptor.forClass(classOf[BusinessObjectDataDdlRequest])
    val businessObjectDataDdl = new BusinessObjectDataDdl
    when(mockBusinessObjectDataApi.businessObjectDataGenerateBusinessObjectDataDdl(businessObjectDataDdlRequestCaptor.capture()))
      .thenReturn(businessObjectDataDdl)
    when(defaultHerdApi.getBusinessObjectDataApi(mockApiClient)).thenReturn(mockBusinessObjectDataApi)

    assertEquals(businessObjectDataDdl, defaultHerdApi.getBusinessObjectDataGenerateDdl(NAMESPACE, BUSINESS_OBJECT_DEFINITION, FORMAT_USAGE,
      FILE_TYPE, FORMAT_VERSION, PARTITION_KEY, PARTITION_VALUES, DATA_VERSION))
    verify(defaultHerdApi).getBusinessObjectDataApi(mockApiClient)
    verify(mockBusinessObjectDataApi).businessObjectDataGenerateBusinessObjectDataDdl(any())

    val businessObjectDataDdlRequest = businessObjectDataDdlRequestCaptor.getValue.asInstanceOf[BusinessObjectDataDdlRequest]
    assertEquals(NAMESPACE, businessObjectDataDdlRequest.getNamespace)
    assertEquals(BUSINESS_OBJECT_DEFINITION, businessObjectDataDdlRequest.getBusinessObjectDefinitionName)
    assertEquals(FORMAT_USAGE, businessObjectDataDdlRequest.getBusinessObjectFormatUsage)
    assertEquals(FILE_TYPE, businessObjectDataDdlRequest.getBusinessObjectFormatFileType)
    assertEquals(FORMAT_VERSION, businessObjectDataDdlRequest.getBusinessObjectFormatVersion)
    val partitionValueFilter = businessObjectDataDdlRequest.getPartitionValueFilters.get(0)
    assertEquals(PARTITION_KEY, partitionValueFilter.getPartitionKey)
    assertEquals(PARTITION_VALUES(0), partitionValueFilter.getPartitionValues.get(0))
    assertEquals(DATA_VERSION, businessObjectDataDdlRequest.getBusinessObjectDataVersion)
    assertEquals(BusinessObjectDataDdlRequest.OutputFormatEnum.HIVE_13_DDL, businessObjectDataDdlRequest.getOutputFormat)
    assertEquals("HerdSpark", businessObjectDataDdlRequest.getTableName)
  }

  test("Test HerdApi.registerBusinessObjectData") {
    val businessObjectData = new BusinessObjectData
    businessObjectData.setVersion(DATA_VERSION)
    val storageUnit = new StorageUnit
    val storage = new Storage
    storage.setName(STORAGE_NAME)
    storageUnit.setStorage(storage)
    storageUnit.setStorageUnitStatus("ENABLED")
    businessObjectData.setStorageUnits(util.Arrays.asList(storageUnit))
    val businessObjectDataCreateRequestCaptor = ArgumentCaptor.forClass(classOf[BusinessObjectDataCreateRequest])
    when(mockBusinessObjectDataApi.businessObjectDataCreateBusinessObjectData(businessObjectDataCreateRequestCaptor.capture())).
      thenReturn(businessObjectData)
    when(defaultHerdApi.getBusinessObjectDataApi(mockApiClient)).thenReturn(mockBusinessObjectDataApi)

    val actualResult = defaultHerdApi.registerBusinessObjectData(NAMESPACE, BUSINESS_OBJECT_DEFINITION, FORMAT_USAGE, FILE_TYPE, FORMAT_VERSION,
      PARTITION_KEY, PARTITION_VALUE, Seq(), ObjectStatus.VALID, STORAGE_NAME, Some(STORAGE_DIRECTORY))
    verify(defaultHerdApi).getBusinessObjectDataApi(mockApiClient)
    verify(mockBusinessObjectDataApi).businessObjectDataCreateBusinessObjectData(any())

    assertEquals(DATA_VERSION, actualResult._1)
    assertEquals("ENABLED", actualResult._2(0).getStorageUnitStatus)
    val businessObjectDataCreateRequest = businessObjectDataCreateRequestCaptor.getValue.asInstanceOf[BusinessObjectDataCreateRequest]
    assertEquals(NAMESPACE, businessObjectDataCreateRequest.getNamespace)
    assertEquals(BUSINESS_OBJECT_DEFINITION, businessObjectDataCreateRequest.getBusinessObjectDefinitionName)
    assertEquals(FORMAT_USAGE, businessObjectDataCreateRequest.getBusinessObjectFormatUsage)
    assertEquals(FILE_TYPE, businessObjectDataCreateRequest.getBusinessObjectFormatFileType)
    assertEquals(FORMAT_VERSION, businessObjectDataCreateRequest.getBusinessObjectFormatVersion)
    assertEquals(PARTITION_KEY, businessObjectDataCreateRequest.getPartitionKey)
    assertEquals(PARTITION_VALUE, businessObjectDataCreateRequest.getPartitionValue)
    assertEquals(0, businessObjectDataCreateRequest.getSubPartitionValues.size())
    assertEquals(ObjectStatus.VALID.toString, businessObjectDataCreateRequest.getStatus)
    assertEquals(true, businessObjectDataCreateRequest.getCreateNewVersion)
    assertEquals(STORAGE_NAME, businessObjectDataCreateRequest.getStorageUnits.get(0).getStorageName)
    assertEquals(ObjectStatus.VALID.toString, businessObjectDataCreateRequest.getStatus)
  }

  test("Test HerdApi.setStorageFiles") {
    val createRequestCaptor = ArgumentCaptor.forClass(classOf[BusinessObjectDataStorageFilesCreateRequest])
    when(mockBusinessObjectDataStorageFileApi.businessObjectDataStorageFileCreateBusinessObjectDataStorageFiles(createRequestCaptor.capture())).thenReturn(null)
    when(defaultHerdApi.getBusinessObjectDataStorageFileApi(mockApiClient)).thenReturn(mockBusinessObjectDataStorageFileApi)

    defaultHerdApi.setStorageFiles(NAMESPACE, BUSINESS_OBJECT_DEFINITION, FORMAT_USAGE, FILE_TYPE, FORMAT_VERSION,
      PARTITION_KEY, PARTITION_VALUE, Seq(), DATA_VERSION, STORAGE_NAME, Seq(("file1", 1), ("file2", 2)))

    verify(defaultHerdApi).getBusinessObjectDataStorageFileApi(mockApiClient)
    verify(mockBusinessObjectDataStorageFileApi).businessObjectDataStorageFileCreateBusinessObjectDataStorageFiles(any())

    val req = createRequestCaptor.getValue.asInstanceOf[BusinessObjectDataStorageFilesCreateRequest]
    assertEquals(NAMESPACE, req.getNamespace)
    assertEquals(BUSINESS_OBJECT_DEFINITION, req.getBusinessObjectDefinitionName)
    assertEquals(FORMAT_USAGE, req.getBusinessObjectFormatUsage)
    assertEquals(FILE_TYPE, req.getBusinessObjectFormatFileType)
    assertEquals(FORMAT_VERSION, req.getBusinessObjectFormatVersion)
    assertEquals(PARTITION_VALUE, req.getPartitionValue)
    assertEquals(0, req.getSubPartitionValues.size())
    assertEquals(STORAGE_NAME, req.getStorageName)
    val storageFiles = req.getStorageFiles
    assertEquals(2, storageFiles.size())
    assertEquals("file1", storageFiles.get(0).getFilePath)
    assertEquals(1L, storageFiles.get(0).getFileSizeBytes)
    assertEquals("file2", storageFiles.get(1).getFilePath)
    assertEquals(2L, storageFiles.get(1).getFileSizeBytes)
  }
}
