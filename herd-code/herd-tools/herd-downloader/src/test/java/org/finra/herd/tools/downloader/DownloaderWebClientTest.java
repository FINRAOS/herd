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
package org.finra.herd.tools.downloader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;

import org.finra.herd.sdk.invoker.ApiClient;
import org.finra.herd.sdk.invoker.ApiException;
import org.finra.herd.sdk.model.*;
import org.finra.herd.tools.common.MockApiClient;
import org.finra.herd.tools.common.dto.DownloaderInputManifestDto;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import org.finra.herd.core.Command;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.tools.common.dto.UploaderInputManifestDto;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

/**
 * Unit tests for DownloaderWebClient class.
 */
public class DownloaderWebClientTest extends AbstractDownloaderTest
{
    @Test
    public void testGetBusinessObjectDataAssertNoAuthorizationHeaderWhenNoSsl() throws Exception
    {
        ApiClient mockApiClient = mock(MockApiClient.class);
        ApiClient originalApiClient = (ApiClient) ReflectionTestUtils.getField(downloaderWebClient, "apiClient");
        ReflectionTestUtils.setField(downloaderWebClient, "apiClient", mockApiClient);

        try
        {
            when(mockApiClient.invokeAPI(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(null);
            when(mockApiClient.escapeString(any())).thenReturn("Test");

            DownloaderInputManifestDto manifest = getTestDownloaderInputManifestDto();

            downloaderWebClient.getRegServerAccessParamsDto().setUseSsl(false);
            downloaderWebClient.getBusinessObjectData(manifest);

            verify(mockApiClient, never()).setUsername(any());
            verify(mockApiClient, never()).setPassword(any());
        }
        finally
        {
            ReflectionTestUtils.setField(downloaderWebClient, "apiClient", originalApiClient);
        }
    }

    @Test
    public void testGetBusinessObjectDataDownloadCredential1() throws Exception
    {
        DownloaderInputManifestDto manifest = new DownloaderInputManifestDto();
        manifest.setNamespace("test1");
        manifest.setBusinessObjectDefinitionName("test2");
        manifest.setBusinessObjectFormatUsage("test3");
        manifest.setBusinessObjectFormatFileType("test4");
        manifest.setBusinessObjectFormatVersion("5");
        manifest.setPartitionValue("test6");
        manifest.setSubPartitionValues(Arrays.asList("test7", "test8"));
        manifest.setBusinessObjectDataVersion("9");
        String storageName = "test10";
        StorageUnitDownloadCredential storageUnitDownloadCredential = downloaderWebClient.getStorageUnitDownloadCredential(manifest, storageName);
        Assert.assertNotNull(storageUnitDownloadCredential);
        AwsCredential awsCredential = storageUnitDownloadCredential.getAwsCredential();
        Assert.assertNotNull(awsCredential);
        Assert.assertEquals("https://testWebServiceHostname:1234/herd-app/rest/storageUnits/download/credential/namespaces/test1" +
            "/businessObjectDefinitionNames/test2/businessObjectFormatUsages/test3/businessObjectFormatFileTypes/test4/businessObjectFormatVersions/5" +
            "/partitionValues/test6/businessObjectDataVersions/9/storageNames/test10?subPartitionValues=test7%7Ctest8", awsCredential.getAwsAccessKey());
    }

    @Test
    public void testGetBusinessObjectDataDownloadCredential2() throws Exception
    {
        DownloaderInputManifestDto manifest = new DownloaderInputManifestDto();
        manifest.setNamespace("test1");
        manifest.setBusinessObjectDefinitionName("test2");
        manifest.setBusinessObjectFormatUsage("test3");
        manifest.setBusinessObjectFormatFileType("test4");
        manifest.setBusinessObjectFormatVersion("5");
        manifest.setPartitionValue("test6");
        manifest.setBusinessObjectDataVersion("9");
        String storageName = "test10";
        downloaderWebClient.getRegServerAccessParamsDto().setUseSsl(true);
        StorageUnitDownloadCredential storageUnitDownloadCredential = downloaderWebClient.getStorageUnitDownloadCredential(manifest, storageName);
        Assert.assertNotNull(storageUnitDownloadCredential);
        AwsCredential awsCredential = storageUnitDownloadCredential.getAwsCredential();
        Assert.assertNotNull(awsCredential);
        Assert.assertEquals("https://testWebServiceHostname:1234/herd-app/rest/storageUnits/download/credential/namespaces/test1" +
            "/businessObjectDefinitionNames/test2/businessObjectFormatUsages/test3/businessObjectFormatFileTypes/test4/businessObjectFormatVersions/5" +
            "/partitionValues/test6/businessObjectDataVersions/9/storageNames/test10", awsCredential.getAwsAccessKey());
    }

    @Test
    public void testGetBusinessObjectDataDownloadCredentialAssertHttpClientClosedWhenIOException() throws Exception
    {
        ApiClient mockApiClient = mock(MockApiClient.class);
        ApiClient originalApiClient = (ApiClient) ReflectionTestUtils.getField(downloaderWebClient, "apiClient");
        ReflectionTestUtils.setField(downloaderWebClient, "apiClient", mockApiClient);

        try
        {
            ApiException expectedException = new ApiException("Missing the required parameter 'namespace' when calling storageUnitGetStorageUnitDownloadCredential");

            when(mockApiClient.invokeAPI(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenThrow(expectedException);

            DownloaderInputManifestDto downloaderInputManifestDto = new DownloaderInputManifestDto();
            String storageName = "storageName";

            try
            {
                downloaderWebClient.getStorageUnitDownloadCredential(downloaderInputManifestDto, storageName);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(expectedException.getClass(), e.getClass());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(downloaderWebClient, "apiClient", originalApiClient);
        }
    }

    @Test
    public void testGetBusinessObjectDataDownloadCredentialAssertNoAuthorizationHeaderWhenNoSsl() throws Exception
    {
        ApiClient mockApiClient = mock(MockApiClient.class);
        ApiClient originalApiClient = (ApiClient) ReflectionTestUtils.getField(downloaderWebClient, "apiClient");
        ReflectionTestUtils.setField(downloaderWebClient, "apiClient", mockApiClient);

        try
        {
            when(mockApiClient.invokeAPI(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(new org.finra.herd.sdk.model.StorageUnitDownloadCredential());
            when(mockApiClient.escapeString(any())).thenReturn("Test");

            DownloaderInputManifestDto downloaderInputManifestDto = getTestDownloaderInputManifestDto();
            String storageName = "storageName";
            boolean useSsl = false;

            downloaderWebClient.getRegServerAccessParamsDto().setUseSsl(useSsl);
            downloaderWebClient.getStorageUnitDownloadCredential(downloaderInputManifestDto, storageName);

            verify(mockApiClient, never()).setUsername(any());
            verify(mockApiClient, never()).setPassword(any());
        }
        finally
        {
            ReflectionTestUtils.setField(downloaderWebClient, "apiClient", originalApiClient);
        }
    }

    /**
     * Asserts that the http client is closed and if an exception is thrown during closing of the http client, the same exception is bubbled up.
     */
    @Test
    public void testGetBusinessObjectDataDownloadCredentialAssertThrowIOExceptionWhenClosingHttpClientThrowsIOException() throws Exception
    {

            DownloaderInputManifestDto downloaderInputManifestDto = new DownloaderInputManifestDto();
            String storageName = "storageName";

            try
            {
                downloaderWebClient.getStorageUnitDownloadCredential(downloaderInputManifestDto, storageName);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(ApiException.class, e.getClass());
            }

    }

    @Test
    public void testGetData() throws Exception
    {
        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(downloaderWebClient);

        // Upload and register the initial version if of the test business object data.
        uploadTestDataFilesToS3(S3_TEST_PATH_V0);
        final UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();

        executeWithoutLogging(DataBridgeWebClient.class, new Command()
        {
            @Override
            public void execute() throws Exception
            {
                BusinessObjectData businessObjectData =
                    downloaderWebClient.preRegisterBusinessObjectData(uploaderInputManifestDto, StorageEntity.MANAGED_STORAGE, false);
                BusinessObjectDataKey businessObjectDataKey = downloaderWebClient.getBusinessObjectDataKey(businessObjectData);
                downloaderWebClient
                    .addStorageFiles(businessObjectDataKey, uploaderInputManifestDto, getTestS3FileTransferRequestParamsDto(S3_TEST_PATH_V0 + "/"),
                        StorageEntity.MANAGED_STORAGE);
                downloaderWebClient.updateBusinessObjectDataStatus(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID);
            }
        });

        // Get business object data information.
        DownloaderInputManifestDto downloaderInputManifestDto = getTestDownloaderInputManifestDto();
        BusinessObjectData resultBusinessObjectData = downloaderWebClient.getBusinessObjectData(downloaderInputManifestDto);

        // Validate the results.
        assertNotNull(resultBusinessObjectData);
    }

    @Test
    public void testGetS3KeyPrefix() throws Exception
    {
        // Upload and register business object data parents.
        uploadAndRegisterTestDataParents(downloaderWebClient);

        // Upload and register the initial version if of the test business object data.
        uploadTestDataFilesToS3(S3_TEST_PATH_V0);
        final UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();

        executeWithoutLogging(DataBridgeWebClient.class, new Command()
        {
            @Override
            public void execute() throws Exception
            {
                BusinessObjectData businessObjectData =
                    downloaderWebClient.preRegisterBusinessObjectData(uploaderInputManifestDto, StorageEntity.MANAGED_STORAGE, false);
                BusinessObjectDataKey businessObjectDataKey = downloaderWebClient.getBusinessObjectDataKey(businessObjectData);
                downloaderWebClient
                    .addStorageFiles(businessObjectDataKey, uploaderInputManifestDto, getTestS3FileTransferRequestParamsDto(S3_TEST_PATH_V0 + "/"),
                        StorageEntity.MANAGED_STORAGE);
                downloaderWebClient.updateBusinessObjectDataStatus(businessObjectDataKey, BusinessObjectDataStatusEntity.VALID);
            }
        });

        // Get S3 key prefix.
        BusinessObjectData businessObjectData = toBusinessObjectData(uploaderInputManifestDto);
        S3KeyPrefixInformation resultS3KeyPrefixInformation = downloaderWebClient.getS3KeyPrefix(businessObjectData);

        // Validate the results.
        assertNotNull(resultS3KeyPrefixInformation);
        assertEquals(S3_SIMPLE_TEST_PATH, resultS3KeyPrefixInformation.getS3KeyPrefix());
    }

    @Test
    public void testWebClientRegServerAccessParamsDtoSetterAndGetter()
    {
        // Create and initialize an instance of RegServerAccessParamsDto.
        RegServerAccessParamsDto regServerAccessParamsDto = new RegServerAccessParamsDto();
        regServerAccessParamsDto.setRegServerHost(WEB_SERVICE_HOSTNAME);
        regServerAccessParamsDto.setRegServerPort(WEB_SERVICE_HTTPS_PORT);
        regServerAccessParamsDto.setUseSsl(true);
        regServerAccessParamsDto.setUsername(WEB_SERVICE_HTTPS_USERNAME);
        regServerAccessParamsDto.setPassword(WEB_SERVICE_HTTPS_PASSWORD);

        // Set the DTO.
        downloaderWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);

        // Retrieve the DTO and validate the results.
        RegServerAccessParamsDto resultRegServerAccessParamsDto = downloaderWebClient.getRegServerAccessParamsDto();

        // validate the results.
        assertEquals(WEB_SERVICE_HOSTNAME, resultRegServerAccessParamsDto.getRegServerHost());
        assertEquals(WEB_SERVICE_HTTPS_PORT, resultRegServerAccessParamsDto.getRegServerPort());
        assertTrue(resultRegServerAccessParamsDto.isUseSsl());
        assertEquals(WEB_SERVICE_HTTPS_USERNAME, resultRegServerAccessParamsDto.getUsername());
        assertEquals(WEB_SERVICE_HTTPS_PASSWORD, resultRegServerAccessParamsDto.getPassword());
    }

    private BusinessObjectData toBusinessObjectData(final UploaderInputManifestDto uploaderInputManifestDto)
    {
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setNamespace(uploaderInputManifestDto.getNamespace());
        businessObjectData.setBusinessObjectDefinitionName(uploaderInputManifestDto.getBusinessObjectDefinitionName());
        businessObjectData.setBusinessObjectFormatUsage(uploaderInputManifestDto.getBusinessObjectFormatUsage());
        businessObjectData.setBusinessObjectFormatFileType(uploaderInputManifestDto.getBusinessObjectFormatFileType());
        businessObjectData.setBusinessObjectFormatVersion(Integer.valueOf(uploaderInputManifestDto.getBusinessObjectFormatVersion()));
        businessObjectData.setPartitionKey(uploaderInputManifestDto.getPartitionKey());
        businessObjectData.setPartitionValue(uploaderInputManifestDto.getPartitionValue());
        businessObjectData.setSubPartitionValues(uploaderInputManifestDto.getSubPartitionValues());
        businessObjectData.setVersion(TEST_DATA_VERSION_V0);
        StorageUnit storageUnit = new StorageUnit();
        Storage storage = new Storage();
        storage.setName(StorageEntity.MANAGED_STORAGE);
        storageUnit.setStorage(storage);
        businessObjectData.setStorageUnits(Collections.singletonList(storageUnit));
        return businessObjectData;
    }
}
