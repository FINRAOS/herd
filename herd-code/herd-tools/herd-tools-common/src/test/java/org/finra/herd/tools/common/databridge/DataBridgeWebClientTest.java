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
package org.finra.herd.tools.common.databridge;

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.impl.MockHttpClientOperationsImpl;
import org.finra.herd.sdk.invoker.ApiClient;
import org.finra.herd.sdk.invoker.ApiException;
import org.finra.herd.sdk.invoker.auth.HttpBasicAuth;
import org.finra.herd.sdk.invoker.auth.OAuth;
import org.finra.herd.sdk.model.*;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.tools.common.dto.DataBridgeBaseManifestDto;
import org.finra.herd.tools.common.dto.ManifestFile;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.tools.common.dto.UploaderInputManifestDto;

import static org.junit.Assert.*;

public class DataBridgeWebClientTest extends AbstractDataBridgeTest
{
    private DataBridgeWebClient dataBridgeWebClient;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private ApiClient apiClient;

    @Autowired
    private OAuthTokenProvider oAuthTokenProvider;

    @Autowired
    private ApiClientHelper apiClientHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Before
    public void before()
    {
        dataBridgeWebClient = new DataBridgeWebClient()
        {

        };

        RegServerAccessParamsDto regServerAccessParamsDto = new RegServerAccessParamsDto();
        regServerAccessParamsDto.setUseSsl(false);
        regServerAccessParamsDto.setRegServerPort(8080);
        dataBridgeWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);
        dataBridgeWebClient.herdStringHelper = herdStringHelper;
        dataBridgeWebClient.apiClient = apiClient;
        dataBridgeWebClient.oauthTokenProvider = oAuthTokenProvider;
        dataBridgeWebClient.apiClientHelper = apiClientHelper;
        dataBridgeWebClient.jsonHelper = jsonHelper;
        dataBridgeWebClient.regServerAccessParamsDto.setRegServerHost("dummyHostName");
    }

    @Test
    public void testAddStorageFiles() throws Exception
    {
        testAddStorageFiles(false);
    }

    @Test
    public void testAddStorageFilesResponse200BadContentReturnsNull() throws Exception
    {
        dataBridgeWebClient.regServerAccessParamsDto.setRegServerHost(MockHttpClientOperationsImpl.HOSTNAME_RESPOND_WITH_STATUS_CODE_200_AND_INVALID_CONTENT);
        dataBridgeWebClient.regServerAccessParamsDto.setUseSsl(false);

        org.finra.herd.sdk.model.BusinessObjectDataKey businessObjectDataKey = new org.finra.herd.sdk.model.BusinessObjectDataKey();
        UploaderInputManifestDto manifest = getUploaderInputManifestDto();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        String storageName = "testStorage";

        try{
            dataBridgeWebClient.addStorageFiles(businessObjectDataKey, manifest, s3FileTransferRequestParamsDto, storageName);}
        catch (ApiException e){
            assertEquals("invalid xml", e.getMessage());
        }
    }

    @Test
    public void testAddStorageFilesUseSsl() throws Exception
    {
        testAddStorageFiles(true);
    }

    @Test
    public void testGetRegServerAccessParamsDto()
    {
        RegServerAccessParamsDto regServerAccessParamsDto = dataBridgeWebClient.getRegServerAccessParamsDto();

        assertEquals(RegServerAccessParamsDto.builder().withRegServerHost("dummyHostName").withRegServerPort(8080).withUseSsl(false).build(), regServerAccessParamsDto);
    }

    @Test
    public void testGetS3KeyPrefix() throws Exception
    {
        testGetS3KeyPrefix("testNamespace", Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2"), 0, "testStorage", false);
    }

    @Test
    public void testGetS3KeyPrefixNoDataVersion() throws Exception
    {
        testGetS3KeyPrefix("testNamespace", Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2"), null, "testStorage", false);
    }

    @Test
    public void testGetS3KeyPrefixNoNamespace() throws Exception
    {
        try {
            testGetS3KeyPrefix(null, Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2"), 0, "testStorage", false);
            fail();
        } catch (ApiException e) {
            assertEquals("Missing the required parameter 'namespace' when calling businessObjectDataGetS3KeyPrefix", e.getMessage());
        }
    }

    @Test
    public void testGetS3KeyPrefixNoStorageName() throws Exception
    {
        testGetS3KeyPrefix("testNamespace", null, 0, null, false);
    }

    @Test
    public void testGetS3KeyPrefixNoSubPartitions() throws Exception
    {
        testGetS3KeyPrefix("testNamespace", null, 0, "testStorage", false);
    }

    @Test
    public void testGetS3KeyPrefixUseSsl() throws Exception
    {
        testGetS3KeyPrefix("testNamespace", Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2"), 0, "testStorage", true);
    }

    @Test
    public void testGetStorage() throws Exception
    {
        testGetStorage(false);
    }

    @Test
    public void testGetStorageUseSsl() throws Exception
    {
        testGetStorage(true);
    }

    @Test
    public void testPreRegisterBusinessObjectData() throws Exception
    {
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put("testAttributeName", "testAttributeValue");
        testPreRegisterBusinessObjectData(attributes, false);
    }

    @Test
    public void testPreRegisterBusinessObjectDataAttributesNull() throws Exception
    {
        testPreRegisterBusinessObjectData(null, true);
    }

    @Test
    public void testPreRegisterBusinessObjectDataUseSsl() throws Exception
    {
        testPreRegisterBusinessObjectData(new HashMap<>(), true);
    }

    @Test
    public void testUpdateBusinessObjectDataStatus() throws Exception
    {
        testUpdateBusinessObjectDataStatus(null, false);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusUseSsl() throws Exception
    {
        testUpdateBusinessObjectDataStatus(null, true);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusWithMoreThanMaxSubPartitions() throws Exception
    {
        testUpdateBusinessObjectDataStatus(
            Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2", "testSubPartitionValue3", "testSubPartitionValue4", "testSubPartitionValue4"),
            true);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusWithSubPartitions() throws Exception
    {
        testUpdateBusinessObjectDataStatus(
            Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2", "testSubPartitionValue3", "testSubPartitionValue4"), true);
    }

    @Test
    public void testGetBusinessObjectDataKey()
    {
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setNamespace("test1");
        businessObjectData.setBusinessObjectDefinitionName("test2");
        businessObjectData.setBusinessObjectFormatUsage("test3");
        businessObjectData.setBusinessObjectFormatFileType("test4");
        businessObjectData.setBusinessObjectFormatVersion(5);
        businessObjectData.setPartitionValue("test6");
        businessObjectData.setSubPartitionValues(Arrays.asList("a", "b"));
        businessObjectData.setVersion(0);
        BusinessObjectDataKey businessObjectDataKey = dataBridgeWebClient.getBusinessObjectDataKey(businessObjectData);
        assertEquals(businessObjectData.getNamespace(), businessObjectDataKey.getNamespace());
        assertEquals(businessObjectData.getBusinessObjectDefinitionName(), businessObjectDataKey.getBusinessObjectDefinitionName());
        assertEquals(businessObjectData.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatUsage());
        assertEquals(businessObjectData.getBusinessObjectFormatFileType(), businessObjectDataKey.getBusinessObjectFormatFileType());
        assertEquals(businessObjectData.getBusinessObjectFormatVersion(), businessObjectDataKey.getBusinessObjectFormatVersion());
        assertEquals(businessObjectData.getPartitionValue(), businessObjectDataKey.getPartitionValue());
        assertEquals(businessObjectData.getSubPartitionValues(), businessObjectDataKey.getSubPartitionValues());
        assertEquals(businessObjectData.getVersion(), businessObjectDataKey.getBusinessObjectDataVersion());
    }

    @Test
    public void testCreateApiClient() throws URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, ApiException
    {
        RegServerAccessParamsDto regServerAccessParamsDto = new RegServerAccessParamsDto();
        regServerAccessParamsDto.setUseSsl(false);
        regServerAccessParamsDto.setRegServerHost("localhost");
        regServerAccessParamsDto.setRegServerPort(8080);
        regServerAccessParamsDto.setUsername("username");
        regServerAccessParamsDto.setPassword("password");
        regServerAccessParamsDto.setUseSsl(true);

        // Basic Auth
        ApiClient oauthApiClient = dataBridgeWebClient.createApiClient(regServerAccessParamsDto);
        OAuth oauth = (OAuth) oauthApiClient.getAuthentication("oauthAuth");
        HttpBasicAuth basicAuth = (HttpBasicAuth) oauthApiClient.getAuthentication("basicAuth");
        assertNull(oauth.getAccessToken());
        assertEquals(regServerAccessParamsDto.getUsername(), basicAuth.getUsername());
        assertEquals(regServerAccessParamsDto.getPassword(), basicAuth.getPassword());
        apiClient.setUsername(null);
        apiClient.setPassword(null);

        // OAuth
        regServerAccessParamsDto.setAccessTokenUrl("dummyUrl");
        ApiClient basicAuthApiClient = dataBridgeWebClient.createApiClient(regServerAccessParamsDto);
        oauth = (OAuth) basicAuthApiClient.getAuthentication("oauthAuth");
        basicAuth = (HttpBasicAuth) basicAuthApiClient.getAuthentication("basicAuth");
        assertNotNull(oauth.getAccessToken());
        assertNull(basicAuth.getUsername());
        assertNull(basicAuth.getPassword());
    }

    /**
     * Creates a UploaderInputManifestDto.
     *
     * @return the created UploaderInputManifestDto instance
     */
    private UploaderInputManifestDto getUploaderInputManifestDto()
    {
        UploaderInputManifestDto manifest = new UploaderInputManifestDto();
        manifest.setNamespace("testNamespace");
        manifest.setBusinessObjectDefinitionName("testBusinessObjectDefinitionName");
        manifest.setBusinessObjectFormatUsage("testBusinessObjectFormatUsage");
        manifest.setBusinessObjectFormatFileType("testBusinessObjectFormatFileType");
        manifest.setBusinessObjectFormatVersion("0");
        manifest.setPartitionKey("testPartitionKey");
        manifest.setPartitionValue("testPartitionValue");
        manifest.setSubPartitionValues(Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2"));
        List<ManifestFile> manifestFiles = new ArrayList<>();
        {
            ManifestFile manifestFile = new ManifestFile();
            manifestFile.setFileName("testFileName");
            manifestFile.setFileSizeBytes(1l);
            manifestFile.setRowCount(2l);
            manifestFiles.add(manifestFile);
        }
        manifest.setManifestFiles(manifestFiles);
        HashMap<String, String> attributes = new HashMap<>();
        {
            attributes.put("testName", "testValue");
        }
        manifest.setAttributes(attributes);
        return manifest;
    }

    /**
     * Calls addStorageFiles() method and makes assertions.
     *
     * @param useSsl specifies whether to use SSL or not
     *
     * @throws ApiException if an Api exception was encountered
     */
    private void testAddStorageFiles(boolean useSsl)
        throws ApiException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        dataBridgeWebClient.regServerAccessParamsDto.setUseSsl(useSsl);

        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        UploaderInputManifestDto manifest = getUploaderInputManifestDto();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        String storageName = "testStorage";

        BusinessObjectDataStorageFilesCreateResponse businessObjectDataStorageFilesCreateResponse =
            dataBridgeWebClient.addStorageFiles(businessObjectDataKey, manifest, s3FileTransferRequestParamsDto, storageName);

        assertNotNull("businessObjectDataStorageFilesCreateResponse", businessObjectDataStorageFilesCreateResponse);
    }

    /**
     * Calls getS3KeyPrefix() method and makes assertions.
     *
     * @param namespace the namespace
     * @param subPartitionValues the list of sub-partition values
     * @param businessObjectDataVersion the version of the business object data, may be null
     * @param useSsl specifies whether to use SSL or not
     *
     * @throws Exception if an error is encountered
     */
    private void testGetS3KeyPrefix(String namespace, List<String> subPartitionValues, Integer businessObjectDataVersion, String storageName, boolean useSsl)
        throws Exception
    {
        dataBridgeWebClient.regServerAccessParamsDto.setUseSsl(useSsl);

        DataBridgeBaseManifestDto manifest = getUploaderInputManifestDto();
        manifest.setNamespace(namespace);
        manifest.setSubPartitionValues(subPartitionValues);
        manifest.setStorageName(storageName);
        Boolean createNewVersion = false;

        S3KeyPrefixInformation s3KeyPrefix = dataBridgeWebClient.getS3KeyPrefix(manifest, businessObjectDataVersion, createNewVersion);

        assertNotNull("s3KeyPrefix is null", s3KeyPrefix);
    }

    /**
     * Calls getStorage() method and makes assertions.
     *
     * @param useSsl specifies whether to use SSL or not
     *
     * @throws ApiException if an Api exception was encountered
     */
    private void testGetStorage(boolean useSsl) throws ApiException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        dataBridgeWebClient.regServerAccessParamsDto.setUseSsl(useSsl);

        String expectedStorageName = "testStorage";

        Storage storage = dataBridgeWebClient.getStorage(expectedStorageName);

        assertNotNull("storage is null", storage);
        assertEquals("storage name", expectedStorageName, storage.getName());
    }

    /**
     * Calls preRegisterBusinessObjectData() method and makes assertions.
     *
     * @param attributes a map of business object data attributes
     * @param useSsl specifies whether to use SSL or not
     *
     * @throws ApiException if an Api exception was encountered
     */
    private void testPreRegisterBusinessObjectData(HashMap<String, String> attributes, boolean useSsl)
        throws ApiException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        dataBridgeWebClient.regServerAccessParamsDto.setUseSsl(useSsl);

        UploaderInputManifestDto manifest = getUploaderInputManifestDto();
        manifest.setAttributes(attributes);
        String storageName = "testStorage";
        Boolean createNewVersion = false;

        BusinessObjectData businessObjectData = dataBridgeWebClient.preRegisterBusinessObjectData(manifest, storageName, createNewVersion);

        assertNotNull("businessObjectData", businessObjectData);
    }

    /**
     * Calls updateBusinessObjectDataStatus() method and makes assertions.
     *
     * @param useSsl specifies whether to use SSL or not
     *
     * @throws ApiException if an Api exception was encountered
     */
    private void testUpdateBusinessObjectDataStatus(List<String> subPartitionValues, boolean useSsl)
        throws ApiException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        dataBridgeWebClient.regServerAccessParamsDto.setUseSsl(useSsl);

        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace("test1");
        businessObjectDataKey.setBusinessObjectDefinitionName("test2");
        businessObjectDataKey.setBusinessObjectFormatUsage("test3");
        businessObjectDataKey.setBusinessObjectFormatFileType("test4");
        businessObjectDataKey.setBusinessObjectFormatVersion(5);
        businessObjectDataKey.setPartitionValue("test6");
        businessObjectDataKey.setSubPartitionValues(subPartitionValues);
        businessObjectDataKey.setBusinessObjectDataVersion(0);
        String businessObjectDataStatus = "testBusinessObjectDataStatus";

        BusinessObjectDataStatusUpdateResponse businessObjectDataStatusUpdateResponse =
            dataBridgeWebClient.updateBusinessObjectDataStatus(businessObjectDataKey, businessObjectDataStatus);

        assertNotNull("businessObjectDataStatusUpdateResponse", businessObjectDataStatusUpdateResponse);
    }
}
