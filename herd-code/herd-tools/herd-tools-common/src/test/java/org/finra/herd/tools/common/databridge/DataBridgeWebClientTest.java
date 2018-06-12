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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.HttpClientOperations;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.HttpClientHelper;
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.dao.impl.MockCloseableHttpResponse;
import org.finra.herd.dao.impl.MockHttpClientOperationsImpl;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.herd.model.api.xml.ErrorInformation;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.dto.DataBridgeBaseManifestDto;
import org.finra.herd.model.dto.ManifestFile;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.UploaderInputManifestDto;

public class DataBridgeWebClientTest extends AbstractDataBridgeTest
{
    private DataBridgeWebClient dataBridgeWebClient;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private HttpClientHelper httpClientHelper;

    @Autowired
    private HttpClientOperations httpClientOperations;

    @Autowired
    private XmlHelper xmlHelper;

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

        dataBridgeWebClient.httpClientHelper = httpClientHelper;
        dataBridgeWebClient.httpClientOperations = httpClientOperations;
        dataBridgeWebClient.herdStringHelper = herdStringHelper;
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

        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        UploaderInputManifestDto manifest = getUploaderInputManifestDto();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        String storageName = "testStorage";

        BusinessObjectDataStorageFilesCreateResponse businessObjectDataStorageFilesCreateResponse =
            dataBridgeWebClient.addStorageFiles(businessObjectDataKey, manifest, s3FileTransferRequestParamsDto, storageName);

        assertNull("businessObjectDataStorageFilesCreateResponse", businessObjectDataStorageFilesCreateResponse);
    }

    @Test
    public void testAddStorageFilesUseSsl() throws Exception
    {
        testAddStorageFiles(true);
    }

    @Test
    public void testGetBusinessObjectDataStorageFilesCreateResponse200BadContentReturnsNull() throws Exception
    {
        CloseableHttpResponse httpResponse = new MockCloseableHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "testReasonPhrase"), false);
        httpResponse.setEntity(new StringEntity("invalid xml"));

        executeWithoutLogging(DataBridgeWebClient.class, () -> {
            BusinessObjectDataStorageFilesCreateResponse businessObjectDataStorageFilesCreateResponse =
                dataBridgeWebClient.getBusinessObjectDataStorageFilesCreateResponse(httpResponse);
            assertNull("businessObjectDataStorageFilesCreateResponse", businessObjectDataStorageFilesCreateResponse);
        });
    }

    @Test
    public void testGetBusinessObjectDataStorageFilesCreateResponse200ValidResponse() throws Exception
    {
        CloseableHttpResponse httpResponse = new MockCloseableHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "testReasonPhrase"), false);
        httpResponse.setEntity(new StringEntity(xmlHelper.objectToXml(new BusinessObjectDataStorageFilesCreateResponse())));
        BusinessObjectDataStorageFilesCreateResponse businessObjectDataStorageFilesCreateResponse =
            dataBridgeWebClient.getBusinessObjectDataStorageFilesCreateResponse(httpResponse);
        assertNotNull("businessObjectDataStorageFilesCreateResponse", businessObjectDataStorageFilesCreateResponse);
    }

    @Test
    public void testGetBusinessObjectDataStorageFilesCreateResponse200ValidResponseHttpResponseThrowsExceptionOnClose() throws Exception
    {
        CloseableHttpResponse httpResponse = new MockCloseableHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "testReasonPhrase"), true);
        httpResponse.setEntity(new StringEntity(xmlHelper.objectToXml(new BusinessObjectDataStorageFilesCreateResponse())));

        executeWithoutLogging(DataBridgeWebClient.class, () -> {
            BusinessObjectDataStorageFilesCreateResponse businessObjectDataStorageFilesCreateResponse =
                dataBridgeWebClient.getBusinessObjectDataStorageFilesCreateResponse(httpResponse);
            assertNotNull("businessObjectDataStorageFilesCreateResponse", businessObjectDataStorageFilesCreateResponse);
        });
    }

    @Test
    public void testGetBusinessObjectDataStorageFilesCreateResponse400BadContentThrows() throws Exception
    {
        int expectedStatusCode = 400;
        String expectedReasonPhrase = "testReasonPhrase";
        String expectedErrorMessage = "invalid xml";

        CloseableHttpResponse httpResponse =
            new MockCloseableHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, expectedStatusCode, expectedReasonPhrase), false);
        httpResponse.setEntity(new StringEntity(expectedErrorMessage));
        try
        {
            executeWithoutLogging(DataBridgeWebClient.class, () -> {
                dataBridgeWebClient.getBusinessObjectDataStorageFilesCreateResponse(httpResponse);
            });
            Assert.fail("expected HttpErrorResponseException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception type", HttpErrorResponseException.class, e.getClass());

            HttpErrorResponseException httpErrorResponseException = (HttpErrorResponseException) e;
            assertEquals("httpErrorResponseException responseMessage", expectedErrorMessage, httpErrorResponseException.getResponseMessage());
            assertEquals("httpErrorResponseException statusCode", expectedStatusCode, httpErrorResponseException.getStatusCode());
            assertEquals("httpErrorResponseException statusDescription", expectedReasonPhrase, httpErrorResponseException.getStatusDescription());
            assertEquals("httpErrorResponseException message", "Failed to add storage files", httpErrorResponseException.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataStorageFilesCreateResponse400Throws() throws Exception
    {
        int expectedStatusCode = 400;
        String expectedReasonPhrase = "testReasonPhrase";
        String expectedErrorMessage = "testErrorMessage";

        ErrorInformation errorInformation = new ErrorInformation();
        errorInformation.setStatusCode(expectedStatusCode);
        errorInformation.setMessage(expectedErrorMessage);
        errorInformation.setStatusDescription(expectedReasonPhrase);

        String requestContent = xmlHelper.objectToXml(errorInformation);

        CloseableHttpResponse httpResponse =
            new MockCloseableHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, expectedStatusCode, expectedReasonPhrase), false);
        httpResponse.setEntity(new StringEntity(requestContent));
        try
        {
            dataBridgeWebClient.getBusinessObjectDataStorageFilesCreateResponse(httpResponse);
            Assert.fail("expected HttpErrorResponseException, but no exception was thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception type", HttpErrorResponseException.class, e.getClass());

            HttpErrorResponseException httpErrorResponseException = (HttpErrorResponseException) e;
            assertEquals("httpErrorResponseException responseMessage", expectedErrorMessage, httpErrorResponseException.getResponseMessage());
            assertEquals("httpErrorResponseException statusCode", expectedStatusCode, httpErrorResponseException.getStatusCode());
            assertEquals("httpErrorResponseException statusDescription", expectedReasonPhrase, httpErrorResponseException.getStatusDescription());
            assertEquals("httpErrorResponseException message", "Failed to add storage files", httpErrorResponseException.getMessage());
        }
    }

    @Test
    public void testGetRegServerAccessParamsDto()
    {
        RegServerAccessParamsDto regServerAccessParamsDto = dataBridgeWebClient.getRegServerAccessParamsDto();

        assertEquals(RegServerAccessParamsDto.builder().withRegServerPort(8080).withUseSsl(false).build(), regServerAccessParamsDto);
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
        testGetS3KeyPrefix(null, Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2"), 0, "testStorage", false);
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
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    private void testAddStorageFiles(boolean useSsl)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
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
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    private void testGetStorage(boolean useSsl)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
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
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    private void testPreRegisterBusinessObjectData(HashMap<String, String> attributes, boolean useSsl)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
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
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    private void testUpdateBusinessObjectDataStatus(List<String> subPartitionValues, boolean useSsl)
        throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        dataBridgeWebClient.regServerAccessParamsDto.setUseSsl(useSsl);

        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setSubPartitionValues(subPartitionValues);
        String businessObjectDataStatus = "testBusinessObjectDataStatus";

        BusinessObjectDataStatusUpdateResponse businessObjectDataStatusUpdateResponse =
            dataBridgeWebClient.updateBusinessObjectDataStatus(businessObjectDataKey, businessObjectDataStatus);

        assertNotNull("businessObjectDataStatusUpdateResponse", businessObjectDataStatusUpdateResponse);
    }
}
