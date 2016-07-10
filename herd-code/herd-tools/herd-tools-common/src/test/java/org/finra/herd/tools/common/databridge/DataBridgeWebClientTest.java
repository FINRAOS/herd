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

import java.io.IOException;
import java.net.URISyntaxException;
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
import org.finra.herd.dao.helper.XmlHelper;
import org.finra.herd.dao.impl.MockCloseableHttpResponse;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.herd.model.api.xml.ErrorInformation;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.dto.DataBridgeBaseManifestDto;
import org.finra.herd.model.dto.ManifestFile;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.model.dto.UploaderInputManifestDto;

public class DataBridgeWebClientTest extends AbstractDataBridgeTest
{
    private DataBridgeWebClient dataBridgeWebClient;

    @Autowired
    private HttpClientOperations httpClientOperations;

    @Autowired
    private XmlHelper xmlHelper;

    @Autowired
    private HerdStringHelper herdStringHelper;

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

        dataBridgeWebClient.httpClientOperations = httpClientOperations;
        dataBridgeWebClient.herdStringHelper = herdStringHelper;
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
    public void testPreRegisterBusinessObjectDataUseSsl() throws Exception
    {
        testPreRegisterBusinessObjectData(new HashMap<>(), true);
    }

    @Test
    public void testPreRegisterBusinessObjectDataAttributesNull() throws Exception
    {
        testPreRegisterBusinessObjectData(null, true);
    }

    @Test
    public void testGetS3KeyPrefix() throws Exception
    {
        testGetS3KeyPrefix("testNamespace", Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2"), 0, false);
    }

    @Test
    public void testGetS3KeyPrefixNoNamespace() throws Exception
    {
        testGetS3KeyPrefix(null, Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2"), 0, false);
    }

    @Test
    public void testGetS3KeyPrefixNoSubPartitions() throws Exception
    {
        testGetS3KeyPrefix("testNamespace", null, 0, false);
    }

    @Test
    public void testGetS3KeyPrefixNoDataVersion() throws Exception
    {
        testGetS3KeyPrefix("testNamespace", Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2"), null, false);
    }

    @Test
    public void testGetS3KeyPrefixUseSsl() throws Exception
    {
        testGetS3KeyPrefix("testNamespace", Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2"), 0, true);
    }

    @Test
    public void testGetBusinessObjectDataStorageFilesCreateResponse200ValidResponse() throws Exception
    {
        CloseableHttpResponse httpResponse = new MockCloseableHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "testReasonPhrase"));
        httpResponse.setEntity(new StringEntity(xmlHelper.objectToXml(new BusinessObjectDataStorageFilesCreateResponse())));
        BusinessObjectDataStorageFilesCreateResponse businessObjectDataStorageFilesCreateResponse =
            dataBridgeWebClient.getBusinessObjectDataStorageFilesCreateResponse(httpResponse);
        Assert.assertNotNull("businessObjectDataStorageFilesCreateResponse", businessObjectDataStorageFilesCreateResponse);
    }

    @Test
    public void testGetBusinessObjectDataStorageFilesCreateResponse200BadContentReturnsNull() throws Exception
    {
        CloseableHttpResponse httpResponse = new MockCloseableHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "testReasonPhrase"));
        httpResponse.setEntity(new StringEntity("invalid xml"));

        executeWithoutLogging(DataBridgeWebClient.class, () -> {
            BusinessObjectDataStorageFilesCreateResponse businessObjectDataStorageFilesCreateResponse =
                dataBridgeWebClient.getBusinessObjectDataStorageFilesCreateResponse(httpResponse);
            Assert.assertNull("businessObjectDataStorageFilesCreateResponse", businessObjectDataStorageFilesCreateResponse);
        });
    }

    @Test
    public void testGetBusinessObjectDataStorageFilesCreateResponse400BadContentThrows() throws Exception
    {
        int expectedStatusCode = 400;
        String expectedReasonPhrase = "testReasonPhrase";
        String expectedErrorMessage = "invalid xml";

        CloseableHttpResponse httpResponse = new MockCloseableHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, expectedStatusCode, expectedReasonPhrase));
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
            Assert.assertEquals("thrown exception type", HttpErrorResponseException.class, e.getClass());

            HttpErrorResponseException httpErrorResponseException = (HttpErrorResponseException) e;
            Assert.assertEquals("httpErrorResponseException responseMessage", expectedErrorMessage, httpErrorResponseException.getResponseMessage());
            Assert.assertEquals("httpErrorResponseException statusCode", expectedStatusCode, httpErrorResponseException.getStatusCode());
            Assert.assertEquals("httpErrorResponseException statusDescription", expectedReasonPhrase, httpErrorResponseException.getStatusDescription());
            Assert.assertEquals("httpErrorResponseException message", "Failed to add storage files", httpErrorResponseException.getMessage());
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

        CloseableHttpResponse httpResponse = new MockCloseableHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, expectedStatusCode, expectedReasonPhrase));
        httpResponse.setEntity(new StringEntity(requestContent));
        try
        {
            dataBridgeWebClient.getBusinessObjectDataStorageFilesCreateResponse(httpResponse);
            Assert.fail("expected HttpErrorResponseException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", HttpErrorResponseException.class, e.getClass());

            HttpErrorResponseException httpErrorResponseException = (HttpErrorResponseException) e;
            Assert.assertEquals("httpErrorResponseException responseMessage", expectedErrorMessage, httpErrorResponseException.getResponseMessage());
            Assert.assertEquals("httpErrorResponseException statusCode", expectedStatusCode, httpErrorResponseException.getStatusCode());
            Assert.assertEquals("httpErrorResponseException statusDescription", expectedReasonPhrase, httpErrorResponseException.getStatusDescription());
            Assert.assertEquals("httpErrorResponseException message", "Failed to add storage files", httpErrorResponseException.getMessage());
        }
    }

    /**
     * Calls registerBusinessObjectData() method and makes assertions.
     *
     * @param attributes a map of business object data attributes
     * @param useSsl specifies whether to use SSL or not
     *
     * @throws IOException
     * @throws JAXBException
     * @throws URISyntaxException
     */
    private void testPreRegisterBusinessObjectData(HashMap<String, String> attributes, boolean useSsl) throws IOException, JAXBException, URISyntaxException
    {
        dataBridgeWebClient.regServerAccessParamsDto.setUseSsl(useSsl);

        UploaderInputManifestDto manifest = getUploaderInputManifestDto();
        manifest.setAttributes(attributes);

        String storageName = "testStorage";
        Boolean createNewVersion = false;
        BusinessObjectData businessObjectData = dataBridgeWebClient.preRegisterBusinessObjectData(manifest, storageName, createNewVersion);
        Assert.assertNotNull("businessObjectData", businessObjectData);
    }

    /**
     * Calls getS3KeyPrefix() method and makes assertions.
     *
     * @param namespace the namespace
     * @param subPartitionValues the list of sub-partition values
     * @param businessObjectDataVersion the version of the business object data, may be null
     * @param useSsl specifies whether to use SSL or not
     *
     * @throws Exception
     */
    private void testGetS3KeyPrefix(String namespace, List<String> subPartitionValues, Integer businessObjectDataVersion, boolean useSsl) throws Exception
    {
        dataBridgeWebClient.regServerAccessParamsDto.setUseSsl(useSsl);

        DataBridgeBaseManifestDto manifest = getUploaderInputManifestDto();
        manifest.setNamespace(namespace);
        manifest.setSubPartitionValues(subPartitionValues);

        Boolean createNewVersion = false;
        S3KeyPrefixInformation s3KeyPrefix = dataBridgeWebClient.getS3KeyPrefix(manifest, businessObjectDataVersion, createNewVersion);
        Assert.assertNotNull("s3KeyPrefix is null", s3KeyPrefix);
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
     * Calls getStorage() method and makes assertions.
     *
     * @param useSsl specifies whether to use SSL or not
     *
     * @throws IOException
     * @throws JAXBException
     * @throws URISyntaxException
     */
    private void testGetStorage(boolean useSsl) throws IOException, JAXBException, URISyntaxException
    {
        dataBridgeWebClient.regServerAccessParamsDto.setUseSsl(useSsl);

        String expectedStorageName = "testStorage";
        Storage storage = dataBridgeWebClient.getStorage(expectedStorageName);
        Assert.assertNotNull("storage is null", storage);
        Assert.assertEquals("storage name", expectedStorageName, storage.getName());
    }
}
