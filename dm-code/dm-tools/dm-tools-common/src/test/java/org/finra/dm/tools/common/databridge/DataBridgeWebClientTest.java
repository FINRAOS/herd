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
package org.finra.dm.tools.common.databridge;

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

import org.finra.dm.dao.HttpClientOperations;
import org.finra.dm.dao.helper.DmStringHelper;
import org.finra.dm.dao.helper.XmlHelper;
import org.finra.dm.dao.impl.MockCloseableHttpResponse;
import org.finra.dm.model.dto.DataBridgeBaseManifestDto;
import org.finra.dm.model.dto.DmRegServerAccessParamsDto;
import org.finra.dm.model.dto.ManifestFile;
import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.dto.UploaderInputManifestDto;
import org.finra.dm.model.api.xml.BusinessObjectData;
import org.finra.dm.model.api.xml.ErrorInformation;
import org.finra.dm.model.api.xml.S3KeyPrefixInformation;
import org.finra.dm.model.api.xml.Storage;

public class DataBridgeWebClientTest extends AbstractDataBridgeTest
{
    private DataBridgeWebClient dataBridgeWebClient;

    @Autowired
    private HttpClientOperations httpClientOperations;

    @Autowired
    private XmlHelper xmlHelper;

    @Autowired
    private DmStringHelper dmStringHelper;

    @Before
    public void before()
    {
        dataBridgeWebClient = new DataBridgeWebClient()
        {

        };

        DmRegServerAccessParamsDto dmRegServerAccessParamsDto = new DmRegServerAccessParamsDto();
        dmRegServerAccessParamsDto.setUseSsl(false);
        dmRegServerAccessParamsDto.setDmRegServerPort(8080);
        dataBridgeWebClient.setDmRegServerAccessParamsDto(dmRegServerAccessParamsDto);

        dataBridgeWebClient.httpClientOperations = httpClientOperations;
        dataBridgeWebClient.dmStringHelper = dmStringHelper;
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
    public void testRegisterBusinessObjectData() throws Exception
    {
        HashMap<String, String> attributes = new HashMap<String, String>();
        attributes.put("testAttributeName", "testAttributeValue");
        testRegisterBusinessObjectData(attributes, false);
    }

    @Test
    public void testRegisterBusinessObjectDataUseSsl() throws Exception
    {
        testRegisterBusinessObjectData(new HashMap<String, String>(), true);
    }

    @Test
    public void testRegisterBusinessObjectDataAttributesNull() throws Exception
    {
        testRegisterBusinessObjectData(null, true);
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
    public void testGetBusinessObjectData200ValidResponse() throws Exception
    {
        CloseableHttpResponse httpResponse = new MockCloseableHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "testReasonPhrase"));
        httpResponse.setEntity(new StringEntity(xmlHelper.objectToXml(new BusinessObjectData())));
        String actionDescription = "testActionDescription";
        BusinessObjectData businessObjectData = dataBridgeWebClient.getBusinessObjectData(httpResponse, actionDescription);
        Assert.assertNotNull("businessObjectData", businessObjectData);
    }

    @Test
    public void testGetBusinessObjectData200BadContentReturnsNull() throws Exception
    {
        CloseableHttpResponse httpResponse = new MockCloseableHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "testReasonPhrase"));
        httpResponse.setEntity(new StringEntity("invalid xml"));
        String actionDescription = "testActionDescription";
        BusinessObjectData businessObjectData = dataBridgeWebClient.getBusinessObjectData(httpResponse, actionDescription);
        Assert.assertNull("businessObjectData", businessObjectData);
    }

    @Test
    public void testGetBusinessObjectData400BadContentThrows() throws Exception
    {
        int expectedStatusCode = 400;
        String expectedReasonPhrase = "testReasonPhrase";
        String expectedErrorMessage = "invalid xml";

        CloseableHttpResponse httpResponse = new MockCloseableHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, expectedStatusCode, expectedReasonPhrase));
        httpResponse.setEntity(new StringEntity(expectedErrorMessage));
        String actionDescription = "testActionDescription";
        try
        {
            dataBridgeWebClient.getBusinessObjectData(httpResponse, actionDescription);
            Assert.fail("expected HttpErrorResponseException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", HttpErrorResponseException.class, e.getClass());

            HttpErrorResponseException httpErrorResponseException = (HttpErrorResponseException) e;
            Assert.assertEquals("httpErrorResponseException responseMessage", expectedErrorMessage, httpErrorResponseException.getResponseMessage());
            Assert.assertEquals("httpErrorResponseException statusCode", expectedStatusCode, httpErrorResponseException.getStatusCode());
            Assert.assertEquals("httpErrorResponseException statusDescription", expectedReasonPhrase, httpErrorResponseException.getStatusDescription());
            Assert.assertEquals("httpErrorResponseException message", "Failed to " + actionDescription, httpErrorResponseException.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectData400Throws() throws Exception
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
        String actionDescription = "testActionDescription";
        try
        {
            dataBridgeWebClient.getBusinessObjectData(httpResponse, actionDescription);
            Assert.fail("expected HttpErrorResponseException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", HttpErrorResponseException.class, e.getClass());

            HttpErrorResponseException httpErrorResponseException = (HttpErrorResponseException) e;
            Assert.assertEquals("httpErrorResponseException responseMessage", expectedErrorMessage, httpErrorResponseException.getResponseMessage());
            Assert.assertEquals("httpErrorResponseException statusCode", expectedStatusCode, httpErrorResponseException.getStatusCode());
            Assert.assertEquals("httpErrorResponseException statusDescription", expectedReasonPhrase, httpErrorResponseException.getStatusDescription());
            Assert.assertEquals("httpErrorResponseException message", "Failed to " + actionDescription, httpErrorResponseException.getMessage());
        }
    }

    /**
     * @param attributes
     * @param useSsl
     *
     * @throws IOException
     * @throws JAXBException
     * @throws URISyntaxException
     */
    private void testRegisterBusinessObjectData(HashMap<String, String> attributes, boolean useSsl) throws IOException, JAXBException, URISyntaxException
    {
        dataBridgeWebClient.dmRegServerAccessParamsDto.setUseSsl(useSsl);

        UploaderInputManifestDto manifest = getUploaderInputManifestDto();
        manifest.setAttributes(attributes);

        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        String storageName = "testStorage";
        Boolean createNewVersion = false;
        BusinessObjectData businessObjectData =
            dataBridgeWebClient.registerBusinessObjectData(manifest, s3FileTransferRequestParamsDto, storageName, createNewVersion);
        Assert.assertNull("businessObjectData", businessObjectData);
    }

    /**
     * @param namespace
     * @param subPartitionValues
     * @param businessObjectDataVersion
     * @param useSsl
     *
     * @throws Exception
     */
    private void testGetS3KeyPrefix(String namespace, List<String> subPartitionValues, Integer businessObjectDataVersion, boolean useSsl) throws Exception
    {
        dataBridgeWebClient.dmRegServerAccessParamsDto.setUseSsl(useSsl);

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
     * @return
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
     * @param useSsl
     *
     * @throws IOException
     * @throws JAXBException
     * @throws URISyntaxException
     */
    private void testGetStorage(boolean useSsl) throws IOException, JAXBException, URISyntaxException
    {
        dataBridgeWebClient.dmRegServerAccessParamsDto.setUseSsl(useSsl);

        String expectedStorageName = "testStorage";
        Storage storage = dataBridgeWebClient.getStorage(expectedStorageName);
        Assert.assertNotNull("storage is null", storage);
        Assert.assertEquals("storage name", expectedStorageName, storage.getName());
    }
}
