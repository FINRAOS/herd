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
package org.finra.herd.tools.uploader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.Test;

import org.finra.herd.dao.impl.MockHttpClientOperationsImpl;
import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataUploadCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataVersions;
import org.finra.herd.model.dto.DataBridgeBaseManifestDto;
import org.finra.herd.model.dto.RegServerAccessParamsDto;

/**
 * Unit tests for UploaderWebClient class.
 */
public class UploaderWebClientTest extends AbstractUploaderTest
{
    @Test
    public void testGetBusinessObjectDataUploadCredential1() throws Exception
    {
        DataBridgeBaseManifestDto manifest = new DataBridgeBaseManifestDto();
        manifest.setNamespace("test1");
        manifest.setBusinessObjectDefinitionName("test2");
        manifest.setBusinessObjectFormatUsage("test3");
        manifest.setBusinessObjectFormatFileType("test4");
        manifest.setBusinessObjectFormatVersion("test5");
        manifest.setPartitionValue("test6");
        manifest.setSubPartitionValues(Arrays.asList("test7", "test8"));
        String storageName = "test8";
        Integer businessObjectDataVersion = 1234;
        Boolean createNewVersion = false;
        uploaderWebClient.getRegServerAccessParamsDto().setUseSsl(false);
        BusinessObjectDataUploadCredential businessObjectDataUploadCredential =
            uploaderWebClient.getBusinessObjectDataUploadCredential(manifest, storageName, businessObjectDataVersion, createNewVersion);
        Assert.assertNotNull(businessObjectDataUploadCredential);
        AwsCredential awsCredential = businessObjectDataUploadCredential.getAwsCredential();
        Assert.assertNotNull(awsCredential);
        Assert.assertEquals("http://testWebServiceHostname:1234/herd-app/rest/businessObjectData/upload/credential/namespaces/test1" +
                "/businessObjectDefinitionNames/test2/businessObjectFormatUsages/test3/businessObjectFormatFileTypes/test4/businessObjectFormatVersions/test5" +
                "/partitionValues/test6?storageName=test8&subPartitionValues=test7%7Ctest8&businessObjectDataVersion=1234&createNewVersion=false",
            awsCredential.getAwsAccessKey());
    }

    @Test
    public void testGetBusinessObjectDataUploadCredential2() throws Exception
    {
        DataBridgeBaseManifestDto manifest = new DataBridgeBaseManifestDto();
        manifest.setNamespace("test1");
        manifest.setBusinessObjectDefinitionName("test2");
        manifest.setBusinessObjectFormatUsage("test3");
        manifest.setBusinessObjectFormatFileType("test4");
        manifest.setBusinessObjectFormatVersion("test5");
        manifest.setPartitionValue("test6");
        String storageName = "test8";
        Integer businessObjectDataVersion = 1234;
        Boolean createNewVersion = null;
        uploaderWebClient.getRegServerAccessParamsDto().setUseSsl(true);
        BusinessObjectDataUploadCredential businessObjectDataUploadCredential =
            uploaderWebClient.getBusinessObjectDataUploadCredential(manifest, storageName, businessObjectDataVersion, createNewVersion);
        Assert.assertNotNull(businessObjectDataUploadCredential);
        AwsCredential awsCredential = businessObjectDataUploadCredential.getAwsCredential();
        Assert.assertNotNull(awsCredential);
        Assert.assertEquals("https://testWebServiceHostname:1234/herd-app/rest/businessObjectData/upload/credential/namespaces/test1" +
            "/businessObjectDefinitionNames/test2/businessObjectFormatUsages/test3/businessObjectFormatFileTypes/test4/businessObjectFormatVersions/test5" +
            "/partitionValues/test6?storageName=test8&businessObjectDataVersion=1234", awsCredential.getAwsAccessKey());
    }

    @Test
    public void testGetBusinessObjectDataUploadCredential3() throws Exception
    {
        DataBridgeBaseManifestDto manifest = new DataBridgeBaseManifestDto();
        manifest.setNamespace("test1");
        manifest.setBusinessObjectDefinitionName("test2");
        manifest.setBusinessObjectFormatUsage("test3");
        manifest.setBusinessObjectFormatFileType("test4");
        manifest.setBusinessObjectFormatVersion("test5");
        manifest.setPartitionValue("test6");
        manifest.setSubPartitionValues(Arrays.asList("test7", "test8"));
        String storageName = "test8";
        Integer businessObjectDataVersion = null;
        Boolean createNewVersion = true;
        uploaderWebClient.getRegServerAccessParamsDto().setUseSsl(true);
        BusinessObjectDataUploadCredential businessObjectDataUploadCredential =
            uploaderWebClient.getBusinessObjectDataUploadCredential(manifest, storageName, businessObjectDataVersion, createNewVersion);
        Assert.assertNotNull(businessObjectDataUploadCredential);
        AwsCredential awsCredential = businessObjectDataUploadCredential.getAwsCredential();
        Assert.assertNotNull(awsCredential);
        Assert.assertEquals("https://testWebServiceHostname:1234/herd-app/rest/businessObjectData/upload/credential/namespaces/test1" +
            "/businessObjectDefinitionNames/test2/businessObjectFormatUsages/test3/businessObjectFormatFileTypes/test4/businessObjectFormatVersions/test5" +
            "/partitionValues/test6?storageName=test8&subPartitionValues=test7%7Ctest8&createNewVersion=true", awsCredential.getAwsAccessKey());
    }

    @Test
    public void testGetBusinessObjectDataVersions() throws Exception
    {
        testGetBusinessObjectDataVersions(null, null, null, false);
    }

    @Test
    public void testGetBusinessObjectDataVersionsUseSsl() throws Exception
    {
        testGetBusinessObjectDataVersions(null, null, null, true);
    }

    @Test
    public void testGetBusinessObjectDataVersionsWithBusinessObjectDataVersion() throws Exception
    {
        testGetBusinessObjectDataVersions(null, null, 5678, true);
    }

    @Test
    public void testGetBusinessObjectDataVersionsWithBusinessObjectFormatVersion() throws Exception
    {
        testGetBusinessObjectDataVersions(null, 1234, null, true);
    }

    @Test
    public void testGetBusinessObjectDataVersionsWithSubPartitions() throws Exception
    {
        testGetBusinessObjectDataVersions(Arrays.asList("testSubPartitionValue1", "testSubPartitionValue2", "testSubPartitionValue3", "testSubPartitionValue4"),
            null, null, true);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusIgnoreException() throws Exception
    {
        uploaderWebClient.getRegServerAccessParamsDto().setRegServerHost(MockHttpClientOperationsImpl.HOSTNAME_THROW_IO_EXCEPTION_DURING_UPDATE_BDATA_STATUS);

        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        String businessObjectDataStatus = "testBusinessObjectDataStatus";

        executeWithoutLogging(UploaderWebClient.class, () -> {
            uploaderWebClient.updateBusinessObjectDataStatusIgnoreException(businessObjectDataKey, businessObjectDataStatus);
        });
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
        uploaderWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);

        // Retrieve the DTO and validate the results.
        RegServerAccessParamsDto resultRegServerAccessParamsDto = uploaderWebClient.getRegServerAccessParamsDto();

        // validate the results.
        assertEquals(WEB_SERVICE_HOSTNAME, resultRegServerAccessParamsDto.getRegServerHost());
        assertEquals(WEB_SERVICE_HTTPS_PORT, resultRegServerAccessParamsDto.getRegServerPort());
        assertTrue(resultRegServerAccessParamsDto.isUseSsl());
        assertEquals(WEB_SERVICE_HTTPS_USERNAME, resultRegServerAccessParamsDto.getUsername());
        assertEquals(WEB_SERVICE_HTTPS_PASSWORD, resultRegServerAccessParamsDto.getPassword());
    }

    /**
     * Calls getBusinessObjectDataVersions() method and makes assertions.
     *
     * @param subPartitionValues the list of sub-partition values
     * @param businessObjectFormatVersion the business object format version
     * @param businessObjectDataVersion the business object data version
     * @param useSsl specifies whether to use SSL or not
     *
     * @throws JAXBException if a JAXB error was encountered
     * @throws IOException if an I/O error was encountered
     * @throws URISyntaxException if a URI syntax error was encountered
     * @throws KeyStoreException if a key store exception occurs
     * @throws NoSuchAlgorithmException if a no such algorithm exception occurs
     * @throws KeyManagementException if key management exception
     */
    private void testGetBusinessObjectDataVersions(List<String> subPartitionValues, Integer businessObjectFormatVersion, Integer businessObjectDataVersion,
        boolean useSsl) throws IOException, JAXBException, URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        uploaderWebClient.getRegServerAccessParamsDto().setUseSsl(useSsl);

        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectDataKey.setSubPartitionValues(subPartitionValues);
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataVersion);

        BusinessObjectDataVersions businessObjectDataVersions = uploaderWebClient.getBusinessObjectDataVersions(businessObjectDataKey);

        assertNotNull("businessObjectDataVersions", businessObjectDataVersions);
    }
}
