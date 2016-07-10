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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.api.xml.BusinessObjectDataUploadCredential;
import org.finra.herd.model.api.xml.S3KeyPrefixInformation;
import org.finra.herd.model.dto.DataBridgeBaseManifestDto;
import org.finra.herd.model.dto.RegServerAccessParamsDto;

/**
 * Unit tests for UploaderWebClient class.
 */
public class UploaderWebClientTest extends AbstractUploaderTest
{
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
        assertTrue(resultRegServerAccessParamsDto.getUseSsl());
        assertEquals(WEB_SERVICE_HTTPS_USERNAME, resultRegServerAccessParamsDto.getUsername());
        assertEquals(WEB_SERVICE_HTTPS_PASSWORD, resultRegServerAccessParamsDto.getPassword());
    }

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
        Boolean createNewVersion = false;
        BusinessObjectDataUploadCredential businessObjectDataUploadCredential =
            uploaderWebClient.getBusinessObjectDataUploadCredential(manifest, storageName, createNewVersion);
        Assert.assertNotNull(businessObjectDataUploadCredential);
        AwsCredential awsCredential = businessObjectDataUploadCredential.getAwsCredential();
        Assert.assertNotNull(awsCredential);
        Assert.assertEquals("https://testWebServiceHostname:1234/herd-app/rest/businessObjectData/upload/credential/namespaces/test1" +
            "/businessObjectDefinitionNames/test2/businessObjectFormatUsages/test3/businessObjectFormatFileTypes/test4/businessObjectFormatVersions/test5" +
            "/partitionValues/test6?storageName=test8&subPartitionValues=test7%7Ctest8&createNewVersion=false", awsCredential.getAwsAccessKey());
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
        Boolean createNewVersion = null;
        uploaderWebClient.getRegServerAccessParamsDto().setUseSsl(true);
        BusinessObjectDataUploadCredential businessObjectDataUploadCredential =
            uploaderWebClient.getBusinessObjectDataUploadCredential(manifest, storageName, createNewVersion);
        Assert.assertNotNull(businessObjectDataUploadCredential);
        AwsCredential awsCredential = businessObjectDataUploadCredential.getAwsCredential();
        Assert.assertNotNull(awsCredential);
        Assert.assertEquals("https://testWebServiceHostname:1234/herd-app/rest/businessObjectData/upload/credential/namespaces/test1" +
            "/businessObjectDefinitionNames/test2/businessObjectFormatUsages/test3/businessObjectFormatFileTypes/test4/businessObjectFormatVersions/test5" +
            "/partitionValues/test6?storageName=test8", awsCredential.getAwsAccessKey());
    }

    /**
     * Validates actualBusinessObjectData contents against specified arguments and expected (hard coded) test values.
     *
     * @param expectedS3KeyPrefix the expected S3 key prefix value
     * @param actualS3KeyPrefixInformation the S3KeyPrefixInformation object instance to be validated
     */
    private void assertS3KeyPrefixInformation(String expectedS3KeyPrefix, S3KeyPrefixInformation actualS3KeyPrefixInformation)
    {
        assertNotNull(actualS3KeyPrefixInformation);
        assertEquals(expectedS3KeyPrefix, actualS3KeyPrefixInformation.getS3KeyPrefix());
    }
}
