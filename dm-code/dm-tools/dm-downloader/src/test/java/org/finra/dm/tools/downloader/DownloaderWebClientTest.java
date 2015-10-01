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
package org.finra.dm.tools.downloader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.finra.dm.core.Command;
import org.finra.dm.model.dto.DmRegServerAccessParamsDto;
import org.finra.dm.model.dto.DownloaderInputManifestDto;
import org.finra.dm.model.dto.UploaderInputManifestDto;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.api.xml.BusinessObjectData;
import org.finra.dm.model.api.xml.S3KeyPrefixInformation;
import org.finra.dm.tools.common.databridge.DataBridgeWebClient;

/**
 * Unit tests for DownloaderWebClient class.
 */
public class DownloaderWebClientTest extends AbstractDownloaderTest
{
    @Test
    public void testWebClientDmRegServerAccessParamsDtoSetterAndGetter()
    {
        // Create and initialize an instance of DmRegServerAccessParamsDto.
        DmRegServerAccessParamsDto dmRegServerAccessParamsDto = new DmRegServerAccessParamsDto();
        dmRegServerAccessParamsDto.setDmRegServerHost(WEB_SERVICE_HOSTNAME);
        dmRegServerAccessParamsDto.setDmRegServerPort(WEB_SERVICE_HTTPS_PORT);
        dmRegServerAccessParamsDto.setUseSsl(true);
        dmRegServerAccessParamsDto.setUsername(WEB_SERVICE_HTTPS_USERNAME);
        dmRegServerAccessParamsDto.setPassword(WEB_SERVICE_HTTPS_PASSWORD);

        // Set the DTO.
        downloaderWebClient.setDmRegServerAccessParamsDto(dmRegServerAccessParamsDto);

        // Retrieve the DTO and validate the results.
        DmRegServerAccessParamsDto resultDmRegServerAccessParamsDto = downloaderWebClient.getDmRegServerAccessParamsDto();

        // validate the results.
        assertEquals(WEB_SERVICE_HOSTNAME, resultDmRegServerAccessParamsDto.getDmRegServerHost());
        assertEquals(WEB_SERVICE_HTTPS_PORT, resultDmRegServerAccessParamsDto.getDmRegServerPort());
        assertTrue(resultDmRegServerAccessParamsDto.getUseSsl());
        assertEquals(WEB_SERVICE_HTTPS_USERNAME, resultDmRegServerAccessParamsDto.getUsername());
        assertEquals(WEB_SERVICE_HTTPS_PASSWORD, resultDmRegServerAccessParamsDto.getPassword());
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
                downloaderWebClient.registerBusinessObjectData(uploaderInputManifestDto, getTestS3FileTransferRequestParamsDto(S3_TEST_PATH_V0 + "/"),
                    StorageEntity.MANAGED_STORAGE, false);
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
                downloaderWebClient.registerBusinessObjectData(uploaderInputManifestDto, getTestS3FileTransferRequestParamsDto(S3_TEST_PATH_V0 + "/"),
                    StorageEntity.MANAGED_STORAGE, false);
            }
        });

        // Get business object data information.
        DownloaderInputManifestDto downloaderInputManifestDto = getTestDownloaderInputManifestDto();
        BusinessObjectData resultBusinessObjectData = downloaderWebClient.getBusinessObjectData(downloaderInputManifestDto);

        // Validate the results.
        assertNotNull(resultBusinessObjectData);
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
        return businessObjectData;
    }
}
