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
package org.finra.dm.tools.uploader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.finra.dm.model.dto.DmRegServerAccessParamsDto;
import org.finra.dm.model.dto.UploaderInputManifestDto;
import org.finra.dm.model.api.xml.S3KeyPrefixInformation;

/**
 * Unit tests for UploaderWebClient class.
 */
public class UploaderWebClientTest extends AbstractUploaderTest
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
        uploaderWebClient.setDmRegServerAccessParamsDto(dmRegServerAccessParamsDto);

        // Retrieve the DTO and validate the results.
        DmRegServerAccessParamsDto resultDmRegServerAccessParamsDto = uploaderWebClient.getDmRegServerAccessParamsDto();

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
        // Get an S3 key prefix for the initial version of the business object data.
        UploaderInputManifestDto uploaderInputManifestDto = getTestUploaderInputManifestDto();
        S3KeyPrefixInformation resultPathInfo = uploaderWebClient.getS3KeyPrefix(uploaderInputManifestDto, false);

        // Validate the results.
        assertS3KeyPrefixInformation(S3_SIMPLE_TEST_PATH, resultPathInfo);
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
