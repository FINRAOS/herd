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
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

/**
 * Unit tests for DownloaderManifestReader class.
 */
public class DownloaderManifestReaderTest extends AbstractDownloaderTest
{
    @Test
    public void testReadJsonManifest() throws IOException
    {
        // Create and read a downloader input manifest file.
        DownloaderInputManifestDto downloaderInputManifestDto = getTestDownloaderInputManifestDto();
        DownloaderInputManifestDto resultManifest =
            downloaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), downloaderInputManifestDto));

        // Validate the results.
        assertDownloaderManifest(downloaderInputManifestDto, resultManifest);
    }

    @Test
    public void testReadJsonManifestMissingRequiredParameters() throws IOException
    {
        DownloaderInputManifestDto downloaderInputManifestDto;

        // Try to create and read the downloader input manifest when namespace is not specified.
        downloaderInputManifestDto = getTestDownloaderInputManifestDto();
        downloaderInputManifestDto.setNamespace(BLANK_TEXT);
        try
        {
            downloaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), downloaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest namespace must be specified.", e.getMessage());
        }

        // Try to create and read the downloader input manifest when business object definition name is not specified.
        downloaderInputManifestDto = getTestDownloaderInputManifestDto();
        downloaderInputManifestDto.setBusinessObjectDefinitionName(BLANK_TEXT);
        try
        {
            downloaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), downloaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest business object definition name must be specified.", e.getMessage());
        }

        // Try to create and read the downloader input manifest when business object format usage is not specified.
        downloaderInputManifestDto = getTestDownloaderInputManifestDto();
        downloaderInputManifestDto.setBusinessObjectFormatUsage(BLANK_TEXT);
        try
        {
            downloaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), downloaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest business object format usage must be specified.", e.getMessage());
        }

        // Try to create and read the downloader input manifest when business object format file type is not specified.
        downloaderInputManifestDto = getTestDownloaderInputManifestDto();
        downloaderInputManifestDto.setBusinessObjectFormatFileType(BLANK_TEXT);
        try
        {
            downloaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), downloaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest business object format file type must be specified.", e.getMessage());
        }

        // Try to create and read the downloader input manifest when partition key is not specified.
        downloaderInputManifestDto = getTestDownloaderInputManifestDto();
        downloaderInputManifestDto.setPartitionKey(BLANK_TEXT);
        try
        {
            downloaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), downloaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when partition key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest business object format partition key must be specified.", e.getMessage());
        }

        // Try to create and read the downloader input manifest when partition value is not specified.
        downloaderInputManifestDto = getTestDownloaderInputManifestDto();
        downloaderInputManifestDto.setPartitionValue(BLANK_TEXT);
        try
        {
            downloaderManifestReader.readJsonManifest(createManifestFile(LOCAL_TEMP_PATH_INPUT.toString(), downloaderInputManifestDto));
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Manifest business object data partition value must be specified.", e.getMessage());
        }
    }

    /**
     * Validates downloader input manifest instance.
     *
     * @param expectedDownloaderInputManifest the expected downloader manifest
     * @param actualDownloaderInputManifest the actual instance of downloader input manifest to be validated
     */
    private void assertDownloaderManifest(DownloaderInputManifestDto expectedDownloaderInputManifest, DownloaderInputManifestDto actualDownloaderInputManifest)
    {
        // Validate all fields.
        assertEquals(expectedDownloaderInputManifest.getBusinessObjectDefinitionName(), actualDownloaderInputManifest.getBusinessObjectDefinitionName());
        assertEquals(expectedDownloaderInputManifest.getBusinessObjectFormatUsage(), actualDownloaderInputManifest.getBusinessObjectFormatUsage());
        assertEquals(expectedDownloaderInputManifest.getBusinessObjectFormatFileType(), actualDownloaderInputManifest.getBusinessObjectFormatFileType());
        assertEquals(expectedDownloaderInputManifest.getBusinessObjectFormatVersion(), actualDownloaderInputManifest.getBusinessObjectFormatVersion());
        assertEquals(expectedDownloaderInputManifest.getPartitionKey(), actualDownloaderInputManifest.getPartitionKey());
        assertEquals(expectedDownloaderInputManifest.getPartitionValue(), actualDownloaderInputManifest.getPartitionValue());
        assertEquals(expectedDownloaderInputManifest.getBusinessObjectDataVersion(), actualDownloaderInputManifest.getBusinessObjectDataVersion());
    }
}
