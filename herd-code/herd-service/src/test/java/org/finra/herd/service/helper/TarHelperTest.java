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
package org.finra.herd.service.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the TarHelper class.
 */
public class TarHelperTest extends AbstractServiceTest
{
    @Before
    public void setup() throws IOException
    {
        // Create a local temp directory.
        localTempPath = Files.createTempDirectory(null);
    }

    @After
    public void cleanup() throws IOException
    {
        FileUtils.deleteDirectory(localTempPath.toFile());
    }

    @Test
    public void testValidateTarFileSize() throws IOException
    {
        // Create a test file.
        File testFile = createLocalFile(localTempPath.toString(), FILE_NAME, FILE_SIZE_1_KB);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Validate the test file size.
        tarHelper.validateTarFileSize(testFile, FILE_SIZE_1_KB, STORAGE_NAME, businessObjectDataKey);

        // Try to validate the test file size when it is less than the total size of storage files.
        try
        {
            tarHelper.validateTarFileSize(testFile, FILE_SIZE_2_KB, STORAGE_NAME, businessObjectDataKey);
            fail("Should throw an IllegalStateException when the test file size is less than the total size of storage files.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("The \"%s\" TAR archive file size (%d bytes) is less than the total size of registered storage files (%d bytes). " +
                "Storage: {%s}, business object data: {%s}", testFile.getPath(), FILE_SIZE_1_KB, FILE_SIZE_2_KB, STORAGE_NAME,
                getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }
}
