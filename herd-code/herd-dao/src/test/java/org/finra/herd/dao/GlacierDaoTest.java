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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.dao.impl.MockGlacierOperationsImpl;
import org.finra.herd.model.dto.GlacierArchiveTransferRequestParamsDto;
import org.finra.herd.model.dto.GlacierArchiveTransferResultsDto;

/**
 * This class tests various functionality within the S3Dao class.
 */
public class GlacierDaoTest extends AbstractDaoTest
{
    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        // Create a local temp directory.
        localTempPath = Files.createTempDirectory(null);
    }

    /**
     * Cleans up the local temp directory.
     */
    @After
    public void cleanEnv() throws IOException
    {
        // Clean up the local directory.
        FileUtils.deleteDirectory(localTempPath.toFile());
    }

    /**
     * Test that we are able to perform an archive upload GlacierDao operation on AWS Glacier using our DAO tier.
     */
    @Test
    public void testUploadArchive() throws IOException, InterruptedException
    {
        // Create local test file.
        File targetFile = createLocalFile(localTempPath.toString(), LOCAL_FILE, FILE_SIZE_1_KB);
        Assert.assertTrue(targetFile.isFile());
        Assert.assertTrue(targetFile.length() == FILE_SIZE_1_KB);

        // Upload test file to AWS Glacier.
        GlacierArchiveTransferRequestParamsDto glacierArchiveTransferRequestParamsDto = new GlacierArchiveTransferRequestParamsDto();
        glacierArchiveTransferRequestParamsDto.setVaultName(GLACIER_VAULT_NAME);
        glacierArchiveTransferRequestParamsDto.setLocalFilePath(targetFile.getPath());
        GlacierArchiveTransferResultsDto results = glacierDao.uploadArchive(glacierArchiveTransferRequestParamsDto);

        // Validate results.
        assertNotNull(results);
        assertEquals(MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID, results.getArchiveId());
        assertEquals(Long.valueOf(FILE_SIZE_1_KB), results.getTotalBytesTransferred());
    }
}
