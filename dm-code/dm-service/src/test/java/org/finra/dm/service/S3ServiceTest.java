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
package org.finra.dm.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.finra.dm.core.Command;
import org.finra.dm.model.dto.S3FileCopyRequestParamsDto;
import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.dto.S3FileTransferResultsDto;
import org.finra.dm.model.api.xml.StorageFile;
import org.finra.dm.service.impl.S3ServiceImpl;

/**
 * This class tests functionality within the S3Service.
 */
public class S3ServiceTest extends AbstractServiceTest
{
    /**
     * Initialize the environment. This method is run once before any of the test methods in the class.
     */
    @BeforeClass
    public static void initEnv() throws IOException
    {
        localTempPath = Paths.get(System.getProperty("java.io.tmpdir"), "dm-s3-service-test-local-folder");
    }

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        // Create local temp directory.
        localTempPath.toFile().mkdir();
    }

    /**
     * Cleans up the local temp directory and S3 test path that we are using.
     */
    @After
    public void cleanEnv() throws IOException
    {
        // Clean up the local directory.
        FileUtils.deleteDirectory(localTempPath.toFile());

        // Clean up the destination S3 folder.
        s3Dao.deleteDirectory(getTestS3FileTransferRequestParamsDto());
    }

    @Test
    public void testUploadFile() throws IOException, InterruptedException
    {
        // Create local test file.
        File targetFile = createLocalFile(localTempPath.toString(), LOCAL_FILE, FILE_SIZE_1_KB);
        assertTrue(targetFile.isFile());
        assertTrue(targetFile.length() == FILE_SIZE_1_KB);

        // Upload test file to S3.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3FileTransferRequestParamsDto.setLocalPath(targetFile.getPath());
        S3FileTransferResultsDto results = s3Service.uploadFile(s3FileTransferRequestParamsDto);

        // Validate results.
        assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate the file upload.
        validateS3FileUpload(s3FileTransferRequestParamsDto, Arrays.asList(TARGET_S3_KEY));
    }

    @Test
    public void testUploadFileList() throws IOException, InterruptedException
    {
        // Create local test files.
        for (String file : LOCAL_FILES)
        {
            createLocalFile(localTempPath.toString(), file, FILE_SIZE_1_KB);
        }

        // Create a list of files to be uploaded along with the list of expected S3 key values.
        List<File> requestFileList = new ArrayList<>();
        List<String> expectedKeys = new ArrayList<>();
        for (String file : LOCAL_FILES_SUBSET)
        {
            requestFileList.add(Paths.get(localTempPath.toString(), file).toFile());
            expectedKeys.add(TEST_S3_KEY_PREFIX + "/" + file.replaceAll("\\\\", "/"));
        }

        // Upload test file to S3.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX);
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setFiles(requestFileList);
        S3FileTransferResultsDto results = s3Service.uploadFileList(s3FileTransferRequestParamsDto);

        // Validate results.
        assertTrue(results.getTotalFilesTransferred() == LOCAL_FILES_SUBSET.size());

        // Validate the upload.
        validateS3FileUpload(s3FileTransferRequestParamsDto, expectedKeys);
    }

    @Test
    public void testCopyFile() throws InterruptedException
    {
        // Copy an S3 file from source to target S3 bucket.
        S3FileCopyRequestParamsDto params = new S3FileCopyRequestParamsDto();
        params.setKmsKeyId("AWS_KMS_EXTERNAL_KEY_ID");
        params.setSourceBucketName("S3_MANAGED_LOADING_DOCK_BUCKET_NAME");
        params.setTargetBucketName("S3_MANAGED_EXTERNAL_BUCKET_NAME");
        params.setS3KeyPrefix(TARGET_S3_KEY);
        S3FileTransferResultsDto results = s3Service.copyFile(params);

        // Validate the results.
        assertNotNull(results);
        assertEquals(Long.valueOf(1L), results.getTotalFilesTransferred());
        assertEquals(Long.valueOf(FILE_SIZE_1_KB), results.getTotalBytesTransferred());
    }

    @Test
    public void deleteFile()
    {
        // Delete an S3 file from an S3 bucket.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName("S3_MANAGED_LOADING_DOCK_BUCKET_NAME");
        params.setS3KeyPrefix(TARGET_S3_KEY);
        s3Service.deleteFile(params);
    }

    @Test
    public void testUploadDirectory() throws IOException, InterruptedException
    {
        // Create local test files.
        for (String file : LOCAL_FILES)
        {
            createLocalFile(localTempPath.toString(), file, FILE_SIZE_1_KB);
        }

        // Upload test file to S3.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX);
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setRecursive(true);
        S3FileTransferResultsDto results = s3Service.uploadDirectory(s3FileTransferRequestParamsDto);

        // Validate results.
        assertTrue(results.getTotalFilesTransferred() == LOCAL_FILES.size());

        // Build a list of expected S3 key values.
        List<String> expectedKeys = new ArrayList<>();
        for (String file : LOCAL_FILES)
        {
            expectedKeys.add(TEST_S3_KEY_PREFIX + "/" + file.replaceAll("\\\\", "/"));
        }

        // Validate the file upload.
        validateS3FileUpload(s3FileTransferRequestParamsDto, expectedKeys);
    }

    @Test
    public void testDeleteDirectory() throws Exception
    {
        // Upload local directory to S3.
        testUploadDirectory();

        // Delete directory from S3.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        s3Service.deleteDirectory(s3FileTransferRequestParamsDto);

        // Validate that S3 directory got deleted.
        List<StorageFile> actualS3Files = s3Service.listDirectory(s3FileTransferRequestParamsDto);
        assertTrue(actualS3Files.size() == 0);
    }

    @Test
    public void testDeleteDirectoryIgnoreException() throws Exception
    {
        // Upload local directory to S3.
        testUploadDirectory();

        // Delete directory from S3.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        s3Service.deleteDirectoryIgnoreException(s3FileTransferRequestParamsDto);

        // Validate that S3 directory got deleted.
        List<StorageFile> actualS3Files = s3Service.listDirectory(s3FileTransferRequestParamsDto);
        assertTrue(actualS3Files.size() == 0);
    }

    @Test
    public void testDeleteDirectoryIgnoreExceptionWithException() throws Exception
    {
        // Try to delete S3 directory using an invalid S3 bucket name.
        final S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName("INVALID_BUCKET_NAME");
        executeWithoutLogging(S3ServiceImpl.class, new Command()
        {
            @Override
            public void execute() throws Exception
            {
                s3Service.deleteDirectoryIgnoreException(s3FileTransferRequestParamsDto);
            }
        });
    }

    @Test
    public void testDownloadFile() throws IOException, InterruptedException
    {
        // Upload local file to S3.
        testUploadFile();

        // Clean up the local directory, so we can test the download.
        FileUtils.deleteDirectory(localTempPath.toFile());

        // Create local temp directory - this also validates that clean up was really executed.
        assertTrue(localTempPath.toFile().mkdir());

        // Destination local file.
        File destinationLocalFile = Paths.get(localTempPath.toString(), LOCAL_FILE).toFile();

        // Execute download.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3FileTransferRequestParamsDto.setLocalPath(destinationLocalFile.getPath());
        S3FileTransferResultsDto results = s3Service.downloadFile(s3FileTransferRequestParamsDto);

        // Validate results.
        assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate that we have the file downloaded from S3.
        assertTrue(destinationLocalFile.isFile());
    }

    @Test
    public void testDownloadDirectory() throws IOException, InterruptedException
    {
        // Upload local directory to S3.
        testUploadDirectory();

        // Clean up the local directory, so we can test the download.
        FileUtils.deleteDirectory(localTempPath.toFile());

        // Create local temp directory - this also validates that clean up was really executed.
        assertTrue(localTempPath.toFile().mkdir());

        // Execute download.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX);
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setRecursive(true);
        S3FileTransferResultsDto results = s3Service.downloadDirectory(s3FileTransferRequestParamsDto);

        // Validate results.
        assertTrue(results.getTotalFilesTransferred() == LOCAL_FILES.size());

        // Validate that we have the directory downloaded from S3.
        for (String file : LOCAL_FILES)
        {
            assertTrue(Paths.get(localTempPath.toString(), TEST_S3_KEY_PREFIX, file).toFile().isFile());
        }
    }
}
