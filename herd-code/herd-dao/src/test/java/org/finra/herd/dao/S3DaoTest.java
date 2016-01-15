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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.Transfer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.fusesource.hawtbuf.ByteArrayInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.dao.impl.MockS3OperationsImpl;
import org.finra.herd.dao.impl.S3DaoImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.S3FileCopyRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferResultsDto;

/**
 * This class tests various functionality within the S3Dao class.
 */
public class S3DaoTest extends AbstractDaoTest
{
    protected static Logger s3DaoImplLogger = Logger.getLogger(S3DaoImpl.class);

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
     * Cleans up the local temp directory and S3 test path that we are using.
     */
    @After
    public void cleanEnv() throws IOException
    {
        // Clean up the local directory.
        FileUtils.deleteDirectory(localTempPath.toFile());

        // Clean up the destination S3 folder.
        s3Dao.deleteDirectory(getTestS3FileTransferRequestParamsDto());

        s3Operations.rollback();
    }

    /**
     * Test S3 file copy without any errors.
     */
    @Test
    public void testCopyFile() throws InterruptedException
    {
        S3FileCopyRequestParamsDto transferDto = new S3FileCopyRequestParamsDto();
        transferDto.setSourceBucketName(getS3LoadingDockBucketName());
        transferDto.setTargetBucketName(getS3ExternalBucketName());
        transferDto.setS3KeyPrefix("testKeyPrefix");
        transferDto.setKmsKeyId(MockS3OperationsImpl.MOCK_KMS_ID);
        S3FileTransferResultsDto resultsDto = s3Dao.copyFile(transferDto);
        assertEquals(Long.valueOf(1L), resultsDto.getTotalFilesTransferred());
    }

    /**
     * Test S3 file copy with an invalid KMS Id. This should throw an AmazonServiceException.
     */
    @Test
    public void testCopyFileInvalidKmsId() throws InterruptedException
    {
        try
        {
            S3FileCopyRequestParamsDto transferDto = new S3FileCopyRequestParamsDto();
            transferDto.setSourceBucketName(getS3LoadingDockBucketName());
            transferDto.setTargetBucketName(getS3ExternalBucketName());
            transferDto.setS3KeyPrefix("testKeyPrefix");
            transferDto.setKmsKeyId(MockS3OperationsImpl.MOCK_KMS_ID_FAILED_TRANSFER);
            s3Dao.copyFile(transferDto);
            fail("An AmazonServiceException was expected but not thrown.");
        }
        catch (AmazonServiceException ex)
        {
            Assert.assertTrue("Invalid AmazonServiceException message returned.",
                ex.getMessage().contains("Key '" + MockS3OperationsImpl.MOCK_KMS_ID_FAILED_TRANSFER + "' does not exist"));
        }
    }

    /**
     * Test S3 file copy with an invalid KMS Id that throws an IllegalStateException because no AmazonServiceException was found.
     */
    @Test
    public void testCopyFileInvalidKmsIdIllegalStateException() throws InterruptedException
    {
        try
        {
            S3FileCopyRequestParamsDto transferDto = new S3FileCopyRequestParamsDto();
            transferDto.setSourceBucketName(getS3LoadingDockBucketName());
            transferDto.setTargetBucketName(getS3ExternalBucketName());
            transferDto.setS3KeyPrefix("testKeyPrefix");
            transferDto.setKmsKeyId(MockS3OperationsImpl.MOCK_KMS_ID_FAILED_TRANSFER_NO_EXCEPTION);
            s3Dao.copyFile(transferDto);
            fail("An IllegalStateException was expected but not thrown.");
        }
        catch (IllegalStateException ex)
        {
            assertEquals("Invalid IllegalStateException message returned.",
                "The transfer operation \"" + MockS3OperationsImpl.MOCK_TRANSFER_DESCRIPTION + "\" failed for an unknown reason.", ex.getMessage());
        }
    }

    /**
     * Test S3 file copy with an invalid KMS Id that will result in a cancelled transfer.
     */
    @Test
    public void testCopyFileInvalidKmsIdCancelled() throws InterruptedException
    {
        try
        {
            S3FileCopyRequestParamsDto transferDto = new S3FileCopyRequestParamsDto();
            transferDto.setSourceBucketName(getS3LoadingDockBucketName());
            transferDto.setTargetBucketName(getS3ExternalBucketName());
            transferDto.setS3KeyPrefix("testKeyPrefix");
            transferDto.setKmsKeyId(MockS3OperationsImpl.MOCK_KMS_ID_CANCELED_TRANSFER);
            s3Dao.copyFile(transferDto);
            fail("An IllegalStateException was expected but not thrown.");
        }
        catch (IllegalStateException ex)
        {
            assertEquals("Invalid IllegalStateException message returned.",
                "The transfer operation \"" + MockS3OperationsImpl.MOCK_TRANSFER_DESCRIPTION + "\" did not complete successfully. " +
                    "Current state: \"" + Transfer.TransferState.Canceled + "\".", ex.getMessage());
        }
    }

    /**
     * Test S3 exception handling in the getObjectMetadata S3Dao operation.
     */
    @Test(expected = IllegalStateException.class)
    public void testGetObjectMetadataException()
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION);
        s3Dao.getObjectMetadata(s3FileTransferRequestParamsDto);
    }

    /**
     * Test "key not found" scenario for the getObjectMetadata S3Dao operation.
     */
    @Test
    public void testGetObjectMetadataKeyNotFound()
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(MockS3OperationsImpl.MOCK_S3_FILE_NAME_NOT_FOUND);
        Assert.assertNull(s3Dao.getObjectMetadata(s3FileTransferRequestParamsDto));
    }

    /**
     * Test that we get an exception when trying to perform getObjectMetadata operation without properly initialized S3FileTransferRequestParamsDto parameters.
     */
    @Test(expected = NullPointerException.class)
    public void testGetObjectMetadataNullPointerException()
    {
        s3Dao.getObjectMetadata(null);
    }

    @Test
    public void testValidateS3File() throws IOException, InterruptedException
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();

        s3Dao.validateS3File(s3FileTransferRequestParamsDto, FILE_SIZE_1_KB);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testValidateS3FileObjectNotFoundException() throws IOException, InterruptedException
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(MockS3OperationsImpl.MOCK_S3_FILE_NAME_NOT_FOUND);

        s3Dao.validateS3File(s3FileTransferRequestParamsDto, FILE_SIZE_1_KB);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateS3FileRuntimeExceptionFileSizeDoesNotMatch() throws IOException, InterruptedException
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();

        s3Dao.validateS3File(s3FileTransferRequestParamsDto, FILE_SIZE_1_KB + 999);
    }

    /**
     * Test that we get an exception when trying to perform listDirectory operation without properly initialized S3FileTransferRequestParamsDto parameters.
     */
    @Test(expected = NullPointerException.class)
    public void testListDirectoryNullPointerException()
    {
        s3Dao.listDirectory(null);
    }

    /**
     * Test that we are able to perform the uploadFile S3Dao operation on S3 using our DAO tier.
     */
    @Test
    public void testUploadFile() throws IOException, InterruptedException
    {
        // Create local test file.
        File targetFile = createLocalFile(localTempPath.toString(), LOCAL_FILE, FILE_SIZE_1_KB);
        Assert.assertTrue(targetFile.isFile());
        Assert.assertTrue(targetFile.length() == FILE_SIZE_1_KB);

        // Upload test file to s3Dao.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3FileTransferRequestParamsDto.setLocalPath(targetFile.getPath());
        S3FileTransferResultsDto results = s3Dao.uploadFile(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate the file upload.
        validateS3FileUpload(s3FileTransferRequestParamsDto, Arrays.asList(TARGET_S3_KEY));
    }

    /**
     * Test that we are able to upload a zero byte file to S3 using our DAO tier.
     */
    @Test
    public void testUploadFileZeroBytes() throws IOException, InterruptedException
    {
        // Create an empty local file.
        File targetFile = createLocalFile(localTempPath.toString(), LOCAL_FILE, FILE_SIZE_0_BYTE);
        Assert.assertTrue(targetFile.isFile());
        Assert.assertTrue(targetFile.length() == FILE_SIZE_0_BYTE);

        // Upload test file to s3Dao.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3FileTransferRequestParamsDto.setLocalPath(targetFile.getPath());
        S3FileTransferResultsDto results = s3Dao.uploadFile(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate the file upload.
        validateS3FileUpload(s3FileTransferRequestParamsDto, Arrays.asList(TARGET_S3_KEY));
    }

    /**
     * Test that we are able to perform the uploadFile S3Dao operation utilizing Reduced Redundancy Storage (RRS) storage option.
     */
    @Test
    public void testUploadFileUseRrs() throws IOException, InterruptedException
    {
        // Create local test file.
        File targetFile = createLocalFile(localTempPath.toString(), LOCAL_FILE, FILE_SIZE_1_KB);
        Assert.assertTrue(targetFile.isFile());
        Assert.assertTrue(targetFile.length() == FILE_SIZE_1_KB);

        // Upload test file to s3Dao.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3FileTransferRequestParamsDto.setLocalPath(targetFile.getPath());
        s3FileTransferRequestParamsDto.setUseRrs(true);
        S3FileTransferResultsDto results = s3Dao.uploadFile(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate the file upload.
        validateS3FileUpload(s3FileTransferRequestParamsDto, Arrays.asList(TARGET_S3_KEY));

        // TODO: Validate Reduced Redundancy Storage (RRS) storage option.
    }

    /**
     * Test that we are able to perform the uploadFile S3Dao operation with the specified maximum number of threads set to some value.
     */
    @Test
    public void testUploadFileWithMaxThreadsSet() throws IOException, InterruptedException
    {
        // Create local test file.
        File targetFile = createLocalFile(localTempPath.toString(), LOCAL_FILE, FILE_SIZE_1_KB);
        Assert.assertTrue(targetFile.isFile());
        Assert.assertTrue(targetFile.length() == FILE_SIZE_1_KB);

        // Upload test file to s3Dao.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3FileTransferRequestParamsDto.setLocalPath(targetFile.getPath());
        s3FileTransferRequestParamsDto.setMaxThreads(3);
        S3FileTransferResultsDto results = s3Dao.uploadFile(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate the file upload.
        validateS3FileUpload(s3FileTransferRequestParamsDto, Arrays.asList(TARGET_S3_KEY));
    }

    /**
     * Test that we get an exception when trying to perform a file transfer without initialized S3FileTransferRequestParamsDto parameters.
     */
    @Test(expected = NullPointerException.class)
    public void testUploadFileNullPointerException() throws InterruptedException
    {
        s3Dao.uploadFile(null);
    }

    /**
     * Test that we are able to perform the uploadFileList S3Dao operation on S3 using our DAO tier.
     */
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

        // Upload test file to s3Dao.
        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setFiles(requestFileList);
        S3FileTransferResultsDto results = s3Dao.uploadFileList(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == LOCAL_FILES_SUBSET.size());

        // Validate the upload.
        validateS3FileUpload(s3FileTransferRequestParamsDto, expectedKeys);
    }

    /**
     * Test that we are able to perform the uploadFileList S3Dao operation with logger level set to ... on S3 using our DAO tier.
     */
    @Test
    public void testUploadFileListWithLoggerLevelSetToWarn() throws IOException, InterruptedException
    {
        Level origLoggerLevel = s3DaoImplLogger.getEffectiveLevel();
        s3DaoImplLogger.setLevel(Level.WARN);

        try
        {
            testUploadFileList();
        }
        finally
        {
            s3DaoImplLogger.setLevel(origLoggerLevel);
        }
    }

    /**
     * Test that we are able to perform the uploadDirectory S3Dao operation on S3 using our DAO tier.
     */
    @Test
    public void testUploadDirectory() throws IOException, InterruptedException
    {
        // Create local test files.
        for (String file : LOCAL_FILES)
        {
            createLocalFile(localTempPath.toString(), file, FILE_SIZE_1_KB);
        }

        // Upload test file to s3Dao.
        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setRecursive(true);
        S3FileTransferResultsDto results = s3Dao.uploadDirectory(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == LOCAL_FILES.size());

        // Build a list of expected S3 key values.
        List<String> expectedKeys = new ArrayList<>();
        for (String file : LOCAL_FILES)
        {
            expectedKeys.add(TEST_S3_KEY_PREFIX + "/" + file.replaceAll("\\\\", "/"));
        }

        // Validate the file upload.
        validateS3FileUpload(s3FileTransferRequestParamsDto, expectedKeys);
    }

    /**
     * Test that we are able to perform the uploadDirectory S3Dao operation on a folder with 0 bytes of data using our DAO tier.
     */
    @Test
    public void testUploadDirectoryZeroBytes() throws IOException, InterruptedException
    {
        // Create a zero size local file.
        File targetFile = createLocalFile(localTempPath.toString(), LOCAL_FILE, FILE_SIZE_0_BYTE);
        Assert.assertTrue(targetFile.isFile());

        // Upload empty folder to s3Dao.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX);
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setRecursive(true);
        S3FileTransferResultsDto results = s3Dao.uploadDirectory(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate the zero bytes upload.
        List<StorageFile> actualStorageFiles = s3Dao.listDirectory(s3FileTransferRequestParamsDto);
        Assert.assertTrue(actualStorageFiles.size() == 1);
    }

    /**
     * Test that we are able to perform the uploadDirectory S3Dao operation on a folder with more than 1000 files using our DAO tier. This test is needed to
     * make AmazonS3.listObjects() to start truncating ObjectListing that it returns.
     */
    @Test
    public void testUploadDirectoryLargeNumberOfFiles() throws IOException, InterruptedException
    {
        final int NUM_OF_FILES = 1001;

        // Create local files.
        for (int i = 0; i < NUM_OF_FILES; i++)
        {
            File targetFile = createLocalFile(localTempPath.toString(), String.format("%04d_%s", i, LOCAL_FILE), FILE_SIZE_0_BYTE);
            Assert.assertTrue(targetFile.isFile());
        }

        // Upload empty folder to s3Dao.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX);
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setRecursive(true);
        S3FileTransferResultsDto results = s3Dao.uploadDirectory(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == NUM_OF_FILES);

        // Validate the empty folder upload.
        List<StorageFile> actualStorageFiles = s3Dao.listDirectory(s3FileTransferRequestParamsDto);
        Assert.assertTrue(actualStorageFiles.size() == NUM_OF_FILES);
    }

    /**
     * Test that we are able to perform the deleteFileList S3Dao operation on S3 using our DAO tier.
     */
    @Test
    public void testDeleteFileList() throws IOException, InterruptedException
    {
        // Upload local directory to s3Dao.
        testUploadDirectory();

        // Validate that S3 directory is not empty.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        List<StorageFile> storageFiles = s3Dao.listDirectory(s3FileTransferRequestParamsDto);
        Assert.assertTrue(storageFiles.size() > 0);

        // Delete a list of files from S3 using s3Dao.
        List<File> requestFileList = new ArrayList<>();
        for (StorageFile storageFile : storageFiles)
        {
            requestFileList.add(new File(storageFile.getFilePath()));
        }
        s3FileTransferRequestParamsDto.setFiles(requestFileList);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(null);
        s3Dao.deleteFileList(s3FileTransferRequestParamsDto);

        // Validate that S3 directory got deleted.
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        storageFiles = s3Dao.listDirectory(s3FileTransferRequestParamsDto);
        Assert.assertTrue(storageFiles.size() == 0);
    }

    /**
     * Test that we are able to make a call to deleteFileList S3Dao method with an empty file list.
     */
    @Test
    public void testDeleteFileListEmptyList()
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setFiles(new ArrayList<File>());
        s3Dao.deleteFileList(s3FileTransferRequestParamsDto);
    }

    /**
     * Test that we are able to perform the deleteDirectory S3Dao operation on S3 using our DAO tier.
     */
    @Test
    public void testDeleteDirectory() throws IOException, InterruptedException
    {
        // Upload local directory to s3Dao.
        testUploadDirectory();

        // Validate that S3 directory is not empty.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        List<StorageFile> storageFiles = s3Dao.listDirectory(s3FileTransferRequestParamsDto);
        Assert.assertTrue(storageFiles.size() > 0);

        // Delete directory from S3 using s3Dao.
        s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);

        // Validate that S3 directory got deleted.
        storageFiles = s3Dao.listDirectory(s3FileTransferRequestParamsDto);
        Assert.assertTrue(storageFiles.size() == 0);
    }

    @Test
    public void testDeleteDirectoryNullParamsDto()
    {
        // Try to delete an S3 directory when transfer request parameters DTO is null.
        try
        {
            s3Dao.deleteDirectory(null);
            fail("Suppose to throw a NullPointerException.");
        }
        catch (NullPointerException e)
        {
            assertNull(e.getMessage());
        }
    }

    /**
     * Test that we are able to perform the uploadFile S3Dao operation on S3 using our DAO tier.
     */
    @Test
    public void testDownloadFile() throws IOException, InterruptedException
    {
        // Upload local file to s3Dao.
        testUploadFile();

        // Clean up the local directory, so we can test the download.
        FileUtils.deleteDirectory(localTempPath.toFile());

        // Create local temp directory - this also validates that clean up was really executed.
        Assert.assertTrue(localTempPath.toFile().mkdir());

        // Destination local file.
        File destinationLocalFile = Paths.get(localTempPath.toString(), LOCAL_FILE).toFile();

        // Execute download.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3FileTransferRequestParamsDto.setLocalPath(destinationLocalFile.getPath());
        S3FileTransferResultsDto results = s3Dao.downloadFile(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate that we have the file downloaded from S3.
        Assert.assertTrue(destinationLocalFile.isFile());
    }

    /**
     * Test that we are able to perform the uploadDirectory S3Dao operation on S3 using our DAO tier.
     */
    @Test
    public void testDownloadDirectory() throws IOException, InterruptedException
    {
        // Upload local directory to s3Dao.
        testUploadDirectory();

        // Clean up the local directory, so we can test the download.
        FileUtils.deleteDirectory(localTempPath.toFile());

        // Create local temp directory - this also validates that clean up was really executed.
        Assert.assertTrue(localTempPath.toFile().mkdir());

        // Execute download.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setRecursive(true);
        S3FileTransferResultsDto results = s3Dao.downloadDirectory(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == LOCAL_FILES.size());

        // Validate that we have the directory downloaded from S3.
        for (String file : LOCAL_FILES)
        {
            Assert.assertTrue(Paths.get(localTempPath.toString(), TEST_S3_KEY_PREFIX, file).toFile().isFile());
        }
    }

    @Test
    public void testAbortMultipartUploadsNullParamsDto()
    {
        // Try to abort multipart uploads when transfer request parameters DTO is null.
        try
        {
            s3Dao.abortMultipartUploads(null, new Date());
            fail("Suppose to throw a NullPointerException.");
        }
        catch (NullPointerException e)
        {
            assertNull(e.getMessage());
        }
    }

    @Test
    public void testListDirectoryNoSuchBucket()
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION);

        try
        {
            s3Dao.listDirectory(s3FileTransferRequestParamsDto);
            fail("expected a IllegalArgumentException to be thrown, but no exceptions thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            assertEquals("thrown exception message",
                "The specified bucket '" + MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION + "' does not exist.", e.getMessage());
        }
    }

    /**
     * The method is successful when both bucket and key exists.
     *
     * @throws IOException
     */
    @Test
    public void testGetS3ObjectSuccess() throws IOException
    {
        String expectedContentString = "test";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(expectedContentString.getBytes());
        PutObjectRequest putObjectRequest = new PutObjectRequest("test_bucket", "test_key", inputStream, new ObjectMetadata());
        s3Operations.putObject(putObjectRequest, null);

        GetObjectRequest getObjectRequest = new GetObjectRequest("test_bucket", "test_key");
        S3Object s3Object = s3Dao.getS3Object(getObjectRequest, getTestS3FileTransferRequestParamsDto());

        S3ObjectInputStream resultInputStream = s3Object.getObjectContent();
        String actualContentString = IOUtils.toString(resultInputStream);
        Assert.assertEquals("result content string", expectedContentString, actualContentString);
    }

    /**
     * Throws an ObjectNotFoundException when S3 object key does not exist. This should result as a 404 to clients.
     */
    @Test
    public void testGetS3ObjectThrowsWhenKeyDoesNotExist()
    {
        GetObjectRequest getObjectRequest = new GetObjectRequest("test_bucket", "test_key");
        try
        {
            s3Dao.getS3Object(getObjectRequest, getTestS3FileTransferRequestParamsDto());
            Assert.fail("expected ObjectNotFoundException to be thrown, but no exceptions were thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "Specified S3 object key '" + getObjectRequest.getKey() + "' does not exist.", e.getMessage());
        }
    }

    /**
     * Throws an ObjectNotFoundException when S3 bucket does not exist. This should result as a 404 to clients.
     */
    @Test
    public void testGetS3ObjectThrowsWhenBucketDoesNotExist()
    {
        GetObjectRequest getObjectRequest = new GetObjectRequest(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION, "test_key");
        try
        {
            s3Dao.getS3Object(getObjectRequest, getTestS3FileTransferRequestParamsDto());
            Assert.fail("expected ObjectNotFoundException to be thrown, but no exceptions were thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "Specified S3 bucket '" + getObjectRequest.getBucketName() + "' does not exist.", e.getMessage());
        }
    }

    /**
     * Throws an ObjectNotFoundException when application does not have access to the given object.
     */
    @Test
    public void testGetS3ObjectThrowsWhenAccessDenied()
    {
        GetObjectRequest getObjectRequest = new GetObjectRequest(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_ACCESS_DENIED, "test_key");
        try
        {
            s3Dao.getS3Object(getObjectRequest, getTestS3FileTransferRequestParamsDto());
            Assert.fail("expected ObjectNotFoundException to be thrown, but no exceptions were thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());
            Assert.assertEquals("thrown exception message",
                "Application does not have access to the specified S3 object at bucket '" + getObjectRequest.getBucketName() + "' and key '" +
                    getObjectRequest.getKey() + "'.", e.getMessage());
        }
    }

    /**
     * Throws the exception as-is without wrapping if the exception is of type AmazonServiceException or children.
     */
    @Test
    public void testGetS3ObjectThrowsAsIsWhenGenericAmazonError()
    {
        GetObjectRequest getObjectRequest = new GetObjectRequest(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_INTERNAL_ERROR, "test_key");
        try
        {
            s3Dao.getS3Object(getObjectRequest, getTestS3FileTransferRequestParamsDto());
            Assert.fail("expected AmazonServiceException to be thrown, but no exceptions were thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", AmazonServiceException.class, e.getClass());
        }
    }

    @Test
    public void testGetProperties()
    {
        String expectedKey = "foo";
        String expectedValue = "bar";
        String s3BucketName = "test_bucket";
        String s3ObjectKey = "test_key";

        ByteArrayInputStream inputStream = new ByteArrayInputStream((expectedKey + "=" + expectedValue).getBytes());
        PutObjectRequest putObjectRequest = new PutObjectRequest(s3BucketName, s3ObjectKey, inputStream, new ObjectMetadata());
        s3Operations.putObject(putObjectRequest, null);

        Properties properties = s3Dao.getProperties(s3BucketName, s3ObjectKey, getTestS3FileTransferRequestParamsDto());

        Assert.assertEquals("properties key '" + expectedKey + "'", expectedValue, properties.get(expectedKey));
    }

    /**
     * Asserts that calling generateGetObjectPresignedUrl() will return the expected mocked pre-signed URL.
     */
    @Test
    public void testGenerateGetObjectPresignedUrl()
    {
        String bucketName = "test_bucketName";
        String key = "test_key";
        Date expiration = new Date(12345l);
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setSignerOverride(S3FileTransferRequestParamsDto.SIGNER_OVERRIDE_V4);
        String result = s3Dao.generateGetObjectPresignedUrl(bucketName, key, expiration, s3FileTransferRequestParamsDto);

        Assert.assertEquals("result", "https://" + bucketName + "/" + key + "?method=GET&expiration=" + expiration.getTime(), result);
    }
}
