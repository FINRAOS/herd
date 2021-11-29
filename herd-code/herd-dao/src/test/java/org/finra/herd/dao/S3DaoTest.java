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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.MultiObjectDeleteException.DeleteError;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.CreateJobRequest;
import com.amazonaws.services.s3control.model.CreateJobResult;
import com.google.common.base.Objects;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.test.util.ReflectionTestUtils;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.dao.impl.MockS3OperationsImpl;
import org.finra.herd.dao.impl.S3DaoImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.dto.HerdAWSCredentialsProvider;
import org.finra.herd.model.dto.S3FileCopyRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferResultsDto;

/**
 * This class tests various functionality within the S3Dao class.
 */
public class S3DaoTest extends AbstractDaoTest
{
    private Path localTempPath;

    /**
     * Cleans up the local temp directory and S3 test path that we are using.
     */
    @After
    public void cleanEnv() throws IOException
    {
        // Clean up the local directory.
        FileUtils.deleteDirectory(localTempPath.toFile());

        // Delete test files from S3 storage. Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        for (S3FileTransferRequestParamsDto params : Arrays.asList(s3DaoTestHelper.getTestS3FileTransferRequestParamsDto(),
            S3FileTransferRequestParamsDto.builder().withS3BucketName(storageDaoTestHelper.getS3LoadingDockBucketName())
                .withS3KeyPrefix(TEST_S3_KEY_PREFIX + "/").build(),
            S3FileTransferRequestParamsDto.builder().withS3BucketName(storageDaoTestHelper.getS3ExternalBucketName()).withS3KeyPrefix(TEST_S3_KEY_PREFIX + "/")
                .build()))
        {
            if (!s3Dao.listDirectory(params).isEmpty())
            {
                s3Dao.deleteDirectory(params);
            }
        }

        s3Operations.rollback();
    }

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        // Create a local temp directory.
        localTempPath = Files.createTempDirectory(null);
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testAbortMultipartUploadsAssertAbortOnlyBeforeThreshold()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String uploadKey = "uploadKey1";
            String uploadId = "uploadId1";
            Date uploadInitiated = new Date(0);

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            Date thresholdDate = new Date(1);

            when(mockS3Operations.listMultipartUploads(any(), any())).then(new Answer<MultipartUploadListing>()
            {
                @Override
                public MultipartUploadListing answer(InvocationOnMock invocation) throws Throwable
                {
                    ListMultipartUploadsRequest listMultipartUploadsRequest = invocation.getArgument(0);
                    assertEquals(s3BucketName, listMultipartUploadsRequest.getBucketName());

                    MultipartUploadListing multipartUploadListing = new MultipartUploadListing();
                    {
                        MultipartUpload multipartUpload = new MultipartUpload();
                        multipartUpload.setUploadId(uploadId);
                        multipartUpload.setKey(uploadKey);
                        multipartUpload.setInitiated(uploadInitiated);
                        multipartUploadListing.getMultipartUploads().add(multipartUpload);
                    }
                    // This upload is not aborted since the initiated date is greater than the threshold
                    {
                        MultipartUpload multipartUpload = new MultipartUpload();
                        multipartUpload.setUploadId("uploadId2");
                        multipartUpload.setKey("uploadKey2");
                        multipartUpload.setInitiated(new Date(2));
                        multipartUploadListing.getMultipartUploads().add(multipartUpload);
                    }
                    return multipartUploadListing;
                }
            });

            assertEquals(1, s3Dao.abortMultipartUploads(s3FileTransferRequestParamsDto, thresholdDate));

            verify(mockS3Operations).listMultipartUploads(any(), any());

            /*
             * Assert that S3Operations.abortMultipartUpload is called exactly ONCE with arguments matching the given ArgumentMatcher
             */
            verify(mockS3Operations).abortMultipartUpload(argThat(
                argument -> Objects.equal(s3BucketName, argument.getBucketName()) && Objects.equal(uploadKey, argument.getKey()) &&
                    Objects.equal(uploadId, argument.getUploadId())), any());

            // Assert that no other interactions occur with the mock
            verifyNoMoreInteractions(mockS3Operations);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    /**
     * When any exception is thrown during the flow, the S3 client should be shut down gracefully. The exception is thrown as-is, without any wrapping or
     * special handling.
     */
    @Test
    public void testAbortMultipartUploadsAssertHandleGenericException()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            Date thresholdDate = new Date(1);

            when(mockS3Operations.listMultipartUploads(any(), any())).thenThrow(new AmazonServiceException("message"));

            try
            {
                s3Dao.abortMultipartUploads(s3FileTransferRequestParamsDto, thresholdDate);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(AmazonServiceException.class, e.getClass());
                assertEquals("message (Service: null; Status Code: 0; Error Code: null; Request ID: null; Proxy: null)", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testAbortMultipartUploadsAssertTruncatedResult()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String uploadKey = "uploadKey";
            String uploadId = "uploadId";
            Date uploadInitiated = new Date(0);

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            Date thresholdDate = new Date(1);

            when(mockS3Operations.listMultipartUploads(any(), any())).then(new Answer<MultipartUploadListing>()
            {
                @Override
                public MultipartUploadListing answer(InvocationOnMock invocation) throws Throwable
                {
                    ListMultipartUploadsRequest listMultipartUploadsRequest = invocation.getArgument(0);
                    String keyMarker = listMultipartUploadsRequest.getKeyMarker();
                    String uploadIdMarker = listMultipartUploadsRequest.getUploadIdMarker();

                    MultipartUploadListing multipartUploadListing = new MultipartUploadListing();
                    if (keyMarker == null || uploadIdMarker == null)
                    {
                        multipartUploadListing.setNextKeyMarker("nextKeyMarker");
                        multipartUploadListing.setNextUploadIdMarker("nextUploadIdMarker");
                        multipartUploadListing.setTruncated(true);
                    }
                    else
                    {
                        assertEquals("nextKeyMarker", keyMarker);
                        assertEquals("nextUploadIdMarker", uploadIdMarker);

                        MultipartUpload multipartUpload = new MultipartUpload();
                        multipartUpload.setUploadId(uploadId);
                        multipartUpload.setKey(uploadKey);
                        multipartUpload.setInitiated(uploadInitiated);
                        multipartUploadListing.getMultipartUploads().add(multipartUpload);
                    }
                    return multipartUploadListing;
                }
            });

            assertEquals(1, s3Dao.abortMultipartUploads(s3FileTransferRequestParamsDto, thresholdDate));

            // Assert listMultipartUploads() is called twice due to truncation
            verify(mockS3Operations, times(2)).listMultipartUploads(any(), any());

            /*
             * Assert that S3Operations.abortMultipartUpload is called exactly ONCE with arguments matching the given ArgumentMatcher
             */
            verify(mockS3Operations).abortMultipartUpload(argThat(
                argument -> Objects.equal(s3BucketName, argument.getBucketName()) && Objects.equal(uploadKey, argument.getKey()) &&
                    Objects.equal(uploadId, argument.getUploadId())), any());

            // Assert that no other interactions occur with the mock
            verifyNoMoreInteractions(mockS3Operations);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
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

    /**
     * Test S3 file copy without any errors.
     */
    @Test
    public void testCopyFile() throws InterruptedException
    {
        // Put a 1 byte file in S3.
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), null), null);

        S3FileCopyRequestParamsDto transferDto = new S3FileCopyRequestParamsDto();
        transferDto.setSourceBucketName(storageDaoTestHelper.getS3LoadingDockBucketName());
        transferDto.setTargetBucketName(storageDaoTestHelper.getS3ExternalBucketName());
        transferDto.setSourceObjectKey(TARGET_S3_KEY);
        transferDto.setTargetObjectKey(TARGET_S3_KEY);
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
        // Put a 1 byte file in S3.
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), null), null);

        try
        {
            S3FileCopyRequestParamsDto transferDto = new S3FileCopyRequestParamsDto();
            transferDto.setSourceBucketName(storageDaoTestHelper.getS3LoadingDockBucketName());
            transferDto.setTargetBucketName(storageDaoTestHelper.getS3ExternalBucketName());
            transferDto.setSourceObjectKey(TARGET_S3_KEY);
            transferDto.setTargetObjectKey(TARGET_S3_KEY);
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
     * Test S3 file copy with an invalid KMS Id that will result in a cancelled transfer.
     */
    @Test
    public void testCopyFileInvalidKmsIdCancelled() throws InterruptedException
    {
        // Put a 1 byte file in S3.
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), null), null);

        try
        {
            S3FileCopyRequestParamsDto transferDto = new S3FileCopyRequestParamsDto();
            transferDto.setSourceBucketName(storageDaoTestHelper.getS3LoadingDockBucketName());
            transferDto.setTargetBucketName(storageDaoTestHelper.getS3ExternalBucketName());
            transferDto.setSourceObjectKey(TARGET_S3_KEY);
            transferDto.setTargetObjectKey(TARGET_S3_KEY);
            transferDto.setKmsKeyId(MockS3OperationsImpl.MOCK_KMS_ID_CANCELED_TRANSFER);
            s3Dao.copyFile(transferDto);
            fail("An IllegalStateException was expected but not thrown.");
        }
        catch (IllegalStateException ex)
        {
            assertEquals("Invalid IllegalStateException message returned.",
                "The transfer operation \"" + MockS3OperationsImpl.MOCK_TRANSFER_DESCRIPTION + "\" did not complete successfully. " + "Current state: \"" +
                    Transfer.TransferState.Canceled + "\".", ex.getMessage());
        }
    }

    /**
     * Test S3 file copy with an invalid KMS Id that throws an IllegalStateException because no AmazonServiceException was found.
     */
    @Test
    public void testCopyFileInvalidKmsIdIllegalStateException() throws InterruptedException
    {
        // Put a 1 byte file in S3.
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), null), null);

        try
        {
            S3FileCopyRequestParamsDto transferDto = new S3FileCopyRequestParamsDto();
            transferDto.setSourceBucketName(storageDaoTestHelper.getS3LoadingDockBucketName());
            transferDto.setTargetBucketName(storageDaoTestHelper.getS3ExternalBucketName());
            transferDto.setSourceObjectKey(TARGET_S3_KEY);
            transferDto.setTargetObjectKey(TARGET_S3_KEY);
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
     * Test S3 file copy without a KMS ID specified.
     */
    @Test
    public void testCopyFileNoKmsId() throws InterruptedException
    {
        // Put a 1 byte file in S3.
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), null), null);

        // Perform an S3 file copy operation when KMS ID value is not specified.
        for (String kmsId : Arrays.asList(BLANK_TEXT, null))
        {
            S3FileCopyRequestParamsDto transferDto = new S3FileCopyRequestParamsDto();
            transferDto.setSourceBucketName(storageDaoTestHelper.getS3LoadingDockBucketName());
            transferDto.setTargetBucketName(storageDaoTestHelper.getS3ExternalBucketName());
            transferDto.setSourceObjectKey(TARGET_S3_KEY);
            transferDto.setTargetObjectKey(TARGET_S3_KEY);
            transferDto.setKmsKeyId(kmsId);
            S3FileTransferResultsDto resultsDto = s3Dao.copyFile(transferDto);
            assertEquals(Long.valueOf(1L), resultsDto.getTotalFilesTransferred());
        }
    }

    @Test
    public void testCreateDirectoryAssertAddTrailingSlashOnlyIfNotPresent()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix/";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);

            when(mockS3Operations.putObject(any(), any())).then(new Answer<PutObjectResult>()
            {
                @Override
                public PutObjectResult answer(InvocationOnMock invocation) throws Throwable
                {
                    PutObjectRequest putObjectRequest = invocation.getArgument(0);
                    assertEquals(s3BucketName, putObjectRequest.getBucketName());
                    assertEquals(s3KeyPrefix, putObjectRequest.getKey());

                    PutObjectResult putObjectResult = new PutObjectResult();
                    return putObjectResult;
                }
            });

            s3Dao.createDirectory(s3FileTransferRequestParamsDto);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testCreateDirectoryAssertCallsPutObject()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";
            String expectedS3KeyPrefix = s3KeyPrefix + "/";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);

            when(mockS3Operations.putObject(any(), any())).then(new Answer<PutObjectResult>()
            {
                @Override
                public PutObjectResult answer(InvocationOnMock invocation) throws Throwable
                {
                    PutObjectRequest putObjectRequest = invocation.getArgument(0);
                    assertEquals(s3BucketName, putObjectRequest.getBucketName());
                    assertEquals(expectedS3KeyPrefix, putObjectRequest.getKey());

                    PutObjectResult putObjectResult = new PutObjectResult();
                    return putObjectResult;
                }
            });

            s3Dao.createDirectory(s3FileTransferRequestParamsDto);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testCreateDirectoryAssertWrapAmazonServiceException()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix/";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);

            when(mockS3Operations.putObject(any(), any())).thenThrow(new AmazonServiceException("amazonServiceExceptionMessage"));

            try
            {
                s3Dao.createDirectory(s3FileTransferRequestParamsDto);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(IllegalStateException.class, e.getClass());
                assertEquals("Failed to create 0 byte S3 object with \"s3KeyPrefix/\" key in bucket \"s3BucketName\". Reason: amazonServiceExceptionMessage " +
                    "(Service: null; Status Code: 0; Error Code: null; Request ID: null; Proxy: null)", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        Assert.assertTrue(s3Dao.listDirectory(s3FileTransferRequestParamsDto).size() > 0);

        // Delete directory from S3 using s3Dao.
        s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);

        // Validate that S3 directory got deleted.
        Assert.assertEquals(0, s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());
    }

    @Test
    public void testDeleteDirectoryAssertHandleAmazonClientException()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName("s3BucketName");
            s3FileTransferRequestParamsDto.setS3KeyPrefix("s3KeyPrefix");

            VersionListing versionListing = new VersionListing();
            versionListing.getVersionSummaries().add(new S3VersionSummary());
            when(mockS3Operations.listVersions(any(), any())).thenReturn(versionListing);
            when(mockS3Operations.deleteObjects(any(), any())).thenThrow(new AmazonClientException("message"));

            try
            {
                s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(IllegalStateException.class, e.getClass());
                assertEquals("Failed to delete keys/key versions with prefix \"s3KeyPrefix\" from bucket \"s3BucketName\". Reason: message", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    /**
     * Asserts that when delete directory is called but S3 throws MultiObjectDeleteException, the exception is logged properly.
     */
    @Test
    @Ignore // TODO: Log4J2 - This test works within an IDE, but not from Maven. We need to figure out why.
    public void testDeleteDirectoryAssertMultiObjectDeleteExceptionLogged() throws Exception
    {
        // Inject mock
        S3Operations mockS3Operations = mock(S3Operations.class);
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        // Override logger with my own appender to inspect the output
        String loggerName = S3DaoImpl.class.getName();
        LogLevel originalLoggerLevel = getLogLevel(loggerName);
        setLogLevel(loggerName, LogLevel.ERROR);

        String appenderName = "TestWriterAppender";
        StringWriter stringWriter = addLoggingWriterAppender(appenderName);

        try
        {
            // Set up mocked behavior

            // Return a list of mock version listing
            VersionListing versionListing = new VersionListing();
            S3VersionSummary s3VersionSummary = new S3VersionSummary();
            s3VersionSummary.setKey("s3VersionSummaryKey");
            s3VersionSummary.setVersionId("s3VersionSummaryVersionId");
            versionListing.setVersionSummaries(Arrays.asList(s3VersionSummary));

            when(mockS3Operations.listVersions(any(), any())).thenReturn(versionListing);

            // Have mock implementation throw exception
            List<DeleteError> errors = new ArrayList<>();
            {
                DeleteError deleteError = new DeleteError();
                deleteError.setCode("deleteError1Code");
                deleteError.setKey("deleteError1Key");
                deleteError.setMessage("deleteError1Message");
                deleteError.setVersionId("deleteError1VersionId");
                errors.add(deleteError);
            }
            {
                DeleteError deleteError = new DeleteError();
                deleteError.setCode("deleteError2Code");
                deleteError.setKey("deleteError2Key");
                deleteError.setMessage("deleteError2Message");
                deleteError.setVersionId("deleteError2VersionId");
                errors.add(deleteError);
            }
            List<DeletedObject> deletedObjects = new ArrayList<>();
            MultiObjectDeleteException multiObjectDeleteException = new MultiObjectDeleteException(errors, deletedObjects);
            when(mockS3Operations.deleteObjects(any(), any())).thenThrow(multiObjectDeleteException);

            // try the operation and catch exception
            try
            {
                S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
                s3FileTransferRequestParamsDto.setS3KeyPrefix("/test/prefix");
                s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
                fail();
            }
            catch (Exception e)
            {
                // Inspect and assert exception
                assertEquals(IllegalStateException.class, e.getClass());
                assertEquals(multiObjectDeleteException, e.getCause());
            }

            assertEquals(String.format("Error deleting multiple objects. Below are the list of objects which failed to delete.%n" +
                "s3Key=\"deleteError1Key\" s3VersionId=\"deleteError1VersionId\" " +
                "s3DeleteErrorCode=\"deleteError1Code\" s3DeleteErrorMessage=\"deleteError1Message\"%n" +
                "s3Key=\"deleteError2Key\" s3VersionId=\"deleteError2VersionId\" " +
                "s3DeleteErrorCode=\"deleteError2Code\" s3DeleteErrorMessage=\"deleteError2Message\"%n%n"), stringWriter.toString());
        }
        finally
        {
            // Restore original resources
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
            setLogLevel(loggerName, originalLoggerLevel);
            removeLoggingAppender(appenderName);
        }
    }

    /**
     * Test that we are able to perform the deleteDirectory S3Dao operation on S3 with enabled versioning using our DAO tier.
     */
    @Test
    public void testDeleteDirectoryBucketVersioningEnabled() throws IOException, InterruptedException
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = null;

        try
        {
            // Create local test files.
            for (String file : LOCAL_FILES)
            {
                createLocalFile(localTempPath.toString(), file, FILE_SIZE_1_KB);
            }

            // Upload twice the same set of test files to an S3 bucket with enabled versioning.
            // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
            s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
            s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
            s3FileTransferRequestParamsDto.setRecursive(true);
            for (int i = 0; i < 2; i++)
            {
                S3FileTransferResultsDto results = s3Dao.uploadDirectory(s3FileTransferRequestParamsDto);
                Assert.assertEquals(Long.valueOf(LOCAL_FILES.size()), results.getTotalFilesTransferred());
            }

            // Validate the existence of keys and key versions for the uploaded test files in S3.
            Assert.assertEquals(LOCAL_FILES.size(), s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());
            Assert.assertEquals(LOCAL_FILES.size() * 2, s3Dao.listVersions(s3FileTransferRequestParamsDto).size());

            // Delete directory from S3 using s3Dao.
            s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);

            // Validate that S3 directory got deleted.
            Assert.assertEquals(0, s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());
            Assert.assertEquals(0, s3Dao.listVersions(s3FileTransferRequestParamsDto).size());
        }
        catch (Exception e)
        {
            // Clean up the S3 on failure.
            if (s3FileTransferRequestParamsDto != null)
            {
                s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
            }
        }
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

    @Test
    public void testDeleteDirectoryRootKeyPrefix()
    {
        for (String s3KeyPrefix : Arrays.asList(null, BLANK_TEXT, "/"))
        {
            try
            {
                s3Dao.deleteDirectory(S3FileTransferRequestParamsDto.builder().withS3KeyPrefix(s3KeyPrefix).build());
                fail("Should throw an IllegalArgumentException when S3 key prefix specifies a root directory.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("Deleting from root directory is not allowed.", e.getMessage());
            }
        }
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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        List<S3ObjectSummary> s3ObjectSummaries = s3Dao.listDirectory(s3FileTransferRequestParamsDto);
        Assert.assertTrue(s3ObjectSummaries.size() > 0);

        // Delete a list of files from S3 using s3Dao.
        List<File> requestFileList = new ArrayList<>();
        for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries)
        {
            requestFileList.add(new File(s3ObjectSummary.getKey()));
        }
        s3FileTransferRequestParamsDto.setFiles(requestFileList);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(null);
        s3Dao.deleteFileList(s3FileTransferRequestParamsDto);

        // Validate that S3 directory got deleted.
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        Assert.assertEquals(0, s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());
    }

    @Test
    public void testDeleteFileListAssertHandleGenericExceptions()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName("s3BucketName");
            s3FileTransferRequestParamsDto.setFiles(Arrays.asList(new File("file")));

            when(mockS3Operations.deleteObjects(any(), any())).thenThrow(new AmazonServiceException("testException"));

            try
            {
                s3Dao.deleteFileList(s3FileTransferRequestParamsDto);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(IllegalStateException.class, e.getClass());
                assertEquals(
                    "Failed to delete a list of keys from bucket \"s3BucketName\". Reason: testException (Service: null; Status Code: 0; Error Code: null; " +
                        "Request ID: null; Proxy: null)", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    /**
     * Test that we are able to make a call to deleteFileList S3Dao method with an empty file list.
     */
    @Test
    public void testDeleteFileListEmptyList()
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setFiles(new ArrayList<File>());
        s3Dao.deleteFileList(s3FileTransferRequestParamsDto);
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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3FileTransferRequestParamsDto.setLocalPath(destinationLocalFile.getPath());
        S3FileTransferResultsDto results = s3Dao.downloadFile(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate that we have the file downloaded from S3.
        Assert.assertTrue(destinationLocalFile.isFile());
    }

    /**
     * Asserts that calling generateGetObjectPresignedUrl() will return the expected mocked pre-signed URL.
     */
    @Test
    public void testGenerateGetObjectPresignedUrl()
    {
        String bucketName = "test_bucketName";
        String key = "test_key";
        Date expiration = new Date(12345L);
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        String result = s3Dao.generateGetObjectPresignedUrl(bucketName, key, expiration, s3FileTransferRequestParamsDto);

        Assert.assertEquals("result", "https://" + bucketName + "/" + key + "?method=GET&expiration=" + expiration.getTime(), result);
    }

    @Test
    public void testGenerateGetObjectPresignedUrlHandleExceptions() throws Exception
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String bucketName = "bucketName";
            String key = "key";
            Date expiration = new Date();
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

            when(mockS3Operations.generatePresignedUrl(any(), any())).thenThrow(new RuntimeException("message"));

            try
            {
                s3Dao.generateGetObjectPresignedUrl(bucketName, key, expiration, s3FileTransferRequestParamsDto);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(RuntimeException.class, e.getClass());
                assertEquals("message", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    /**
     * A case where additional credentials provider is given in the request params. The credentials returned should be an AWS session credential where the
     * values are from the provided custom credentials provider.
     */
    @Test
    public void testGetAWSCredentialsProviderAssertAdditionalProviderIsSet() throws Exception
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";
            String awsAccessKey = "awsAccessKey";
            String awsSecretKey = "awsSecretKey";
            String awsSessionToken = "awsSessionToken";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            s3FileTransferRequestParamsDto.setAdditionalAwsCredentialsProviders(Arrays.asList(new HerdAWSCredentialsProvider()
            {
                @Override
                public AwsCredential getAwsCredential()
                {
                    return new AwsCredential(awsAccessKey, awsSecretKey, awsSessionToken, null);
                }
            }));

            when(mockS3Operations.putObject(any(), any())).then(new Answer<PutObjectResult>()
            {
                @SuppressWarnings("unchecked")
                @Override
                public PutObjectResult answer(InvocationOnMock invocation) throws Throwable
                {
                    AmazonS3Client amazonS3Client = invocation.getArgument(1);
                    AWSCredentialsProviderChain awsCredentialsProviderChain =
                        (AWSCredentialsProviderChain) ReflectionTestUtils.getField(amazonS3Client, "awsCredentialsProvider");
                    List<AWSCredentialsProvider> credentialsProviders =
                        (List<AWSCredentialsProvider>) ReflectionTestUtils.getField(awsCredentialsProviderChain, "credentialsProviders");
                    assertEquals(2, credentialsProviders.size());

                    // refresh() does nothing, but gives code coverage
                    credentialsProviders.get(0).refresh();

                    /*
                     * We can't inspect the field directly since the class definition is private.
                     * Instead we call the getCredentials() and verify that it returns the credentials staged as part of this test.
                     */
                    AWSCredentials credentials = awsCredentialsProviderChain.getCredentials();
                    assertEquals(BasicSessionCredentials.class, credentials.getClass());

                    BasicSessionCredentials basicSessionCredentials = (BasicSessionCredentials) credentials;

                    assertEquals(awsAccessKey, basicSessionCredentials.getAWSAccessKeyId());
                    assertEquals(awsSecretKey, basicSessionCredentials.getAWSSecretKey());
                    assertEquals(awsSessionToken, basicSessionCredentials.getSessionToken());

                    return new PutObjectResult();
                }
            });

            s3Dao.createDirectory(s3FileTransferRequestParamsDto);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testGetAWSCredentialsProviderAssertStaticCredentialsIsNotSetWhenAccessKeyIsNull()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";
            String s3AccessKey = null;
            String s3SecretKey = "s3SecretKey";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            s3FileTransferRequestParamsDto.setAwsAccessKeyId(s3AccessKey);
            s3FileTransferRequestParamsDto.setAwsSecretKey(s3SecretKey);

            when(mockS3Operations.putObject(any(), any())).then(new Answer<PutObjectResult>()
            {
                @SuppressWarnings("unchecked")
                @Override
                public PutObjectResult answer(InvocationOnMock invocation) throws Throwable
                {
                    AmazonS3Client amazonS3Client = invocation.getArgument(1);
                    AWSCredentialsProviderChain awsCredentialsProviderChain =
                        (AWSCredentialsProviderChain) ReflectionTestUtils.getField(amazonS3Client, "awsCredentialsProvider");
                    List<AWSCredentialsProvider> credentialsProviders =
                        (List<AWSCredentialsProvider>) ReflectionTestUtils.getField(awsCredentialsProviderChain, "credentialsProviders");
                    assertEquals(1, credentialsProviders.size());
                    assertEquals(DefaultAWSCredentialsProviderChain.class, credentialsProviders.get(0).getClass());
                    return new PutObjectResult();
                }
            });

            s3Dao.createDirectory(s3FileTransferRequestParamsDto);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testGetAWSCredentialsProviderAssertStaticCredentialsIsNotSetWhenSecretKeyIsNull()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";
            String s3AccessKey = "s3AccessKey";
            String s3SecretKey = null;

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            s3FileTransferRequestParamsDto.setAwsAccessKeyId(s3AccessKey);
            s3FileTransferRequestParamsDto.setAwsSecretKey(s3SecretKey);

            when(mockS3Operations.putObject(any(), any())).then(new Answer<PutObjectResult>()
            {
                @SuppressWarnings("unchecked")
                @Override
                public PutObjectResult answer(InvocationOnMock invocation) throws Throwable
                {
                    AmazonS3Client amazonS3Client = invocation.getArgument(1);
                    AWSCredentialsProviderChain awsCredentialsProviderChain =
                        (AWSCredentialsProviderChain) ReflectionTestUtils.getField(amazonS3Client, "awsCredentialsProvider");
                    List<AWSCredentialsProvider> credentialsProviders =
                        (List<AWSCredentialsProvider>) ReflectionTestUtils.getField(awsCredentialsProviderChain, "credentialsProviders");
                    assertEquals(1, credentialsProviders.size());
                    assertEquals(DefaultAWSCredentialsProviderChain.class, credentialsProviders.get(0).getClass());
                    return new PutObjectResult();
                }
            });

            s3Dao.createDirectory(s3FileTransferRequestParamsDto);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testGetAWSCredentialsProviderAssertStaticCredentialsSet()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";
            String s3AccessKey = "s3AccessKey";
            String s3SecretKey = "s3SecretKey";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            s3FileTransferRequestParamsDto.setAwsAccessKeyId(s3AccessKey);
            s3FileTransferRequestParamsDto.setAwsSecretKey(s3SecretKey);

            when(mockS3Operations.putObject(any(), any())).then(new Answer<PutObjectResult>()
            {
                @SuppressWarnings("unchecked")
                @Override
                public PutObjectResult answer(InvocationOnMock invocation) throws Throwable
                {
                    AmazonS3Client amazonS3Client = invocation.getArgument(1);
                    AWSCredentialsProviderChain awsCredentialsProviderChain =
                        (AWSCredentialsProviderChain) ReflectionTestUtils.getField(amazonS3Client, "awsCredentialsProvider");
                    List<AWSCredentialsProvider> credentialsProviders =
                        (List<AWSCredentialsProvider>) ReflectionTestUtils.getField(awsCredentialsProviderChain, "credentialsProviders");
                    // Expect 2 providers: the static provider, and the default provider
                    assertEquals(2, credentialsProviders.size());

                    // Only verify the static value
                    assertEquals(StaticCredentialsProvider.class, credentialsProviders.get(0).getClass());
                    StaticCredentialsProvider staticCredentialsProvider = (StaticCredentialsProvider) credentialsProviders.get(0);
                    assertEquals(s3AccessKey, staticCredentialsProvider.getCredentials().getAWSAccessKeyId());
                    assertEquals(s3SecretKey, staticCredentialsProvider.getCredentials().getAWSSecretKey());
                    return new PutObjectResult();
                }
            });

            s3Dao.createDirectory(s3FileTransferRequestParamsDto);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testGetAmazonS3AssertProxyIsNotSetWhenProxyHostIsBlank()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";
            String httpProxyHost = "";
            Integer httpProxyPort = 1234;

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            s3FileTransferRequestParamsDto.setHttpProxyHost(httpProxyHost);
            s3FileTransferRequestParamsDto.setHttpProxyPort(httpProxyPort);

            when(mockS3Operations.putObject(any(), any())).then(new Answer<PutObjectResult>()
            {
                @Override
                public PutObjectResult answer(InvocationOnMock invocation) throws Throwable
                {
                    AmazonS3Client amazonS3Client = invocation.getArgument(1);
                    ClientConfiguration clientConfiguration = (ClientConfiguration) ReflectionTestUtils.getField(amazonS3Client, "clientConfiguration");
                    assertNull(clientConfiguration.getProxyHost());
                    return new PutObjectResult();
                }
            });

            s3Dao.createDirectory(s3FileTransferRequestParamsDto);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testGetAmazonS3AssertProxyIsNotSetWhenProxyPortIsNull()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";
            String httpProxyHost = "httpProxyHost";
            Integer httpProxyPort = null;

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            s3FileTransferRequestParamsDto.setHttpProxyHost(httpProxyHost);
            s3FileTransferRequestParamsDto.setHttpProxyPort(httpProxyPort);

            when(mockS3Operations.putObject(any(), any())).then(new Answer<PutObjectResult>()
            {
                @Override
                public PutObjectResult answer(InvocationOnMock invocation) throws Throwable
                {
                    AmazonS3Client amazonS3Client = invocation.getArgument(1);
                    ClientConfiguration clientConfiguration = (ClientConfiguration) ReflectionTestUtils.getField(amazonS3Client, "clientConfiguration");
                    assertNull(clientConfiguration.getProxyHost());
                    return new PutObjectResult();
                }
            });

            s3Dao.createDirectory(s3FileTransferRequestParamsDto);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testGetAmazonS3AssertProxyIsSet()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";
            String httpProxyHost = "httpProxyHost";
            Integer httpProxyPort = 1234;

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            s3FileTransferRequestParamsDto.setHttpProxyHost(httpProxyHost);
            s3FileTransferRequestParamsDto.setHttpProxyPort(httpProxyPort);

            when(mockS3Operations.putObject(any(), any())).then(new Answer<PutObjectResult>()
            {
                @Override
                public PutObjectResult answer(InvocationOnMock invocation) throws Throwable
                {
                    AmazonS3Client amazonS3Client = invocation.getArgument(1);
                    ClientConfiguration clientConfiguration = (ClientConfiguration) ReflectionTestUtils.getField(amazonS3Client, "clientConfiguration");
                    assertEquals(httpProxyHost, clientConfiguration.getProxyHost());
                    assertEquals(httpProxyPort.intValue(), clientConfiguration.getProxyPort());
                    return new PutObjectResult();
                }
            });

            s3Dao.createDirectory(s3FileTransferRequestParamsDto);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testGetAmazonS3AssertS3EndpointIsSet()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";
            String s3Endpoint = "s3Endpoint";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            s3FileTransferRequestParamsDto.setS3Endpoint(s3Endpoint);

            when(mockS3Operations.putObject(any(), any())).then(new Answer<PutObjectResult>()
            {
                @Override
                public PutObjectResult answer(InvocationOnMock invocation) throws Throwable
                {
                    AmazonS3Client amazonS3Client = invocation.getArgument(1);
                    assertEquals(new URI("https://" + s3Endpoint), ReflectionTestUtils.getField(amazonS3Client, "endpoint"));
                    return new PutObjectResult();
                }
            });

            s3Dao.createDirectory(s3FileTransferRequestParamsDto);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testGetAmazonS3AssertV4SigningAlwaysPresent()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);

            when(mockS3Operations.putObject(any(), any())).then(invocation -> {
                AmazonS3Client amazonS3Client = invocation.getArgument(1);
                ClientConfiguration clientConfiguration = (ClientConfiguration) ReflectionTestUtils.getField(amazonS3Client, "clientConfiguration");
                assertEquals(S3Dao.SIGNER_OVERRIDE_V4, clientConfiguration.getSignerOverride());
                return new PutObjectResult();
            });
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    /**
     * Test "access denied" scenario for the getObjectMetadata S3Dao operation.
     */
    @Test
    public void testGetObjectMetadataAccessDenied()
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();

        // Try to retrieve S3 object metadata when S3 access is denied.
        try
        {
            s3FileTransferRequestParamsDto.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_ACCESS_DENIED);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
            s3Dao.getObjectMetadata(s3FileTransferRequestParamsDto);
            fail("Should throw an ObjectNotFoundException when S3 access is denied.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Failed to get S3 metadata for object key \"%s\" from bucket \"%s\". " +
                    "Reason: AccessDenied (Service: null; Status Code: 403; Error Code: AccessDenied; Request ID: null; Proxy: null)", TARGET_S3_KEY,
                MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_ACCESS_DENIED), e.getMessage());
        }
    }

    /**
     * Test that we get an exception when trying to perform getObjectMetadata operation without properly initialized S3FileTransferRequestParamsDto parameters.
     */
    @Test(expected = NullPointerException.class)
    public void testGetObjectMetadataNullPointerException()
    {
        s3Dao.getObjectMetadata(null);
    }

    /**
     * Test "bucket not found" scenario for the getObjectMetadata S3Dao operation.
     */
    @Test
    public void testGetObjectMetadataS3BucketNoExists()
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();

        // Try to retrieve S3 object metadata when S3 bucket does not exist.
        s3FileTransferRequestParamsDto.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        assertNull(s3Dao.getObjectMetadata(s3FileTransferRequestParamsDto));
    }

    /**
     * Test "key not found" scenario for the getObjectMetadata S3Dao operation.
     */
    @Test
    public void testGetObjectMetadataS3KeyNoExists()
    {
        // Try to retrieve S3 object metadata for a non-existing S3 key.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        assertNull(s3Dao.getObjectMetadata(s3FileTransferRequestParamsDto));
    }

    /**
     * Test S3 exception handling in the getObjectMetadata S3Dao operation.
     */
    @Test
    public void testGetObjectMetadataServiceException()
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();

        // Validate that S3 service exception is handled correctly when retrieving S3 object metadata.
        try
        {
            s3FileTransferRequestParamsDto.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_INTERNAL_ERROR);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
            s3Dao.getObjectMetadata(s3FileTransferRequestParamsDto);
            fail("Should throw an IllegalStateException when Amazon service exception occurs.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Failed to get S3 metadata for object key \"%s\" from bucket \"%s\". " +
                    "Reason: InternalError (Service: null; Status Code: 0; Error Code: InternalError; Request ID: null; Proxy: null)",
                s3FileTransferRequestParamsDto.getS3KeyPrefix(), s3FileTransferRequestParamsDto.getS3BucketName()), e.getMessage());
        }
    }

    /**
     * Get ObjectMetadata with a socket timeout setting.
     */
    @Test
    public void testGetObjectMetadataWithSocketTimeout()
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setSocketTimeout(10000);

        // Try to retrieve S3 object metadata when S3 access is denied.
        try
        {
            s3FileTransferRequestParamsDto.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_ACCESS_DENIED);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
            s3Dao.getObjectMetadata(s3FileTransferRequestParamsDto);
            fail("Should throw an ObjectNotFoundException when S3 access is denied.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Failed to get S3 metadata for object key \"%s\" from bucket \"%s\". " +
                    "Reason: AccessDenied (Service: null; Status Code: 403; Error Code: AccessDenied; Request ID: null; Proxy: null)", TARGET_S3_KEY,
                MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_ACCESS_DENIED), e.getMessage());
        }
    }

    /**
     * The method is successful when both bucket and key exists.
     */
    @Test
    public void testGetProperties()
    {
        String expectedKey = "foo";
        String expectedValue = "bar";

        ByteArrayInputStream inputStream = new ByteArrayInputStream((expectedKey + "=" + expectedValue).getBytes());
        PutObjectRequest putObjectRequest = new PutObjectRequest(S3_BUCKET_NAME, TARGET_S3_KEY, inputStream, new ObjectMetadata());
        s3Operations.putObject(putObjectRequest, null);

        Properties properties = s3Dao.getProperties(S3_BUCKET_NAME, TARGET_S3_KEY, s3DaoTestHelper.getTestS3FileTransferRequestParamsDto());

        Assert.assertEquals("properties key '" + expectedKey + "'", expectedValue, properties.get(expectedKey));
    }

    @Test
    public void testGetPropertiesHandleGenericException() throws Exception
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        JavaPropertiesHelper originalJavaPropertiesHelper = (JavaPropertiesHelper) ReflectionTestUtils.getField(s3Dao, "javaPropertiesHelper");
        JavaPropertiesHelper mockJavaPropertiesHelper = mock(JavaPropertiesHelper.class);
        ReflectionTestUtils.setField(s3Dao, "javaPropertiesHelper", mockJavaPropertiesHelper);

        try
        {
            String bucketName = "bucketName";
            String key = "key";
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

            S3Object s3Object = new S3Object();
            s3Object.setObjectContent(new ByteArrayInputStream(new byte[] {0}));
            when(mockS3Operations.getS3Object(any(), any())).thenReturn(s3Object);
            when(mockJavaPropertiesHelper.getProperties(any(InputStream.class))).thenThrow(new RuntimeException("message"));

            try
            {
                s3Dao.getProperties(bucketName, key, s3FileTransferRequestParamsDto);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(RuntimeException.class, e.getClass());
                assertEquals("message", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
            ReflectionTestUtils.setField(s3Dao, "javaPropertiesHelper", originalJavaPropertiesHelper);
        }
    }

    @Test
    public void testGetPropertiesHandleIllegalArgumentException() throws Exception
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        JavaPropertiesHelper originalJavaPropertiesHelper = (JavaPropertiesHelper) ReflectionTestUtils.getField(s3Dao, "javaPropertiesHelper");
        JavaPropertiesHelper mockJavaPropertiesHelper = mock(JavaPropertiesHelper.class);
        ReflectionTestUtils.setField(s3Dao, "javaPropertiesHelper", mockJavaPropertiesHelper);

        try
        {
            String bucketName = "bucketName";
            String key = "key";
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

            S3Object s3Object = new S3Object();
            s3Object.setObjectContent(new ByteArrayInputStream(new byte[] {0}));
            when(mockS3Operations.getS3Object(any(), any())).thenReturn(s3Object);
            when(mockJavaPropertiesHelper.getProperties(any(InputStream.class))).thenThrow(new IllegalArgumentException("message"));

            try
            {
                s3Dao.getProperties(bucketName, key, s3FileTransferRequestParamsDto);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(IllegalArgumentException.class, e.getClass());
                assertEquals("The properties file in S3 bucket 'bucketName' and key 'key' is invalid.", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
            ReflectionTestUtils.setField(s3Dao, "javaPropertiesHelper", originalJavaPropertiesHelper);
        }
    }

    /**
     * Throws the exception as-is without wrapping if the exception is of type AmazonServiceException or children.
     */
    @Test
    public void testGetPropertiesThrowsAsIsWhenGenericAmazonError()
    {
        try
        {
            s3Dao.getProperties(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_INTERNAL_ERROR, TARGET_S3_KEY,
                s3DaoTestHelper.getTestS3FileTransferRequestParamsDto());
            Assert.fail("expected AmazonServiceException to be thrown, but no exceptions were thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", AmazonServiceException.class, e.getClass());
        }
    }

    /**
     * Throws an ObjectNotFoundException when application does not have access to the given object.
     */
    @Test
    public void testGetPropertiesThrowsWhenAccessDenied()
    {
        try
        {
            s3Dao.getProperties(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_ACCESS_DENIED, TARGET_S3_KEY, s3DaoTestHelper.getTestS3FileTransferRequestParamsDto());
            Assert.fail("expected ObjectNotFoundException to be thrown, but no exceptions were thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());
            Assert.assertEquals("thrown exception message",
                "Application does not have access to the specified S3 object at bucket '" + MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_ACCESS_DENIED +
                    "' and key '" + TARGET_S3_KEY + "'.", e.getMessage());
        }
    }

    /**
     * Throws an ObjectNotFoundException when S3 bucket does not exist. This should result as a 404 to clients.
     */
    @Test
    public void testGetPropertiesThrowsWhenBucketDoesNotExist()
    {
        try
        {
            s3Dao.getProperties(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION, TARGET_S3_KEY,
                s3DaoTestHelper.getTestS3FileTransferRequestParamsDto());
            Assert.fail("expected ObjectNotFoundException to be thrown, but no exceptions were thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());
            Assert.assertEquals("thrown exception message",
                "Specified S3 bucket '" + MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION + "' does not exist.", e.getMessage());
        }
    }

    /**
     * Throws an ObjectNotFoundException when S3 object key does not exist. This should result as a 404 to clients.
     */
    @Test
    public void testGetPropertiesThrowsWhenKeyDoesNotExist()
    {
        try
        {
            s3Dao.getProperties(S3_BUCKET_NAME, TARGET_S3_KEY, s3DaoTestHelper.getTestS3FileTransferRequestParamsDto());
            Assert.fail("expected ObjectNotFoundException to be thrown, but no exceptions were thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "Specified S3 object key '" + TARGET_S3_KEY + "' does not exist.", e.getMessage());
        }
    }

    @Test
    public void testListDirectoryAssertHandleAmazonClientException()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            boolean ignoreZeroByteDirectoryMarkers = true;

            when(mockS3Operations.listObjects(any(), any())).thenThrow(new AmazonClientException("message"));

            try
            {
                s3Dao.listDirectory(s3FileTransferRequestParamsDto, ignoreZeroByteDirectoryMarkers);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(IllegalStateException.class, e.getClass());
                assertEquals("Failed to list keys with prefix \"s3KeyPrefix\" from bucket \"s3BucketName\". Reason: message", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testListDirectoryAssertHandleGenericAmazonS3Exception()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            boolean ignoreZeroByteDirectoryMarkers = true;

            when(mockS3Operations.listObjects(any(), any())).thenThrow(new AmazonS3Exception("message"));

            try
            {
                s3Dao.listDirectory(s3FileTransferRequestParamsDto, ignoreZeroByteDirectoryMarkers);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(IllegalStateException.class, e.getClass());
                assertEquals("Error accessing S3", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testListDirectoryAssertIgnoreDirectories()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            boolean ignoreZeroByteDirectoryMarkers = true;

            when(mockS3Operations.listObjects(any(), any())).then(new Answer<ObjectListing>()
            {
                @Override
                public ObjectListing answer(InvocationOnMock invocation) throws Throwable
                {
                    ListObjectsRequest listObjectsRequest = invocation.getArgument(0);
                    assertEquals(s3BucketName, listObjectsRequest.getBucketName());
                    assertEquals(s3KeyPrefix, listObjectsRequest.getPrefix());

                    ObjectListing objectListing = new ObjectListing();
                    {
                        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
                        s3ObjectSummary.setBucketName(s3BucketName);
                        s3ObjectSummary.setKey("valid/object/key");
                        s3ObjectSummary.setSize(1024l);
                        objectListing.getObjectSummaries().add(s3ObjectSummary);
                    }
                    {
                        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
                        s3ObjectSummary.setBucketName(s3BucketName);
                        s3ObjectSummary.setKey("empty/file");
                        s3ObjectSummary.setSize(0l);
                        objectListing.getObjectSummaries().add(s3ObjectSummary);
                    }
                    {
                        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
                        s3ObjectSummary.setBucketName(s3BucketName);
                        s3ObjectSummary.setKey("directory/path/");
                        s3ObjectSummary.setSize(0l);
                        objectListing.getObjectSummaries().add(s3ObjectSummary);
                    }
                    // directory with a non-zero size is impossible, but we have a conditional branch to cover
                    {
                        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
                        s3ObjectSummary.setBucketName(s3BucketName);
                        s3ObjectSummary.setKey("another/directory/path/");
                        s3ObjectSummary.setSize(1024l);
                        objectListing.getObjectSummaries().add(s3ObjectSummary);
                    }
                    return objectListing;
                }
            });

            List<S3ObjectSummary> s3ObjectSummaries = s3Dao.listDirectory(s3FileTransferRequestParamsDto, ignoreZeroByteDirectoryMarkers);
            assertEquals(3, s3ObjectSummaries.size());
            assertEquals("valid/object/key", s3ObjectSummaries.get(0).getKey());
            assertEquals(1024l, s3ObjectSummaries.get(0).getSize());
            assertEquals("empty/file", s3ObjectSummaries.get(1).getKey());
            assertEquals(0l, s3ObjectSummaries.get(1).getSize());
            assertEquals("another/directory/path/", s3ObjectSummaries.get(2).getKey());
            assertEquals(1024l, s3ObjectSummaries.get(2).getSize());
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testListDirectoryAssertTruncatedResult()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";
            String expectedFilePath = "key";
            long expectedFileSize = 1024l;

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            boolean ignoreZeroByteDirectoryMarkers = true;

            when(mockS3Operations.listObjects(any(), any())).then(new Answer<ObjectListing>()
            {
                @Override
                public ObjectListing answer(InvocationOnMock invocation) throws Throwable
                {
                    ListObjectsRequest listObjectsRequest = invocation.getArgument(0);
                    String marker = listObjectsRequest.getMarker();

                    ObjectListing objectListing = new ObjectListing();
                    if (marker == null)
                    {
                        objectListing.setNextMarker("marker");
                        objectListing.setTruncated(true);
                    }
                    else
                    {
                        assertEquals("marker", marker);
                        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
                        s3ObjectSummary.setKey(expectedFilePath);
                        s3ObjectSummary.setSize(expectedFileSize);
                        objectListing.getObjectSummaries().add(s3ObjectSummary);
                    }
                    return objectListing;
                }
            });

            List<S3ObjectSummary> s3ObjectSummaries = s3Dao.listDirectory(s3FileTransferRequestParamsDto, ignoreZeroByteDirectoryMarkers);
            assertEquals(1, s3ObjectSummaries.size());
            assertEquals(expectedFilePath, s3ObjectSummaries.get(0).getKey());
            assertEquals(expectedFileSize, s3ObjectSummaries.get(0).getSize());
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testListDirectoryNoSuchBucket()
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX);

        try
        {
            s3Dao.listDirectory(s3FileTransferRequestParamsDto);
            fail("expected an IllegalArgumentException to be thrown, but no exceptions thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            assertEquals("thrown exception message",
                "The specified bucket '" + MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION + "' does not exist.", e.getMessage());
        }
    }

    /**
     * Test that we get an exception when trying to perform listDirectory operation without properly initialized S3FileTransferRequestParamsDto parameters.
     */
    @Test(expected = NullPointerException.class)
    public void testListDirectoryNullPointerException()
    {
        s3Dao.listDirectory(null);
    }

    @Test
    public void testListDirectoryRootKeyPrefix()
    {
        for (String s3KeyPrefix : Arrays.asList(null, BLANK_TEXT, "/"))
        {
            try
            {
                s3Dao.listDirectory(S3FileTransferRequestParamsDto.builder().withS3KeyPrefix(s3KeyPrefix).build());
                fail("Should throw an IllegalArgumentException when S3 key prefix specifies a root directory.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("Listing of S3 objects from root directory is not allowed.", e.getMessage());
            }
        }
    }

    @Test
    public void testListVersionsAssertHandleAmazonClientException()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);

            when(mockS3Operations.listVersions(any(), any())).thenThrow(new AmazonClientException("message"));

            try
            {
                s3Dao.listVersions(s3FileTransferRequestParamsDto);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(IllegalStateException.class, e.getClass());
                assertEquals("Failed to list S3 versions with prefix \"s3KeyPrefix\" from bucket \"s3BucketName\". Reason: message", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testListVersionsAssertHandleGenericAmazonS3Exception()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);

            when(mockS3Operations.listVersions(any(), any())).thenThrow(new AmazonS3Exception("message"));

            try
            {
                s3Dao.listVersions(s3FileTransferRequestParamsDto);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(IllegalStateException.class, e.getClass());
                assertEquals("Error accessing S3", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testListVersionsAssertKeyAndVersionIdMarker()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";
            String expectedKey = "key";
            String expectedVersionId = "versionId";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);

            when(mockS3Operations.listVersions(any(), any())).then(new Answer<VersionListing>()
            {
                @Override
                public VersionListing answer(InvocationOnMock invocation) throws Throwable
                {
                    ListVersionsRequest listVersionsRequest = invocation.getArgument(0);
                    String keyMarker = listVersionsRequest.getKeyMarker();
                    String versionIdMarker = listVersionsRequest.getVersionIdMarker();

                    VersionListing versionListing = new VersionListing();
                    if (keyMarker == null || versionIdMarker == null)
                    {
                        versionListing.setTruncated(true);
                        versionListing.setNextKeyMarker("nextKeyMarker");
                        versionListing.setNextVersionIdMarker("nextVersionIdMarker");
                    }
                    else
                    {
                        assertEquals("nextKeyMarker", listVersionsRequest.getKeyMarker());
                        assertEquals("nextVersionIdMarker", listVersionsRequest.getVersionIdMarker());

                        S3VersionSummary s3VersionSummary = new S3VersionSummary();
                        s3VersionSummary.setKey(expectedKey);
                        s3VersionSummary.setVersionId(expectedVersionId);
                        versionListing.getVersionSummaries().add(s3VersionSummary);
                    }
                    return versionListing;
                }
            });

            List<S3VersionSummary> result = s3Dao.listVersions(s3FileTransferRequestParamsDto);
            assertEquals(1, result.size());
            assertEquals(expectedKey, result.get(0).getKey());
            assertEquals(expectedVersionId, result.get(0).getVersionId());
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testListVersionsNoSuchBucket()
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX);

        try
        {
            s3Dao.listVersions(s3FileTransferRequestParamsDto);
            fail("expected an IllegalArgumentException to be thrown, but no exceptions thrown");
        }
        catch (Exception e)
        {
            assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            assertEquals("thrown exception message",
                "The specified bucket '" + MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION + "' does not exist.", e.getMessage());
        }
    }

    /**
     * Test that we get an exception when trying to perform listVersions operation without properly initialized S3FileTransferRequestParamsDto parameters.
     */
    @Test(expected = NullPointerException.class)
    public void testListVersionsNullPointerException()
    {
        s3Dao.listVersions(null);
    }

    @Test
    public void testListVersionsRootKeyPrefix()
    {
        for (String s3KeyPrefix : Arrays.asList(null, BLANK_TEXT, "/"))
        {
            try
            {
                s3Dao.listVersions(S3FileTransferRequestParamsDto.builder().withS3KeyPrefix(s3KeyPrefix).build());
                fail("Should throw an IllegalArgumentException when S3 key prefix specifies a root directory.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals("Listing of S3 versions from root directory is not allowed.", e.getMessage());
            }
        }
    }

    @Test
    public void testPerformTransferAssertErrorWhenTransferBytesMismatch() throws Exception
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        // Shorten the sleep interval for faster tests
        long originalSleepIntervalsMillis = (long) ReflectionTestUtils.getField(s3Dao, "sleepIntervalsMillis");
        ReflectionTestUtils.setField(s3Dao, "sleepIntervalsMillis", 1l);

        try
        {
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setLocalPath("localPath");

            when(mockS3Operations.upload(any(), any())).then(new Answer<Upload>()
            {
                @Override
                public Upload answer(InvocationOnMock invocation) throws Throwable
                {
                    Upload mockedUpload = mock(Upload.class);
                    TransferProgress transferProgress = new TransferProgress();
                    // bytesTransferred < totalBytesToTransfer should cause error
                    ReflectionTestUtils.setField(transferProgress, "bytesTransferred", 0l);
                    ReflectionTestUtils.setField(transferProgress, "totalBytesToTransfer", 1l);
                    when(mockedUpload.getProgress()).thenReturn(transferProgress);
                    when(mockedUpload.isDone()).thenReturn(true);
                    when(mockedUpload.getState()).thenReturn(TransferState.Completed);
                    return mockedUpload;
                }
            });

            try
            {
                s3Dao.uploadFile(s3FileTransferRequestParamsDto);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(IllegalArgumentException.class, e.getClass());
                assertEquals("Actual number of bytes transferred is less than expected (actual: 0 bytes; expected: 1 bytes).", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
            ReflectionTestUtils.setField(s3Dao, "sleepIntervalsMillis", originalSleepIntervalsMillis);
        }
    }

    @Test
    public void testPerformTransferAssertHandleFailedWithAmazonClientException() throws Exception
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        // Shorten the sleep interval for faster tests
        long originalSleepIntervalsMillis = (long) ReflectionTestUtils.getField(s3Dao, "sleepIntervalsMillis");
        ReflectionTestUtils.setField(s3Dao, "sleepIntervalsMillis", 1l);

        try
        {
            S3FileCopyRequestParamsDto s3FileCopyRequestParamsDto = new S3FileCopyRequestParamsDto();
            s3FileCopyRequestParamsDto.setSourceBucketName("sourceBucketName");
            s3FileCopyRequestParamsDto.setSourceObjectKey("sourceObjectKey");
            s3FileCopyRequestParamsDto.setTargetBucketName("targetBucketName");
            s3FileCopyRequestParamsDto.setTargetObjectKey("targetObjectKey");
            s3FileCopyRequestParamsDto.setKmsKeyId("kmsKeyId");

            when(mockS3Operations.copyFile(any(), any())).then(new Answer<Copy>()
            {
                @Override
                public Copy answer(InvocationOnMock invocation) throws Throwable
                {
                    Copy mockTransfer = mock(Copy.class);

                    when(mockTransfer.getProgress()).thenReturn(new TransferProgress());
                    when(mockTransfer.getState()).thenReturn(TransferState.Failed);
                    when(mockTransfer.isDone()).thenReturn(true);
                    when(mockTransfer.waitForException()).thenReturn(new AmazonClientException("message"));
                    return mockTransfer;
                }
            });

            try
            {
                s3Dao.copyFile(s3FileCopyRequestParamsDto);
                fail();
            }
            catch (Exception e)
            {
                assertEquals(AmazonClientException.class, e.getClass());
                assertEquals("message", e.getMessage());
            }
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
            ReflectionTestUtils.setField(s3Dao, "sleepIntervalsMillis", originalSleepIntervalsMillis);
        }
    }

    @Test
    public void testPerformTransferAssertLogWhenStepCountGt300() throws Exception
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        // Shorten the sleep interval for faster tests
        long originalSleepIntervalsMillis = (long) ReflectionTestUtils.getField(s3Dao, "sleepIntervalsMillis");
        ReflectionTestUtils.setField(s3Dao, "sleepIntervalsMillis", 1l);

        try
        {
            S3FileCopyRequestParamsDto s3FileCopyRequestParamsDto = new S3FileCopyRequestParamsDto();
            s3FileCopyRequestParamsDto.setSourceBucketName("sourceBucketName");
            s3FileCopyRequestParamsDto.setSourceObjectKey("sourceObjectKey");
            s3FileCopyRequestParamsDto.setTargetBucketName("targetBucketName");
            s3FileCopyRequestParamsDto.setTargetObjectKey("targetObjectKey");
            s3FileCopyRequestParamsDto.setKmsKeyId("kmsKeyId");

            when(mockS3Operations.copyFile(any(), any())).then(new Answer<Copy>()
            {
                @Override
                public Copy answer(InvocationOnMock invocation) throws Throwable
                {
                    Copy mockTransfer = mock(Copy.class);

                    when(mockTransfer.getProgress()).thenReturn(new TransferProgress());
                    when(mockTransfer.getState()).thenReturn(TransferState.Completed);
                    when(mockTransfer.isDone()).then(new Answer<Boolean>()
                    {
                        int callCount = 0;

                        @Override
                        public Boolean answer(InvocationOnMock invocation) throws Throwable
                        {
                            return callCount++ > 600;
                        }
                    });
                    return mockTransfer;
                }
            });

            s3Dao.copyFile(s3FileCopyRequestParamsDto);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
            ReflectionTestUtils.setField(s3Dao, "sleepIntervalsMillis", originalSleepIntervalsMillis);
        }
    }

    @Test
    public void testPrepareMetadataAssertSetKmsHeaders()
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            String s3BucketName = "s3BucketName";
            String s3KeyPrefix = "s3KeyPrefix";
            String kmsKeyId = "kmsKeyId";

            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(s3BucketName);
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
            s3FileTransferRequestParamsDto.setKmsKeyId(kmsKeyId);

            when(mockS3Operations.putObject(any(), any())).then(new Answer<PutObjectResult>()
            {
                @Override
                public PutObjectResult answer(InvocationOnMock invocation) throws Throwable
                {
                    PutObjectRequest putObjectRequest = invocation.getArgument(0);
                    ObjectMetadata metadata = putObjectRequest.getMetadata();
                    assertEquals("aws:kms", metadata.getSSEAlgorithm());
                    assertEquals(kmsKeyId, metadata.getRawMetadata().get(Headers.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID));
                    return new PutObjectResult();
                }
            });

            s3Dao.createDirectory(s3FileTransferRequestParamsDto);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testRestoreObjects()
    {
        // Put a 1 byte Glacier storage class file in S3.
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
        metadata.setOngoingRestore(false);
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3ManagedBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), metadata), null);

        // Initiate a restore request for the test S3 file.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(storageDaoTestHelper.getS3ManagedBucketName());
        params.setFiles(Arrays.asList(new File(TARGET_S3_KEY)));
        s3Dao.restoreObjects(params, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);

        // Validate that there is an ongoing restore request for this object.
        ObjectMetadata objectMetadata = s3Operations.getObjectMetadata(storageDaoTestHelper.getS3ManagedBucketName(), TARGET_S3_KEY, null);
        assertTrue(objectMetadata.getOngoingRestore());
    }

    @Test
    public void testRestoreObjectsAmazonServiceException()
    {
        // Build a mock file path that triggers an Amazon service exception when we request to restore an object.
        String testKey = String.format("%s/%s", TEST_S3_KEY_PREFIX, MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION);

        // Put a 1 byte Glacier storage class file in S3.
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
        metadata.setOngoingRestore(false);
        s3Operations.putObject(new PutObjectRequest(storageDaoTestHelper.getS3ManagedBucketName(), testKey, new ByteArrayInputStream(new byte[1]), metadata),
            null);

        // Try to initiate a restore request for a mocked S3 file that would trigger an Amazon service exception when we request to restore an object.
        try
        {
            S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
            params.setS3BucketName(storageDaoTestHelper.getS3ManagedBucketName());
            params.setFiles(Arrays.asList(new File(testKey)));
            s3Dao.restoreObjects(params, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail("Should throw an IllegalStateException when an S3 restore object operation fails.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Failed to initiate a restore request for \"%s\" key in \"%s\" bucket. " +
                    "Reason: InternalError (Service: null; Status Code: 0; Error Code: InternalError; Request ID: null; Proxy: null)", testKey,
                storageDaoTestHelper.getS3ManagedBucketName()), e.getMessage());
        }
    }

    @Test
    public void testRestoreObjectsEmptyList()
    {
        // Initiate a restore request for an empty list of S3 files.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setFiles(new ArrayList<>());
        s3Dao.restoreObjects(s3FileTransferRequestParamsDto, 0, ARCHIVE_RETRIEVAL_OPTION);
    }

    @Test
    public void testRestoreObjectsGlacierObjectAlreadyBeingRestored()
    {
        // Put a 1 byte Glacier storage class file in S3 flagged as already being restored.
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
        metadata.setOngoingRestore(true);
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3ManagedBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), metadata), null);

        // Initiate a restore request for the test S3 file.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(storageDaoTestHelper.getS3ManagedBucketName());
        params.setFiles(Arrays.asList(new File(TARGET_S3_KEY)));
        s3Dao.restoreObjects(params, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);

        // Validate that there is still an ongoing restore request for this object.
        ObjectMetadata objectMetadata = s3Operations.getObjectMetadata(storageDaoTestHelper.getS3ManagedBucketName(), TARGET_S3_KEY, null);
        assertTrue(objectMetadata.getOngoingRestore());
    }

    @Test
    public void testRestoreObjectsNonGlacierNonDeepArchiveObject()
    {
        // Put a 1 byte non-Glacier storage class file in S3.
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Standard);
        metadata.setOngoingRestore(false);
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3ManagedBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), metadata), null);

        // Try to initiate a restore request for a non-Glacier file.
        try
        {
            S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
            params.setS3BucketName(storageDaoTestHelper.getS3ManagedBucketName());
            params.setFiles(Arrays.asList(new File(TARGET_S3_KEY)));
            s3Dao.restoreObjects(params, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail("Should throw an IllegalStateException when file has a non-Glacier storage class.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Failed to initiate a restore request for \"%s\" key in \"%s\" bucket. " +
                    "Reason: object is not in Glacier or DeepArchive (Service: null; Status Code: 0; Error Code: null; Request ID: null; Proxy: null)",
                TARGET_S3_KEY, storageDaoTestHelper.getS3ManagedBucketName()), e.getMessage());
        }
    }

    /**
     * The method is successful when both bucket and key exists.
     */
    @Test
    public void testS3FileExists()
    {
        String expectedKey = "foo";
        String expectedValue = "bar";

        ByteArrayInputStream inputStream = new ByteArrayInputStream((expectedKey + "=" + expectedValue).getBytes());
        PutObjectRequest putObjectRequest = new PutObjectRequest(S3_BUCKET_NAME, TARGET_S3_KEY, inputStream, new ObjectMetadata());
        s3Operations.putObject(putObjectRequest, null);

        S3FileTransferRequestParamsDto params = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        params.setS3BucketName(S3_BUCKET_NAME);
        params.setS3KeyPrefix(TARGET_S3_KEY);
        Assert.assertTrue(s3Dao.s3FileExists(params));
    }

    /**
     * Throws an ObjectNotFoundException when application does not have access to the given object.
     */
    @Test
    public void testS3FileExistsAccessDenied()
    {
        try
        {
            S3FileTransferRequestParamsDto params = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
            params.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_ACCESS_DENIED);
            params.setS3KeyPrefix(TARGET_S3_KEY);
            s3Dao.s3FileExists(params);
            Assert.fail("expected ObjectNotFoundException to be thrown, but no exceptions were thrown");
        }
        catch (ObjectNotFoundException e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());
            Assert.assertEquals("thrown exception message",
                "Application does not have access to the specified S3 object at bucket '" + MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_ACCESS_DENIED +
                    "' and key '" + TARGET_S3_KEY + "'.", e.getMessage());
        }
    }

    /**
     * Throws an ObjectNotFoundException when S3 bucket does not exist. This should result as a 404 to clients.
     */
    @Test
    public void testS3FileExistsBucketNoExists()
    {
        try
        {
            S3FileTransferRequestParamsDto params = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
            params.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION);
            params.setS3KeyPrefix(TARGET_S3_KEY);
            s3Dao.s3FileExists(params);
            Assert.fail("expected ObjectNotFoundException to be thrown, but no exceptions were thrown");
        }
        catch (ObjectNotFoundException e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());
            Assert.assertEquals("thrown exception message",
                "Specified S3 bucket '" + MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION + "' does not exist.", e.getMessage());
        }
    }

    /**
     * Throws the exception as-is without wrapping if the exception is of type AmazonServiceException or children.
     */
    @Test
    public void testS3FileExistsGenericAmazonError()
    {
        try
        {
            S3FileTransferRequestParamsDto params = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
            params.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_INTERNAL_ERROR);
            params.setS3KeyPrefix(TARGET_S3_KEY);
            s3Dao.s3FileExists(params);
            Assert.fail("expected AmazonServiceException to be thrown, but no exceptions were thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", AmazonServiceException.class, e.getClass());
        }
    }

    @Test
    public void testS3FileExistsKeyNoExists()
    {
        S3FileTransferRequestParamsDto params = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        params.setS3BucketName(S3_BUCKET_NAME);
        params.setS3KeyPrefix(TARGET_S3_KEY);
        Assert.assertFalse(s3Dao.s3FileExists(params));
    }

    @Test
    public void testTagObjects()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(S3_BUCKET_NAME);

        // Create an S3 object summary.
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(TARGET_S3_KEY);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Put a file in S3.
        s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), new ObjectMetadata()), null);

        // Tag the file with an S3 object tag.
        s3Dao.tagObjects(params, new S3FileTransferRequestParamsDto(), Collections.singletonList(s3ObjectSummary), tag);

        // Validate that the object got tagged.
        GetObjectTaggingResult getObjectTaggingResult = s3Operations.getObjectTagging(new GetObjectTaggingRequest(S3_BUCKET_NAME, TARGET_S3_KEY), null);
        assertEquals(Collections.singletonList(tag), getObjectTaggingResult.getTagSet());
    }

    @Test
    public void testTagObjectsAmazonServiceException()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects with a mocked S3 bucket name that would trigger an AWS exception.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_INTERNAL_ERROR);

        // Create an S3 object summary.
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(TARGET_S3_KEY);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        try
        {
            s3Dao.tagObjects(params, new S3FileTransferRequestParamsDto(), Collections.singletonList(s3ObjectSummary), tag);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Failed to tag S3 object with \"%s\" key and \"null\" version id in \"%s\" bucket. " +
                    "Reason: InternalError (Service: null; Status Code: 0; Error Code: InternalError; Request ID: null; Proxy: null)", TARGET_S3_KEY,
                MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_INTERNAL_ERROR), e.getMessage());
        }
    }

    @Test
    public void testTagObjectsOtherTagKeyAlreadyExists()
    {
        // Create two S3 object tags having different tag keys.
        List<Tag> tags = Arrays.asList(new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE), new Tag(S3_OBJECT_TAG_KEY_2, S3_OBJECT_TAG_VALUE_2));

        // Put a file in S3 that is already tagged with the first S3 object tag.
        PutObjectRequest putObjectRequest = new PutObjectRequest(S3_BUCKET_NAME, TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), new ObjectMetadata());
        putObjectRequest.setTagging(new ObjectTagging(Collections.singletonList(tags.get(0))));
        s3Operations.putObject(putObjectRequest, null);

        // Validate that the S3 object is tagged with the first tag only.
        GetObjectTaggingResult getObjectTaggingResult = s3Operations.getObjectTagging(new GetObjectTaggingRequest(S3_BUCKET_NAME, TARGET_S3_KEY), null);
        assertEquals(Collections.singletonList(tags.get(0)), getObjectTaggingResult.getTagSet());

        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(S3_BUCKET_NAME);

        // Create an S3 object summary.
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(TARGET_S3_KEY);

        // Tag the S3 file with the second S3 object tag.
        s3Dao.tagObjects(params, new S3FileTransferRequestParamsDto(), Collections.singletonList(s3ObjectSummary), tags.get(1));

        // Validate that the S3 object is now tagged with both tags.
        getObjectTaggingResult = s3Operations.getObjectTagging(new GetObjectTaggingRequest(S3_BUCKET_NAME, TARGET_S3_KEY), null);
        assertEquals(tags.size(), getObjectTaggingResult.getTagSet().size());
        assertTrue(getObjectTaggingResult.getTagSet().containsAll(tags));
    }

    @Test
    public void testTagObjectsTargetTagKeyAlreadyExists()
    {
        // Create two S3 object tags having the same tag key.
        List<Tag> tags = Arrays.asList(new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE), new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE_2));

        // Put a file in S3 that is already tagged with the first S3 object tag.
        PutObjectRequest putObjectRequest = new PutObjectRequest(S3_BUCKET_NAME, TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), new ObjectMetadata());
        putObjectRequest.setTagging(new ObjectTagging(Collections.singletonList(tags.get(0))));
        s3Operations.putObject(putObjectRequest, null);

        // Validate that the S3 object is tagged with the first tag.
        GetObjectTaggingResult getObjectTaggingResult = s3Operations.getObjectTagging(new GetObjectTaggingRequest(S3_BUCKET_NAME, TARGET_S3_KEY), null);
        assertEquals(Collections.singletonList(tags.get(0)), getObjectTaggingResult.getTagSet());

        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(S3_BUCKET_NAME);

        // Create an S3 object summary.
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(TARGET_S3_KEY);

        // Tag the S3 file with the second S3 object tag.
        s3Dao.tagObjects(params, new S3FileTransferRequestParamsDto(), Collections.singletonList(s3ObjectSummary), tags.get(1));

        // Validate that the S3 object is tagged with the second tag now.
        getObjectTaggingResult = s3Operations.getObjectTagging(new GetObjectTaggingRequest(S3_BUCKET_NAME, TARGET_S3_KEY), null);
        assertEquals(Collections.singletonList(tags.get(1)), getObjectTaggingResult.getTagSet());
    }

    @Test
    public void testTagVersionsAmazonServiceException()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects with a mocked S3 bucket name that would trigger an AWS exception.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_INTERNAL_ERROR);

        // Create an S3 version summary.
        S3VersionSummary s3VersionSummary = new S3VersionSummary();
        s3VersionSummary.setKey(S3_KEY);
        s3VersionSummary.setVersionId(S3_VERSION_ID);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        try
        {
            s3Dao.tagVersions(params, new S3FileTransferRequestParamsDto(), Collections.singletonList(s3VersionSummary), tag);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Failed to tag S3 object with \"%s\" key and \"%s\" version id in \"%s\" bucket. " +
                    "Reason: InternalError (Service: null; Status Code: 0; Error Code: InternalError; Request ID: null; Proxy: null)", S3_KEY, S3_VERSION_ID,
                MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_INTERNAL_ERROR), e.getMessage());
        }
    }

    @Test
    public void testTagVersionsOtherTagKeyAlreadyExists()
    {
        // Create two S3 object tags having different tag keys.
        List<Tag> tags = Arrays.asList(new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE), new Tag(S3_OBJECT_TAG_KEY_2, S3_OBJECT_TAG_VALUE_2));

        // Put an S3 file that is already tagged with the first S3 object tag in an S3 bucket that has versioning enabled.
        PutObjectRequest putObjectRequest =
            new PutObjectRequest(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED, TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]),
                new ObjectMetadata());
        putObjectRequest.setTagging(new ObjectTagging(Collections.singletonList(tags.get(0))));
        s3Operations.putObject(putObjectRequest, null);

        // List S3 versions that match the test S3 key.
        ListVersionsRequest listVersionsRequest =
            new ListVersionsRequest().withBucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED).withPrefix(TARGET_S3_KEY);
        VersionListing versionListing = s3Operations.listVersions(listVersionsRequest, null);
        assertEquals(1, CollectionUtils.size(versionListing.getVersionSummaries()));
        assertEquals(TARGET_S3_KEY, versionListing.getVersionSummaries().get(0).getKey());
        assertNotNull(versionListing.getVersionSummaries().get(0).getVersionId());

        // Validate that the S3 object is tagged with the first tag only.
        GetObjectTaggingResult getObjectTaggingResult = s3Operations.getObjectTagging(
            new GetObjectTaggingRequest(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED, TARGET_S3_KEY,
                versionListing.getVersionSummaries().get(0).getVersionId()), null);
        assertEquals(Collections.singletonList(tags.get(0)), getObjectTaggingResult.getTagSet());

        // Tag the S3 version with the second S3 object tag.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED);
        s3Dao.tagVersions(params, new S3FileTransferRequestParamsDto(), versionListing.getVersionSummaries(), tags.get(1));

        // Validate that the S3 object is now tagged with both tags.
        getObjectTaggingResult = s3Operations.getObjectTagging(
            new GetObjectTaggingRequest(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED, TARGET_S3_KEY,
                versionListing.getVersionSummaries().get(0).getVersionId()), null);
        assertEquals(tags.size(), getObjectTaggingResult.getTagSet().size());
        assertTrue(getObjectTaggingResult.getTagSet().containsAll(tags));
    }

    @Test
    public void testTagVersionsS3BucketWithVersioningDisabled()
    {
        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Put S3 objects in S3 bucket that has versioning disabled.
        for (int i = 0; i < 2; i++)
        {
            s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, TARGET_S3_KEY + i, new ByteArrayInputStream(new byte[1]), new ObjectMetadata()), null);
        }

        // List S3 versions that match the test prefix.
        ListVersionsRequest listVersionsRequest = new ListVersionsRequest().withBucketName(S3_BUCKET_NAME).withPrefix(TARGET_S3_KEY);
        VersionListing versionListing = s3Operations.listVersions(listVersionsRequest, null);
        assertEquals(2, CollectionUtils.size(versionListing.getVersionSummaries()));
        for (int i = 0; i < 2; i++)
        {
            assertNull(versionListing.getVersionSummaries().get(i).getVersionId());
        }

        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(S3_BUCKET_NAME);

        // Tag listed S3 versions with an S3 object tag.
        s3Dao.tagVersions(params, new S3FileTransferRequestParamsDto(), versionListing.getVersionSummaries(), tag);

        // Validate that both S3 objects got tagged.
        for (int i = 0; i < 2; i++)
        {
            GetObjectTaggingResult getObjectTaggingResult =
                s3Operations.getObjectTagging(new GetObjectTaggingRequest(S3_BUCKET_NAME, TARGET_S3_KEY + i, null), null);
            assertEquals(Collections.singletonList(tag), getObjectTaggingResult.getTagSet());
        }
    }

    @Test
    public void testTagVersionsS3BucketWithVersioningEnabled()
    {
        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Put two S3 versions in S3 bucket that has versioning enabled.
        for (int i = 0; i < 2; i++)
        {
            s3Operations.putObject(
                new PutObjectRequest(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED, TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]),
                    new ObjectMetadata()), null);
        }

        // List S3 versions that match the test key.
        ListVersionsRequest listVersionsRequest =
            new ListVersionsRequest().withBucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED).withPrefix(TARGET_S3_KEY);
        VersionListing versionListing = s3Operations.listVersions(listVersionsRequest, null);
        assertEquals(2, CollectionUtils.size(versionListing.getVersionSummaries()));
        for (int i = 0; i < 2; i++)
        {
            assertNotNull(versionListing.getVersionSummaries().get(i).getVersionId());
        }

        // Create an S3 file transfer request parameters DTO to access S3 objects with a mocked S3 bucket name that enables S3 bucket versioning.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED);

        // Tag listed S3 version with an S3 object tag.
        s3Dao.tagVersions(params, new S3FileTransferRequestParamsDto(), versionListing.getVersionSummaries(), tag);

        // Validate that both versions got tagged.
        for (int i = 0; i < 2; i++)
        {
            GetObjectTaggingResult getObjectTaggingResult = s3Operations.getObjectTagging(
                new GetObjectTaggingRequest(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED, TARGET_S3_KEY,
                    versionListing.getVersionSummaries().get(i).getVersionId()), null);
            assertEquals(Collections.singletonList(tag), getObjectTaggingResult.getTagSet());
        }
    }

    @Test
    public void testTagVersionsTargetTagKeyAlreadyExists()
    {
        // Create two S3 object tags having the same tag key.
        List<Tag> tags = Arrays.asList(new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE), new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE_2));

        // Put an S3 file that is already tagged with the first S3 object tag in an S3 bucket that has versioning enabled.
        PutObjectRequest putObjectRequest =
            new PutObjectRequest(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED, TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]),
                new ObjectMetadata());
        putObjectRequest.setTagging(new ObjectTagging(Collections.singletonList(tags.get(0))));
        s3Operations.putObject(putObjectRequest, null);

        // List S3 versions that match the test S3 key.
        ListVersionsRequest listVersionsRequest =
            new ListVersionsRequest().withBucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED).withPrefix(TARGET_S3_KEY);
        VersionListing versionListing = s3Operations.listVersions(listVersionsRequest, null);
        assertEquals(1, CollectionUtils.size(versionListing.getVersionSummaries()));
        assertEquals(TARGET_S3_KEY, versionListing.getVersionSummaries().get(0).getKey());
        assertNotNull(versionListing.getVersionSummaries().get(0).getVersionId());

        // Validate that the S3 object is tagged with the first tag only.
        GetObjectTaggingResult getObjectTaggingResult = s3Operations.getObjectTagging(
            new GetObjectTaggingRequest(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED, TARGET_S3_KEY,
                versionListing.getVersionSummaries().get(0).getVersionId()), null);
        assertEquals(Collections.singletonList(tags.get(0)), getObjectTaggingResult.getTagSet());

        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED);

        // Tag the S3 version with the second S3 object tag.
        s3Dao.tagVersions(params, new S3FileTransferRequestParamsDto(), versionListing.getVersionSummaries(), tags.get(1));

        // Validate that the S3 object is now tagged with the second tag only.
        getObjectTaggingResult = s3Operations.getObjectTagging(
            new GetObjectTaggingRequest(MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED, TARGET_S3_KEY,
                versionListing.getVersionSummaries().get(0).getVersionId()), null);
        assertEquals(Collections.singletonList(tags.get(1)), getObjectTaggingResult.getTagSet());
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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
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
        s3DaoTestHelper.validateS3FileUpload(s3FileTransferRequestParamsDto, expectedKeys);
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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX);
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setRecursive(true);
        S3FileTransferResultsDto results = s3Dao.uploadDirectory(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == NUM_OF_FILES);

        // Validate the empty folder upload.
        Assert.assertEquals(NUM_OF_FILES, s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());
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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX);
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setRecursive(true);
        S3FileTransferResultsDto results = s3Dao.uploadDirectory(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate the zero bytes upload.
        Assert.assertEquals(1, s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());
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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3FileTransferRequestParamsDto.setLocalPath(targetFile.getPath());
        S3FileTransferResultsDto results = s3Dao.uploadFile(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate the file upload.
        s3DaoTestHelper.validateS3FileUpload(s3FileTransferRequestParamsDto, Arrays.asList(TARGET_S3_KEY));
    }

    /**
     * Test that we are able to perform the uploadFileList S3Dao operation on S3 using our DAO tier.
     */
    @Test
    public void testUploadFileList() throws IOException, InterruptedException
    {
        runUploadFileListTest();
    }

    /**
     * Test that we are able to perform the uploadFileList S3Dao operation on S3 using our DAO tier with logger level set to INFO.
     */
    @Test
    public void testUploadFileListWithLoggerLevelSetToInfo() throws IOException, InterruptedException
    {
        String loggerName = S3DaoImpl.class.getName();
        LogLevel origLoggerLevel = getLogLevel(loggerName);
        setLogLevel(loggerName, LogLevel.INFO);

        try
        {
            runUploadFileListTest();
        }
        finally
        {
            setLogLevel(loggerName, origLoggerLevel);
        }
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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3FileTransferRequestParamsDto.setLocalPath(targetFile.getPath());
        s3FileTransferRequestParamsDto.setUseRrs(true);
        S3FileTransferResultsDto results = s3Dao.uploadFile(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate the file upload.
        s3DaoTestHelper.validateS3FileUpload(s3FileTransferRequestParamsDto, Arrays.asList(TARGET_S3_KEY));

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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3FileTransferRequestParamsDto.setLocalPath(targetFile.getPath());
        s3FileTransferRequestParamsDto.setMaxThreads(3);
        S3FileTransferResultsDto results = s3Dao.uploadFile(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate the file upload.
        s3DaoTestHelper.validateS3FileUpload(s3FileTransferRequestParamsDto, Arrays.asList(TARGET_S3_KEY));
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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3FileTransferRequestParamsDto.setLocalPath(targetFile.getPath());
        S3FileTransferResultsDto results = s3Dao.uploadFile(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == 1L);

        // Validate the file upload.
        s3DaoTestHelper.validateS3FileUpload(s3FileTransferRequestParamsDto, Arrays.asList(TARGET_S3_KEY));
    }

    @Test
    public void testValidateGlacierS3FilesRestored()
    {
        // Put a 1 byte already restored Glacier storage class file in S3.
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
        metadata.setOngoingRestore(false);
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3ManagedBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), metadata), null);

        // Validate the file.
        S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(storageDaoTestHelper.getS3ManagedBucketName());
        params.setFiles(Arrays.asList(new File(TARGET_S3_KEY)));
        s3Dao.validateGlacierS3FilesRestored(params);
    }

    @Test
    public void testValidateGlacierS3FilesRestoredAmazonServiceException()
    {
        // Build a mock file path that triggers an Amazon service exception when we request S3 metadata for the object.
        String testKey = String.format("%s/%s", TEST_S3_KEY_PREFIX, MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION);

        // Put a 1 byte Glacier storage class file in S3.
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
        metadata.setOngoingRestore(false);
        s3Operations.putObject(new PutObjectRequest(storageDaoTestHelper.getS3ManagedBucketName(), testKey, new ByteArrayInputStream(new byte[1]), metadata),
            null);

        // Try to validate if the Glacier S3 file is already restored for a mocked S3 file
        // that triggers an Amazon service exception when we request S3 metadata for the object.
        try
        {
            S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
            params.setS3BucketName(storageDaoTestHelper.getS3ManagedBucketName());
            params.setFiles(Arrays.asList(new File(testKey)));
            s3Dao.validateGlacierS3FilesRestored(params);
            fail("Should throw an IllegalStateException when Glacier S3 object validation fails due to an Amazon service exception.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Fail to check restore status for \"%s\" key in \"%s\" bucket. " +
                    "Reason: InternalError (Service: null; Status Code: 0; Error Code: InternalError; Request ID: null; Proxy: null)", testKey,
                storageDaoTestHelper.getS3ManagedBucketName()), e.getMessage());
        }
    }

    @Test
    public void testValidateGlacierS3FilesRestoredEmptyList()
    {
        // Make a call to validate Glacier S3 files being restored for an empty list of S3 files.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setFiles(new ArrayList<>());
        s3Dao.validateGlacierS3FilesRestored(s3FileTransferRequestParamsDto);
    }

    @Test
    public void testValidateGlacierS3FilesRestoredGlacierObjectRestoreInProgress()
    {
        // Put a 1 byte Glacier storage class file in S3 that is still being restored (OngoingRestore flag is true).
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
        metadata.setOngoingRestore(true);
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3ManagedBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), metadata), null);

        // Try to validate if the Glacier S3 file is already restored.
        try
        {
            S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
            params.setS3BucketName(storageDaoTestHelper.getS3ManagedBucketName());
            params.setFiles(Arrays.asList(new File(TARGET_S3_KEY)));
            s3Dao.validateGlacierS3FilesRestored(params);
            fail("Should throw an IllegalArgumentException when Glacier S3 file is not restored.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Archived S3 file \"%s\" is not restored. StorageClass {GLACIER}, OngoingRestore flag {true}, S3 bucket name {%s}", TARGET_S3_KEY,
                    storageDaoTestHelper.getS3ManagedBucketName()), e.getMessage());
        }
    }

    @Test
    public void testValidateGlacierS3FilesRestoredGlacierObjectRestoreNotInitiated()
    {
        // Put a 1 byte Glacier storage class file in S3 that has no restore initiated (OngoingRestore flag is null).
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3ManagedBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[1]), metadata), null);

        // Try to validate if the Glacier S3 file is already restored.
        try
        {
            S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
            params.setS3BucketName(storageDaoTestHelper.getS3ManagedBucketName());
            params.setFiles(Arrays.asList(new File(TARGET_S3_KEY)));
            s3Dao.validateGlacierS3FilesRestored(params);
            fail("Should throw an IllegalArgumentException when Glacier S3 file is not restored.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Archived S3 file \"%s\" is not restored. StorageClass {GLACIER}, OngoingRestore flag {null}, S3 bucket name {%s}", TARGET_S3_KEY,
                    storageDaoTestHelper.getS3ManagedBucketName()), e.getMessage());
        }
    }

    @Test
    public void testValidateS3File() throws IOException, InterruptedException
    {
        // Put a 1 KB file in S3.
        PutObjectRequest putObjectRequest =
            new PutObjectRequest(storageDaoTestHelper.getS3ManagedBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]), null);
        s3Operations.putObject(putObjectRequest, null);

        // Validate the S3 file.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3Dao.validateS3File(s3FileTransferRequestParamsDto, FILE_SIZE_1_KB);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testValidateS3FileObjectNotFoundException() throws IOException, InterruptedException
    {
        // Try to validate a non-existing S3 file.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3Dao.validateS3File(s3FileTransferRequestParamsDto, FILE_SIZE_1_KB);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateS3FileRuntimeExceptionFileSizeDoesNotMatch() throws IOException, InterruptedException
    {
        // Put a 1 KB file in S3.
        PutObjectRequest putObjectRequest =
            new PutObjectRequest(storageDaoTestHelper.getS3ManagedBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]), null);
        s3Operations.putObject(putObjectRequest, null);

        // Try to validate an S3 file by specifying an incorrect file size.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TARGET_S3_KEY);
        s3Dao.validateS3File(s3FileTransferRequestParamsDto, FILE_SIZE_1_KB + 999);
    }

    @Test
    public void testValidateS3FileSkipSizeValidationWhenSizeIsNull() throws IOException, InterruptedException
    {
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        S3Operations mockS3Operations = mock(S3Operations.class);
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
            when(mockS3Operations.getObjectMetadata(any(), any(), any())).thenReturn(new ObjectMetadata());
            s3Dao.validateS3File(s3FileTransferRequestParamsDto, null);
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    private void runUploadFileListTest() throws IOException, InterruptedException
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
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3KeyPrefix(TEST_S3_KEY_PREFIX + "/");
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setFiles(requestFileList);
        S3FileTransferResultsDto results = s3Dao.uploadFileList(s3FileTransferRequestParamsDto);

        // Validate results.
        Assert.assertTrue(results.getTotalFilesTransferred() == LOCAL_FILES_SUBSET.size());

        // Validate the upload.
        s3DaoTestHelper.validateS3FileUpload(s3FileTransferRequestParamsDto, expectedKeys);
    }

    @Test
    public void testCreateBatchRestoreJob()
    {
        // Initialize test values
        final String bucketName = storageDaoTestHelper.getS3ManagedBucketName();
        final String expectedJobId = UUID.randomUUID().toString();

        final S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(bucketName);
        params.setFiles(Arrays.asList(new File(TARGET_S3_KEY)));
        params.setAwsRegionName("us-east-1");

        ArgumentCaptor<PutObjectRequest> uploadArgumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<CreateJobRequest> createJobRequestArgumentCaptor = ArgumentCaptor.forClass(CreateJobRequest.class);

        // Create mocks
        S3Operations mockS3Operations = mock(S3Operations.class);
        CreateJobResult mockCreateJobResult = mock(CreateJobResult.class);

        // Configure mocks
        when(mockS3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(TransferState.Completed);
            return mockedUpload;
        });

        when(mockCreateJobResult.getJobId()).thenReturn(expectedJobId);
        when(mockS3Operations.createBatchJob(any(CreateJobRequest.class), any(AWSS3Control.class))).thenReturn(mockCreateJobResult);

        // Setup s3Dao mocks
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            // Execute target method
            String jobId = s3Dao.createBatchRestoreJob(params, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            assertEquals(expectedJobId, jobId);

            // Verify interactions
            verify(mockS3Operations).upload(uploadArgumentCaptor.capture(), any());

            PutObjectRequest putRequest = uploadArgumentCaptor.getValue();
            assertNotNull(putRequest);

            verify(mockS3Operations).createBatchJob(createJobRequestArgumentCaptor.capture(), any(AWSS3Control.class));

            CreateJobRequest createJobRequest = createJobRequestArgumentCaptor.getValue();
            assertNotNull(createJobRequest);
            assertNotNull(createJobRequest.getOperation().getS3InitiateRestoreObject());
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }

    @Test
    public void testCreateBatchRestoreJobUploadFailed()
    {
        // Initialize test values
        final String bucketName = storageDaoTestHelper.getS3ManagedBucketName();
        final String expectedJobId = UUID.randomUUID().toString();

        final S3FileTransferRequestParamsDto params = new S3FileTransferRequestParamsDto();
        params.setS3BucketName(bucketName);
        params.setFiles(Arrays.asList(new File(TARGET_S3_KEY)));
        params.setAwsRegionName("us-east-1");

        // Create mocks
        S3Operations mockS3Operations = mock(S3Operations.class);
        CreateJobResult mockCreateJobResult = mock(CreateJobResult.class);

        // Configure mocks
        when(mockS3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(TransferState.Failed);
            return mockedUpload;
        });

        when(mockCreateJobResult.getJobId()).thenReturn(expectedJobId);
        when(mockS3Operations.createBatchJob(any(CreateJobRequest.class), any(AWSS3Control.class))).thenReturn(mockCreateJobResult);

        // Replace s3Dao.s3Operations with mock
        S3Operations originalS3Operations = (S3Operations) ReflectionTestUtils.getField(s3Dao, "s3Operations");
        ReflectionTestUtils.setField(s3Dao, "s3Operations", mockS3Operations);

        try
        {
            // Execute target method
            s3Dao.createBatchRestoreJob(params, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail("Should throw an IllegalStateException when upload failed.");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().startsWith(String.format("Failed to initiate")));
        }
        finally
        {
            ReflectionTestUtils.setField(s3Dao, "s3Operations", originalS3Operations);
        }
    }
}
