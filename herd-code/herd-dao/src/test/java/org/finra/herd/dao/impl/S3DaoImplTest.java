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
package org.finra.herd.dao.impl;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingResult;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.Tier;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.BadRequestException;
import com.amazonaws.services.s3control.model.CreateJobRequest;
import com.amazonaws.services.s3control.model.CreateJobResult;
import com.amazonaws.services.s3control.model.DescribeJobRequest;
import com.amazonaws.services.s3control.model.DescribeJobResult;
import com.amazonaws.services.s3control.model.InternalServiceException;
import com.amazonaws.services.s3control.model.JobDescriptor;
import com.amazonaws.services.s3control.model.JobStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.AwsS3ClientFactory;
import org.finra.herd.dao.S3Operations;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.helper.S3BatchHelper;
import org.finra.herd.model.dto.BatchJobConfigDto;
import org.finra.herd.model.dto.BatchJobManifestDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.S3ObjectTaggerRoleParamsDto;

/**
 * This class tests functionality within the S3 DAO implementation.
 */
public class S3DaoImplTest extends AbstractDaoTest
{
    private static final String OTHER_EXCEPTION_MESSAGE = "OtherExceptionMessage";

    private static final String RESTORE_ALREADY_IN_PROGRESS_EXCEPTION_MESSAGE = "RestoreAlreadyInProgress";

    private static final String TEST_FILE = "UT_S3DaoImplTest_Test_File";

    @Mock
    private AwsS3ClientFactory awsS3ClientFactory;

    @Mock
    private AwsHelper awsHelper;

    @Mock
    private S3BatchHelper batchHelper;

    @Mock
    private JavaPropertiesHelper javaPropertiesHelper;

    @Mock
    private JsonHelper jsonHelper;

    @InjectMocks
    private S3DaoImpl s3DaoImpl;

    @Mock
    private S3Operations s3Operations;

    @Mock
    private AmazonS3Client s3Client;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDeleteDirectoryMultiObjectDeleteException()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(S3_KEY_PREFIX);

        // Create an S3 version summary.
        S3VersionSummary s3VersionSummary = new S3VersionSummary();
        s3VersionSummary.setKey(S3_KEY);
        s3VersionSummary.setVersionId(S3_VERSION_ID);

        // Create a version listing.
        VersionListing versionListing = new VersionListing();
        versionListing.setVersionSummaries(Collections.singletonList(s3VersionSummary));

        // Create a delete error.
        MultiObjectDeleteException.DeleteError deleteError = new MultiObjectDeleteException.DeleteError();
        deleteError.setKey(S3_KEY);
        deleteError.setVersionId(S3_VERSION_ID);
        deleteError.setCode(ERROR_CODE);
        deleteError.setMessage(ERROR_MESSAGE);

        // Create a multi object delete exception.
        MultiObjectDeleteException multiObjectDeleteException = new MultiObjectDeleteException(Collections.singletonList(deleteError), new ArrayList<>());

        // Mock the external calls.
        when(awsS3ClientFactory.getAmazonS3Client(any(S3FileTransferRequestParamsDto.class))).thenReturn(s3Client);
        when(s3Operations.listVersions(any(ListVersionsRequest.class), any(AmazonS3Client.class))).thenReturn(versionListing);
        when(s3Operations.deleteObjects(any(DeleteObjectsRequest.class), any(AmazonS3Client.class))).thenThrow(multiObjectDeleteException);

        // Try to call the method under test.
        try
        {
            s3DaoImpl.deleteDirectory(s3FileTransferRequestParamsDto);
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format(
                "Failed to delete keys/key versions with prefix \"%s\" from bucket \"%s\". Reason: One or more objects could not be deleted " +
                    "(Service: null; Status Code: 0; Error Code: null; Request ID: null; S3 Extended Request ID: null; Proxy: null)", S3_KEY_PREFIX,
                S3_BUCKET_NAME), e.getMessage());
        }

        // Verify the external calls.
        verify(awsS3ClientFactory, times(2)).getAmazonS3Client(any(S3FileTransferRequestParamsDto.class));
        verify(s3Operations).listVersions(any(ListVersionsRequest.class), any(AmazonS3Client.class));
        verify(s3Operations).deleteObjects(any(DeleteObjectsRequest.class), any(AmazonS3Client.class));
        verify(s3Client, times(2)).shutdown();
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testRestoreObjectsInDeepArchiveWithExpeditedArchiveRetrievalOption()
    {
        List<File> files = Collections.singletonList(new File(TEST_FILE));

        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(S3_KEY_PREFIX);
        s3FileTransferRequestParamsDto.setFiles(files);

        // Create an Object Metadata with DeepArchive storage class.
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setOngoingRestore(false);
        objectMetadata.setHeader(Headers.STORAGE_CLASS, StorageClass.DeepArchive);

        ArgumentCaptor<AmazonS3Client> s3ClientCaptor = ArgumentCaptor.forClass(AmazonS3Client.class);
        ArgumentCaptor<String> s3BucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);

        // Mock the external calls.
        when(awsS3ClientFactory.getAmazonS3Client(any(S3FileTransferRequestParamsDto.class))).thenReturn(s3Client);
        when(s3Operations.getObjectMetadata(s3BucketNameCaptor.capture(), keyCaptor.capture(), s3ClientCaptor.capture())).thenReturn(objectMetadata);

        doThrow(new AmazonServiceException("Retrieval option is not supported by this storage class")).when(s3Operations)
            .restoreObject(any(RestoreObjectRequest.class), any(AmazonS3.class));

        try
        {
            s3DaoImpl.restoreObjects(s3FileTransferRequestParamsDto, EXPIRATION_IN_DAYS, Tier.Expedited.toString());
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Failed to initiate a restore request for \"%s\" key in \"%s\" bucket. " +
                    "Reason: Retrieval option is not supported by this storage class (Service: null; Status Code: 0; Error Code: null; Request ID: null; Proxy: null)",
                TEST_FILE, S3_BUCKET_NAME), e.getMessage());
        }
    }

    @Test
    public void testDeleteDirectoryNoS3VersionsExist()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(S3_KEY_PREFIX);

        // Create an empty version listing.
        VersionListing versionListing = new VersionListing();

        // Mock the external calls.
        when(awsS3ClientFactory.getAmazonS3Client(any(S3FileTransferRequestParamsDto.class))).thenReturn(s3Client);
        when(s3Operations.listVersions(any(ListVersionsRequest.class), any(AmazonS3Client.class))).thenReturn(versionListing);

        // Call the method under test.
        s3DaoImpl.deleteDirectory(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(awsS3ClientFactory).getAmazonS3Client(any(S3FileTransferRequestParamsDto.class));
        verify(s3Operations).listVersions(any(ListVersionsRequest.class), any(AmazonS3Client.class));
        verify(s3Client).shutdown();
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testTagObjects()
    {
        runTagObjectsTest();
    }

    @Test
    public void testTagObjectsNoS3ObjectSummaries()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);

        // Create an S3 object tagger role parameters DTO.
        S3ObjectTaggerRoleParamsDto s3ObjectTaggerRoleParamsDto =
            new S3ObjectTaggerRoleParamsDto(S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, S3_OBJECT_TAGGER_ROLE_SESSION_DURATION_SECONDS);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Call the method under test with a list of S3 object summaries passed as null.
        s3DaoImpl.tagObjects(s3FileTransferRequestParamsDto, s3ObjectTaggerRoleParamsDto, null, tag);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testTagObjectsS3ClientCreationFails()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);

        // Create an S3 object tagger role parameters DTO.
        S3ObjectTaggerRoleParamsDto s3ObjectTaggerRoleParamsDto =
            new S3ObjectTaggerRoleParamsDto(S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, S3_OBJECT_TAGGER_ROLE_SESSION_DURATION_SECONDS);

        // Create an S3 object summary.
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(S3_KEY);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Mock the external calls.
        when(awsS3ClientFactory.getAmazonS3Client(any(S3FileTransferRequestParamsDto.class))).thenThrow(new AmazonServiceException(ERROR_MESSAGE));

        // Try to call the method under test.
        try
        {
            s3DaoImpl.tagObjects(s3FileTransferRequestParamsDto, s3ObjectTaggerRoleParamsDto, Collections.singletonList(s3ObjectSummary), tag);
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Failed to tag S3 object with \"%s\" key and \"null\" version id in \"%s\" bucket. " +
                    "Reason: %s (Service: null; Status Code: 0; Error Code: null; Request ID: null; Proxy: null)", S3_KEY, S3_BUCKET_NAME, ERROR_MESSAGE),
                e.getMessage());
        }

        // Verify the external calls.
        verify(awsS3ClientFactory).getAmazonS3Client(any(S3FileTransferRequestParamsDto.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testTagObjectsWithLoggerLevelSetToInfo()
    {
        String loggerName = S3DaoImpl.class.getName();
        LogLevel origLoggerLevel = getLogLevel(loggerName);
        setLogLevel(loggerName, LogLevel.INFO);

        try
        {
            runTagObjectsTest();
        }
        finally
        {
            setLogLevel(loggerName, origLoggerLevel);
        }
    }

    @Test
    public void testTagVersions()
    {
        runTagVersionsTest();
    }

    @Test
    public void testTagVersionsNoS3VersionSummaries()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);

        // Create an S3 object tagger role parameters DTO.
        S3ObjectTaggerRoleParamsDto s3ObjectTaggerRoleParamsDto =
            new S3ObjectTaggerRoleParamsDto(S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, S3_OBJECT_TAGGER_ROLE_SESSION_DURATION_SECONDS);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Call the method under test with a list of S3 version summaries passed as null.
        s3DaoImpl.tagVersions(s3FileTransferRequestParamsDto, s3ObjectTaggerRoleParamsDto, null, tag);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testTagVersionsOrphanS3DeleteMarker()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);

        // Create an S3 object tagger role parameters DTO.
        S3ObjectTaggerRoleParamsDto s3ObjectTaggerRoleParamsDto =
            new S3ObjectTaggerRoleParamsDto(S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, S3_OBJECT_TAGGER_ROLE_SESSION_DURATION_SECONDS);

        // Create an S3 version summary for an S3 delete marker.
        S3VersionSummary s3VersionSummary = new S3VersionSummary();
        s3VersionSummary.setKey(S3_KEY);
        s3VersionSummary.setVersionId(S3_VERSION_ID);
        s3VersionSummary.setIsDeleteMarker(true);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Call the method under test.
        s3DaoImpl.tagVersions(s3FileTransferRequestParamsDto, s3ObjectTaggerRoleParamsDto, Collections.singletonList(s3VersionSummary), tag);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testTagVersionsWithLoggerLevelSetToInfo()
    {
        String loggerName = S3DaoImpl.class.getName();
        LogLevel origLoggerLevel = getLogLevel(loggerName);
        setLogLevel(loggerName, LogLevel.INFO);

        try
        {
            runTagVersionsTest();
        }
        finally
        {
            setLogLevel(loggerName, origLoggerLevel);
        }
    }

    @Test
    public void testRestoreObjectsBulkArchiveRetrievalOption()
    {
        runRestoreObjects(Tier.Bulk.toString(), null);
    }

    @Test
    public void testRestoreObjectsStandardArchiveRetrievalOption()
    {
        runRestoreObjects(Tier.Standard.toString(), null);
    }

    @Test
    public void testRestoreObjectsExpeditedArchiveRetrievalOption()
    {
        runRestoreObjects(Tier.Expedited.toString(), null);
    }

    @Test
    public void testRestoreObjectsNullArchiveRetrievalOption()
    {
        runRestoreObjects(null, null);
    }

    @Test
    public void testRestoreObjectsInDeepArchiveNullArchiveRetrievalOption()
    {
        runRestoreObjects(null, StorageClass.DeepArchive);
    }

    @Test
    public void testRestoreObjectsInDeepArchiveBulkArchiveRetrievalOption()
    {
        runRestoreObjects(Tier.Bulk.toString(), StorageClass.DeepArchive);
    }


    @Test
    public void testRestoreObjectsWithS3ExceptionRestoreAlreadyInProgress()
    {
        testRestoreObjectsWithS3Exception(RESTORE_ALREADY_IN_PROGRESS_EXCEPTION_MESSAGE, HttpStatus.SC_CONFLICT);
    }

    @Test
    public void testRestoreObjectsWithS3ExceptionOther()
    {
        testRestoreObjectsWithS3Exception(OTHER_EXCEPTION_MESSAGE, HttpStatus.SC_METHOD_FAILURE);
    }

    @Test
    public void testCreateBatchRestoreJobComplete()
    {
        // Setup test objects
        final S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withFiles(Collections.singletonList(new File(TARGET_S3_KEY)))
                .withAwsRegionName(AWS_REGION_NAME).build();

        final BatchJobManifestDto manifest =
            BatchJobManifestDto.builder().withBucketName(S3_BUCKET_NAME_2).withKey(TEST_S3_KEY_PREFIX_2).withContent(TEST_CSV_FILE_CONTENT).build();

        // Init captors
        ArgumentCaptor<PutObjectRequest> uploadArgumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<String> jobIdCaptor = ArgumentCaptor.forClass(String.class);

        // Create mocks
        CreateJobResult mockCreateJobResult = mock(CreateJobResult.class);
        BatchJobConfigDto jobConfig = mock(BatchJobConfigDto.class);
        CreateJobRequest createJobRequest = mock(CreateJobRequest.class);

        // Configure mocks
        when(batchHelper.createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig))).thenReturn(manifest);
        when(awsS3ClientFactory.getTransferManager(any())).thenReturn(mock(TransferManager.class));
        when(s3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(Transfer.TransferState.Completed);
            return mockedUpload;
        });
        when(batchHelper.generateCreateRestoreJobRequest(any(), any(), anyInt(), any(), any())).thenReturn(createJobRequest);
        when(awsS3ClientFactory.getAmazonS3Control(any())).thenReturn(mock(AWSS3Control.class));
        when(s3Operations.createBatchJob(any(), any())).thenReturn(mockCreateJobResult);

        // Execute target method
        s3DaoImpl.createBatchRestoreJob(params, jobConfig, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);

        // Verify interactions
        verify(batchHelper).createCSVBucketKeyManifest(jobIdCaptor.capture(), any(), any(), eq(jobConfig));
        String jobId = jobIdCaptor.getValue();

        verify(s3Operations).upload(uploadArgumentCaptor.capture(), any());

        PutObjectRequest putRequest = uploadArgumentCaptor.getValue();
        assertNotNull(putRequest);
        assertEquals(S3_BUCKET_NAME_2, putRequest.getBucketName());
        assertEquals(TEST_S3_KEY_PREFIX_2, putRequest.getKey());

        verify(s3Operations).createBatchJob(eq(createJobRequest), any(AWSS3Control.class));
        verify(awsS3ClientFactory).getAmazonS3Control(any());
        verify(batchHelper).generateCreateRestoreJobRequest(eq(manifest), eq(jobId), anyInt(), eq(ARCHIVE_RETRIEVAL_OPTION), eq(jobConfig));

        verifyNoMoreInteractions(batchHelper);
        verifyNoMoreInteractions(s3Operations);
    }

    @Test
    public void testCreateBatchRestoreUploadFailed()
    {
        // Setup test objects
        final S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withFiles(Collections.singletonList(new File(TARGET_S3_KEY)))
                .withAwsRegionName(AWS_REGION_NAME).build();

        final BatchJobManifestDto manifest =
            BatchJobManifestDto.builder().withBucketName(S3_BUCKET_NAME_2).withKey(TEST_S3_KEY_PREFIX_2).withContent(TEST_CSV_FILE_CONTENT).build();

        // Init captors
        ArgumentCaptor<PutObjectRequest> uploadArgumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);

        // Create mocks
        BatchJobConfigDto jobConfig = mock(BatchJobConfigDto.class);

        // Configure mocks
        when(batchHelper.createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig))).thenReturn(manifest);
        when(awsS3ClientFactory.getTransferManager(any())).thenReturn(mock(TransferManager.class));
        when(s3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(Transfer.TransferState.Failed);
            return mockedUpload;
        });

        try
        {
            // Execute target method
            s3DaoImpl.createBatchRestoreJob(params, jobConfig, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail("Should throw an IllegalStateException when upload failed.");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().startsWith("Failed to initiate"));
        }

        // Verify interactions
        verify(batchHelper).createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig));
        verify(s3Operations).upload(uploadArgumentCaptor.capture(), any());

        PutObjectRequest putRequest = uploadArgumentCaptor.getValue();
        assertNotNull(putRequest);
        assertEquals(S3_BUCKET_NAME_2, putRequest.getBucketName());
        assertEquals(TEST_S3_KEY_PREFIX_2, putRequest.getKey());

        verifyNoMoreInteractions(batchHelper);
        verifyNoMoreInteractions(s3Operations);
    }

    @Test
    public void testCreateBatchRestoreCreateManifestFailed()
    {
        // Setup test objects
        final S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withFiles(Collections.singletonList(new File(TARGET_S3_KEY)))
                .withAwsRegionName(AWS_REGION_NAME).build();

        // Create mocks
        BatchJobConfigDto jobConfig = mock(BatchJobConfigDto.class);

        // Configure mocks
        when(batchHelper.createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig))).thenThrow(new IllegalArgumentException());

        try
        {
            // Execute target method
            s3DaoImpl.createBatchRestoreJob(params, jobConfig, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail("Should throw an IllegalStateException.");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().startsWith("Failed to initiate"));
        }

        // Verify interactions
        verify(batchHelper).createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig));

        verifyNoMoreInteractions(batchHelper);
        verifyNoMoreInteractions(s3Operations);
    }

    @Test
    public void testCreateBatchRestoreJobGenerateRequestFailed()
    {
        // Setup test objects
        final S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withFiles(Collections.singletonList(new File(TARGET_S3_KEY)))
                .withAwsRegionName(AWS_REGION_NAME).build();

        final BatchJobManifestDto manifest =
            BatchJobManifestDto.builder().withBucketName(S3_BUCKET_NAME_2).withKey(TEST_S3_KEY_PREFIX_2).withContent(TEST_CSV_FILE_CONTENT).build();

        // Create mocks
        BatchJobConfigDto jobConfig = mock(BatchJobConfigDto.class);

        // Configure mocks
        when(batchHelper.createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig))).thenReturn(manifest);
        when(awsS3ClientFactory.getTransferManager(any())).thenReturn(mock(TransferManager.class));
        when(s3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(Transfer.TransferState.Completed);
            return mockedUpload;
        });
        when(batchHelper.generateCreateRestoreJobRequest(any(), any(), anyInt(), any(), any())).thenThrow(new IllegalArgumentException());

        try
        {
            // Execute target method
            s3DaoImpl.createBatchRestoreJob(params, jobConfig, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail("Should throw an IllegalStateException when upload failed.");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().startsWith("Failed to initiate"));
        }

        // Verify interactions
        verify(batchHelper).createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig));
        verify(s3Operations).upload(any(), any());
        verify(batchHelper).generateCreateRestoreJobRequest(eq(manifest), any(), anyInt(), eq(ARCHIVE_RETRIEVAL_OPTION), eq(jobConfig));

        verifyNoMoreInteractions(batchHelper);
        verifyNoMoreInteractions(s3Operations);
    }

    @Test
    public void testCreateBatchRestoreJobAWSCreateJobFailed()
    {
        // Setup test objects
        final S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withFiles(Collections.singletonList(new File(TARGET_S3_KEY)))
                .withAwsRegionName(AWS_REGION_NAME).build();

        final BatchJobManifestDto manifest =
            BatchJobManifestDto.builder().withBucketName(S3_BUCKET_NAME_2).withKey(TEST_S3_KEY_PREFIX_2).withContent(TEST_CSV_FILE_CONTENT).build();

        // Init captors
        ArgumentCaptor<String> jobIdCaptor = ArgumentCaptor.forClass(String.class);

        // Create mocks
        BatchJobConfigDto jobConfig = mock(BatchJobConfigDto.class);
        CreateJobRequest createJobRequest = mock(CreateJobRequest.class);

        // Configure mocks
        when(batchHelper.createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig))).thenReturn(manifest);
        when(awsS3ClientFactory.getTransferManager(any())).thenReturn(mock(TransferManager.class));
        when(s3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(Transfer.TransferState.Completed);
            return mockedUpload;
        });
        when(batchHelper.generateCreateRestoreJobRequest(any(), any(), anyInt(), any(), any())).thenReturn(createJobRequest);
        when(awsS3ClientFactory.getAmazonS3Control(any())).thenReturn(mock(AWSS3Control.class));
        when(s3Operations.createBatchJob(any(), any())).thenThrow(new InternalServiceException("Internal Service Exception"));

        try
        {
            // Execute target method
            s3DaoImpl.createBatchRestoreJob(params, jobConfig, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail("Should throw an IllegalStateException.");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().startsWith("Failed to initiate"));
            assertTrue(e.getCause().getMessage().contains("Internal Service Exception"));
        }

        // Verify interactions
        verify(batchHelper).createCSVBucketKeyManifest(jobIdCaptor.capture(), any(), any(), eq(jobConfig));
        String jobId = jobIdCaptor.getValue();

        verify(s3Operations).upload(any(), any());
        verify(s3Operations).createBatchJob(eq(createJobRequest), any(AWSS3Control.class));
        verify(awsS3ClientFactory).getAmazonS3Control(any());
        verify(batchHelper).generateCreateRestoreJobRequest(eq(manifest), eq(jobId), anyInt(), eq(ARCHIVE_RETRIEVAL_OPTION), eq(jobConfig));

        verifyNoMoreInteractions(batchHelper);
        verifyNoMoreInteractions(s3Operations);
    }

    @Test
    public void testBatchRestoreObjectsSuccess()
    {
        // Setup test objects
        final S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withFiles(Collections.singletonList(new File(TARGET_S3_KEY)))
                .withAwsRegionName(AWS_REGION_NAME).build();

        final BatchJobManifestDto manifest =
            BatchJobManifestDto.builder().withBucketName(S3_BUCKET_NAME_2).withKey(TEST_S3_KEY_PREFIX_2).withContent(TEST_CSV_FILE_CONTENT).build();

        final BatchJobConfigDto jobConfig = BatchJobConfigDto.builder().withMaxAttempts(5).withBackoffPeriod(2000).build();

        // Create mocks
        AWSS3Control s3Control = mock(AWSS3Control.class);
        CreateJobResult mockCreateJobResult = mock(CreateJobResult.class);
        CreateJobRequest createJobRequest = mock(CreateJobRequest.class);
        DescribeJobRequest describeRequest = mock(DescribeJobRequest.class);
        DescribeJobResult describeResult = mock(DescribeJobResult.class);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);

        // Configure mocks
        when(describeResult.getJob()).thenReturn(jobDescriptor);
        when(jobDescriptor.getStatus()).thenReturn(JobStatus.Failed.toString());

        when(batchHelper.createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig))).thenReturn(manifest);
        when(batchHelper.generateCreateRestoreJobRequest(any(), any(), anyInt(), any(), any())).thenReturn(createJobRequest);
        when(batchHelper.generateDescribeJobRequest(any(), any())).thenReturn(describeRequest);

        when(awsS3ClientFactory.getTransferManager(any())).thenReturn(mock(TransferManager.class));
        when(awsS3ClientFactory.getAmazonS3Control(any())).thenReturn(s3Control);

        when(s3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(Transfer.TransferState.Completed);
            return mockedUpload;
        });
        when(s3Operations.createBatchJob(any(), any())).thenReturn(mockCreateJobResult);
        when(s3Operations.describeBatchJob(any(), any())).thenReturn(describeResult);

        // Execute target method
        try
        {
            s3DaoImpl.batchRestoreObjects(params, jobConfig, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains(describeResult.toString()));
        }

        // Verifications
        verify(batchHelper).createCSVBucketKeyManifest(any(), eq(S3_BUCKET_NAME), eq(params.getFiles()), eq(jobConfig));
        verify(s3Operations).upload(any(), any());
        verify(awsS3ClientFactory).getTransferManager(eq(params));
        verify(batchHelper)
            .generateCreateRestoreJobRequest(eq(manifest), any(), eq(S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS), eq(ARCHIVE_RETRIEVAL_OPTION), eq(jobConfig));
        verify(s3Operations).createBatchJob(eq(createJobRequest), eq(s3Control));
        verify(batchHelper).generateDescribeJobRequest(any(), eq(jobConfig));
        verify(awsS3ClientFactory, times(2)).getAmazonS3Control(eq(params));
        verify(s3Operations).describeBatchJob(eq(describeRequest), eq(s3Control));
        verify(s3Control, times(2)).shutdown();

        verifyNoMoreInteractions(batchHelper, s3Operations, awsS3ClientFactory, s3Control);
    }

    @Test
    public void testBatchRestoreObjectsAwaitSuccess()
    {
        // Setup test objects
        final S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withFiles(Collections.singletonList(new File(TARGET_S3_KEY)))
                .withAwsRegionName(AWS_REGION_NAME).build();

        final BatchJobManifestDto manifest =
            BatchJobManifestDto.builder().withBucketName(S3_BUCKET_NAME_2).withKey(TEST_S3_KEY_PREFIX_2).withContent(TEST_CSV_FILE_CONTENT).build();

        final BatchJobConfigDto jobConfig = BatchJobConfigDto.builder().withMaxAttempts(5).withBackoffPeriod(2000).build();

        // Create mocks
        AWSS3Control s3Control = mock(AWSS3Control.class);
        CreateJobResult mockCreateJobResult = mock(CreateJobResult.class);
        CreateJobRequest createJobRequest = mock(CreateJobRequest.class);
        DescribeJobRequest describeRequest = mock(DescribeJobRequest.class);
        DescribeJobResult describeResult = mock(DescribeJobResult.class);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);

        // Configure mocks
        when(describeResult.getJob()).thenReturn(jobDescriptor);
        when(jobDescriptor.getStatus()).thenReturn(JobStatus.Active.toString(), JobStatus.Complete.toString());

        when(batchHelper.createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig))).thenReturn(manifest);
        when(batchHelper.generateCreateRestoreJobRequest(any(), any(), anyInt(), any(), any())).thenReturn(createJobRequest);
        when(batchHelper.generateDescribeJobRequest(any(), any())).thenReturn(describeRequest);

        when(awsS3ClientFactory.getTransferManager(any())).thenReturn(mock(TransferManager.class));
        when(awsS3ClientFactory.getAmazonS3Control(any())).thenReturn(s3Control);

        when(s3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(Transfer.TransferState.Completed);
            return mockedUpload;
        });
        when(s3Operations.createBatchJob(any(), any())).thenReturn(mockCreateJobResult);
        when(s3Operations.describeBatchJob(any(), any())).thenReturn(describeResult);

        // Execute target method
        s3DaoImpl.batchRestoreObjects(params, jobConfig, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);

        // Verifications
        verify(batchHelper).createCSVBucketKeyManifest(any(), eq(S3_BUCKET_NAME), eq(params.getFiles()), eq(jobConfig));
        verify(s3Operations).upload(any(), any());
        verify(awsS3ClientFactory).getTransferManager(eq(params));
        verify(batchHelper)
            .generateCreateRestoreJobRequest(eq(manifest), any(), eq(S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS), eq(ARCHIVE_RETRIEVAL_OPTION), eq(jobConfig));
        verify(s3Operations).createBatchJob(eq(createJobRequest), eq(s3Control));
        verify(batchHelper, times(2)).generateDescribeJobRequest(any(), eq(jobConfig));
        verify(awsS3ClientFactory, times(3)).getAmazonS3Control(eq(params));
        verify(s3Operations, times(2)).describeBatchJob(eq(describeRequest), eq(s3Control));
        verify(s3Control, times(3)).shutdown();

        verifyNoMoreInteractions(batchHelper, s3Operations, awsS3ClientFactory, s3Control);
    }

    @Test
    public void testBatchRestoreObjectsFailed()
    {
        // Setup test objects
        final S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withFiles(Collections.singletonList(new File(TARGET_S3_KEY)))
                .withAwsRegionName(AWS_REGION_NAME).build();

        final BatchJobManifestDto manifest =
            BatchJobManifestDto.builder().withBucketName(S3_BUCKET_NAME_2).withKey(TEST_S3_KEY_PREFIX_2).withContent(TEST_CSV_FILE_CONTENT).build();

        final BatchJobConfigDto jobConfig = BatchJobConfigDto.builder().withMaxAttempts(5).withBackoffPeriod(2000).build();

        // Create mocks
        AWSS3Control s3Control = mock(AWSS3Control.class);
        CreateJobResult mockCreateJobResult = mock(CreateJobResult.class);
        CreateJobRequest createJobRequest = mock(CreateJobRequest.class);
        DescribeJobRequest describeRequest = mock(DescribeJobRequest.class);
        DescribeJobResult describeResult = mock(DescribeJobResult.class);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);

        // Configure mocks
        when(describeResult.getJob()).thenReturn(jobDescriptor);
        when(jobDescriptor.getStatus()).thenReturn(JobStatus.Failed.toString());

        when(batchHelper.createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig))).thenReturn(manifest);
        when(batchHelper.generateCreateRestoreJobRequest(any(), any(), anyInt(), any(), any())).thenReturn(createJobRequest);
        when(batchHelper.generateDescribeJobRequest(any(), any())).thenReturn(describeRequest);

        when(awsS3ClientFactory.getTransferManager(any())).thenReturn(mock(TransferManager.class));
        when(awsS3ClientFactory.getAmazonS3Control(any())).thenReturn(s3Control);

        when(s3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(Transfer.TransferState.Completed);
            return mockedUpload;
        });
        when(s3Operations.createBatchJob(any(), any())).thenReturn(mockCreateJobResult);
        when(s3Operations.describeBatchJob(any(), any())).thenReturn(describeResult);

        // Execute target method
        try
        {
            s3DaoImpl.batchRestoreObjects(params, jobConfig, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().startsWith("S3 batch job was not complete."));
            assertTrue(e.getMessage().contains(describeResult.toString()));
        }

        // Verifications
        verify(batchHelper).createCSVBucketKeyManifest(any(), eq(S3_BUCKET_NAME), eq(params.getFiles()), eq(jobConfig));
        verify(s3Operations).upload(any(), any());
        verify(awsS3ClientFactory).getTransferManager(eq(params));
        verify(batchHelper)
            .generateCreateRestoreJobRequest(eq(manifest), any(), eq(S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS), eq(ARCHIVE_RETRIEVAL_OPTION), eq(jobConfig));
        verify(s3Operations).createBatchJob(eq(createJobRequest), eq(s3Control));
        verify(batchHelper).generateDescribeJobRequest(any(), eq(jobConfig));
        verify(awsS3ClientFactory, times(2)).getAmazonS3Control(eq(params));
        verify(s3Operations).describeBatchJob(eq(describeRequest), eq(s3Control));
        verify(s3Control, times(2)).shutdown();

        verifyNoMoreInteractions(batchHelper, s3Operations, awsS3ClientFactory, s3Control);
    }

    @Test
    public void testBatchRestoreObjectsDescribeBatchJobFailed()
    {
        // Setup test objects
        final S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withFiles(Collections.singletonList(new File(TARGET_S3_KEY)))
                .withAwsRegionName(AWS_REGION_NAME).build();

        final BatchJobManifestDto manifest =
            BatchJobManifestDto.builder().withBucketName(S3_BUCKET_NAME_2).withKey(TEST_S3_KEY_PREFIX_2).withContent(TEST_CSV_FILE_CONTENT).build();

        final BatchJobConfigDto jobConfig = BatchJobConfigDto.builder().withMaxAttempts(5).withBackoffPeriod(2000).build();

        // Create mocks
        AWSS3Control s3Control = mock(AWSS3Control.class);
        CreateJobResult mockCreateJobResult = mock(CreateJobResult.class);
        CreateJobRequest createJobRequest = mock(CreateJobRequest.class);
        DescribeJobRequest describeRequest = mock(DescribeJobRequest.class);
        DescribeJobResult describeResult = mock(DescribeJobResult.class);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);

        // Configure mocks
        when(describeResult.getJob()).thenReturn(jobDescriptor);
        when(jobDescriptor.getStatus()).thenReturn(JobStatus.Failed.toString());

        when(batchHelper.createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig))).thenReturn(manifest);
        when(batchHelper.generateCreateRestoreJobRequest(any(), any(), anyInt(), any(), any())).thenReturn(createJobRequest);
        when(batchHelper.generateDescribeJobRequest(any(), any())).thenReturn(describeRequest);

        when(awsS3ClientFactory.getTransferManager(any())).thenReturn(mock(TransferManager.class));
        when(awsS3ClientFactory.getAmazonS3Control(any())).thenReturn(s3Control);

        when(s3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(Transfer.TransferState.Completed);
            return mockedUpload;
        });
        when(s3Operations.createBatchJob(any(), any())).thenReturn(mockCreateJobResult);
        when(s3Operations.describeBatchJob(any(), any())).thenThrow(new BadRequestException(ERROR_MESSAGE));

        // Execute target method
        try
        {
            s3DaoImpl.batchRestoreObjects(params, jobConfig, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains(ERROR_MESSAGE));
        }

        // Verifications
        verify(batchHelper).createCSVBucketKeyManifest(any(), eq(S3_BUCKET_NAME), eq(params.getFiles()), eq(jobConfig));
        verify(s3Operations).upload(any(), any());
        verify(awsS3ClientFactory).getTransferManager(eq(params));
        verify(batchHelper)
            .generateCreateRestoreJobRequest(eq(manifest), any(), eq(S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS), eq(ARCHIVE_RETRIEVAL_OPTION), eq(jobConfig));
        verify(s3Operations).createBatchJob(eq(createJobRequest), eq(s3Control));
        verify(batchHelper).generateDescribeJobRequest(any(), eq(jobConfig));
        verify(awsS3ClientFactory, times(2)).getAmazonS3Control(eq(params));
        verify(s3Operations).describeBatchJob(eq(describeRequest), eq(s3Control));
        verify(s3Control, times(2)).shutdown();

        verifyNoMoreInteractions(batchHelper, s3Operations, awsS3ClientFactory, s3Control);
    }


    @Test
    public void testBatchRestoreObjectsAwaitFailed()
    {
        // Setup test objects
        final S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withFiles(Collections.singletonList(new File(TARGET_S3_KEY)))
                .withAwsRegionName(AWS_REGION_NAME).build();

        final BatchJobManifestDto manifest =
            BatchJobManifestDto.builder().withBucketName(S3_BUCKET_NAME_2).withKey(TEST_S3_KEY_PREFIX_2).withContent(TEST_CSV_FILE_CONTENT).build();

        final BatchJobConfigDto jobConfig = BatchJobConfigDto.builder().withMaxAttempts(5).withBackoffPeriod(2000).build();

        // Create mocks
        AWSS3Control s3Control = mock(AWSS3Control.class);
        CreateJobResult mockCreateJobResult = mock(CreateJobResult.class);
        CreateJobRequest createJobRequest = mock(CreateJobRequest.class);
        DescribeJobRequest describeRequest = mock(DescribeJobRequest.class);
        DescribeJobResult describeResult = mock(DescribeJobResult.class);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);

        // Configure mocks
        when(describeResult.getJob()).thenReturn(jobDescriptor);
        when(jobDescriptor.getStatus()).thenReturn(JobStatus.Active.toString(), JobStatus.Failed.toString());

        when(batchHelper.createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig))).thenReturn(manifest);
        when(batchHelper.generateCreateRestoreJobRequest(any(), any(), anyInt(), any(), any())).thenReturn(createJobRequest);
        when(batchHelper.generateDescribeJobRequest(any(), any())).thenReturn(describeRequest);

        when(awsS3ClientFactory.getTransferManager(any())).thenReturn(mock(TransferManager.class));
        when(awsS3ClientFactory.getAmazonS3Control(any())).thenReturn(s3Control);

        when(s3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(Transfer.TransferState.Completed);
            return mockedUpload;
        });
        when(s3Operations.createBatchJob(any(), any())).thenReturn(mockCreateJobResult);
        when(s3Operations.describeBatchJob(any(), any())).thenReturn(describeResult);

        // Execute target method
        try
        {
            s3DaoImpl.batchRestoreObjects(params, jobConfig, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().startsWith("S3 batch job was not complete."));
            assertTrue(e.getMessage().contains(describeResult.toString()));
        }
        // Verifications
        verify(batchHelper).createCSVBucketKeyManifest(any(), eq(S3_BUCKET_NAME), eq(params.getFiles()), eq(jobConfig));
        verify(s3Operations).upload(any(), any());
        verify(awsS3ClientFactory).getTransferManager(eq(params));
        verify(batchHelper)
            .generateCreateRestoreJobRequest(eq(manifest), any(), eq(S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS), eq(ARCHIVE_RETRIEVAL_OPTION), eq(jobConfig));
        verify(s3Operations).createBatchJob(eq(createJobRequest), eq(s3Control));
        verify(batchHelper, times(2)).generateDescribeJobRequest(any(), eq(jobConfig));
        verify(awsS3ClientFactory, times(3)).getAmazonS3Control(eq(params));
        verify(s3Operations, times(2)).describeBatchJob(eq(describeRequest), eq(s3Control));
        verify(s3Control, times(3)).shutdown();

        verifyNoMoreInteractions(batchHelper, s3Operations, awsS3ClientFactory, s3Control);
    }

    @Test
    public void testBatchRestoreObjectsAwaitCancelled()
    {
        // Setup test objects
        final S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withFiles(Collections.singletonList(new File(TARGET_S3_KEY)))
                .withAwsRegionName(AWS_REGION_NAME).build();

        final BatchJobManifestDto manifest =
            BatchJobManifestDto.builder().withBucketName(S3_BUCKET_NAME_2).withKey(TEST_S3_KEY_PREFIX_2).withContent(TEST_CSV_FILE_CONTENT).build();

        final BatchJobConfigDto jobConfig = BatchJobConfigDto.builder().withMaxAttempts(5).withBackoffPeriod(2000).build();

        // Create mocks
        AWSS3Control s3Control = mock(AWSS3Control.class);
        CreateJobResult mockCreateJobResult = mock(CreateJobResult.class);
        CreateJobRequest createJobRequest = mock(CreateJobRequest.class);
        DescribeJobRequest describeRequest = mock(DescribeJobRequest.class);
        DescribeJobResult describeResult = mock(DescribeJobResult.class);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);

        // Configure mocks
        when(describeResult.getJob()).thenReturn(jobDescriptor);
        when(jobDescriptor.getStatus()).thenReturn(JobStatus.Active.toString(), JobStatus.Cancelled.toString());

        when(batchHelper.createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig))).thenReturn(manifest);
        when(batchHelper.generateCreateRestoreJobRequest(any(), any(), anyInt(), any(), any())).thenReturn(createJobRequest);
        when(batchHelper.generateDescribeJobRequest(any(), any())).thenReturn(describeRequest);

        when(awsS3ClientFactory.getTransferManager(any())).thenReturn(mock(TransferManager.class));
        when(awsS3ClientFactory.getAmazonS3Control(any())).thenReturn(s3Control);

        when(s3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(Transfer.TransferState.Completed);
            return mockedUpload;
        });
        when(s3Operations.createBatchJob(any(), any())).thenReturn(mockCreateJobResult);
        when(s3Operations.describeBatchJob(any(), any())).thenReturn(describeResult);

        // Execute target method
        try
        {
            s3DaoImpl.batchRestoreObjects(params, jobConfig, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().startsWith("S3 batch job was not complete."));
            assertTrue(e.getMessage().contains(describeResult.toString()));
        }
        // Verifications
        verify(batchHelper).createCSVBucketKeyManifest(any(), eq(S3_BUCKET_NAME), eq(params.getFiles()), eq(jobConfig));
        verify(s3Operations).upload(any(), any());
        verify(awsS3ClientFactory).getTransferManager(eq(params));
        verify(batchHelper)
            .generateCreateRestoreJobRequest(eq(manifest), any(), eq(S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS), eq(ARCHIVE_RETRIEVAL_OPTION), eq(jobConfig));
        verify(s3Operations).createBatchJob(eq(createJobRequest), eq(s3Control));
        verify(batchHelper, times(2)).generateDescribeJobRequest(any(), eq(jobConfig));
        verify(awsS3ClientFactory, times(3)).getAmazonS3Control(eq(params));
        verify(s3Operations, times(2)).describeBatchJob(eq(describeRequest), eq(s3Control));
        verify(s3Control, times(3)).shutdown();

        verifyNoMoreInteractions(batchHelper, s3Operations, awsS3ClientFactory, s3Control);
    }

    @Test
    public void batchRestoreObjectsAwaitExhaustedTest()
    {
        // Setup test objects
        final S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withFiles(Collections.singletonList(new File(TARGET_S3_KEY)))
                .withAwsRegionName(AWS_REGION_NAME).build();

        final BatchJobManifestDto manifest =
            BatchJobManifestDto.builder().withBucketName(S3_BUCKET_NAME_2).withKey(TEST_S3_KEY_PREFIX_2).withContent(TEST_CSV_FILE_CONTENT).build();

        final BatchJobConfigDto jobConfig = BatchJobConfigDto.builder().withMaxAttempts(5).withBackoffPeriod(2000).build();

        // Create mocks
        AWSS3Control s3Control = mock(AWSS3Control.class);
        CreateJobResult mockCreateJobResult = mock(CreateJobResult.class);
        CreateJobRequest createJobRequest = mock(CreateJobRequest.class);
        DescribeJobRequest describeRequest = mock(DescribeJobRequest.class);
        DescribeJobResult describeResult = mock(DescribeJobResult.class);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);

        // Configure mocks
        when(describeResult.getJob()).thenReturn(jobDescriptor);
        when(jobDescriptor.getStatus()).thenReturn(JobStatus.Active.toString());

        when(batchHelper.createCSVBucketKeyManifest(any(), any(), any(), eq(jobConfig))).thenReturn(manifest);
        when(batchHelper.generateCreateRestoreJobRequest(any(), any(), anyInt(), any(), any())).thenReturn(createJobRequest);
        when(batchHelper.generateDescribeJobRequest(any(), any())).thenReturn(describeRequest);

        when(awsS3ClientFactory.getTransferManager(any())).thenReturn(mock(TransferManager.class));
        when(awsS3ClientFactory.getAmazonS3Control(any())).thenReturn(s3Control);

        when(s3Operations.upload(any(), any())).then((Answer<Upload>) invocation -> {
            Upload mockedUpload = mock(Upload.class);
            TransferProgress transferProgress = new TransferProgress();
            when(mockedUpload.getProgress()).thenReturn(transferProgress);
            when(mockedUpload.isDone()).thenReturn(true);
            when(mockedUpload.getState()).thenReturn(Transfer.TransferState.Completed);
            return mockedUpload;
        });
        when(s3Operations.createBatchJob(any(), any())).thenReturn(mockCreateJobResult);
        when(s3Operations.describeBatchJob(any(), any())).thenReturn(describeResult);

        // Execute target method
        try
        {
            s3DaoImpl.batchRestoreObjects(params, jobConfig, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().startsWith("S3 batch job was not complete. Detailed descriptor:"));
            assertTrue(e.getMessage().contains(describeResult.toString()));
        }
        // Verifications
        verify(batchHelper).createCSVBucketKeyManifest(any(), eq(S3_BUCKET_NAME), eq(params.getFiles()), eq(jobConfig));
        verify(s3Operations).upload(any(), any());
        verify(awsS3ClientFactory).getTransferManager(eq(params));
        verify(batchHelper)
            .generateCreateRestoreJobRequest(eq(manifest), any(), eq(S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS), eq(ARCHIVE_RETRIEVAL_OPTION), eq(jobConfig));
        verify(s3Operations).createBatchJob(eq(createJobRequest), eq(s3Control));
        verify(batchHelper, times(5)).generateDescribeJobRequest(any(), eq(jobConfig));
        verify(awsS3ClientFactory, times(6)).getAmazonS3Control(eq(params));
        verify(s3Operations, times(5)).describeBatchJob(eq(describeRequest), eq(s3Control));
        verify(s3Control, times(6)).shutdown();

        verifyNoMoreInteractions(batchHelper, s3Operations, awsS3ClientFactory, s3Control);
    }

    private void testRestoreObjectsWithS3Exception(String exceptionMessage, int statusCode)
    {
        List<File> files = Collections.singletonList(new File(TEST_FILE));

        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(S3_KEY_PREFIX);
        s3FileTransferRequestParamsDto.setFiles(files);

        // Create an Object Metadata
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setOngoingRestore(false);
        objectMetadata.setHeader(Headers.STORAGE_CLASS, StorageClass.DeepArchive);

        ArgumentCaptor<AmazonS3Client> s3ClientCaptor = ArgumentCaptor.forClass(AmazonS3Client.class);
        ArgumentCaptor<String> s3BucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RestoreObjectRequest> requestStoreCaptor = ArgumentCaptor.forClass(RestoreObjectRequest.class);

        // Create an Amazon S3 Exception
        AmazonS3Exception amazonS3Exception = new AmazonS3Exception(exceptionMessage);
        amazonS3Exception.setStatusCode(statusCode);

        // Mock the external calls.
        when(awsS3ClientFactory.getAmazonS3Client(any(S3FileTransferRequestParamsDto.class))).thenReturn(s3Client);
        when(s3Operations.getObjectMetadata(s3BucketNameCaptor.capture(), keyCaptor.capture(), s3ClientCaptor.capture())).thenReturn(objectMetadata);
        doThrow(amazonS3Exception).when(s3Operations).restoreObject(requestStoreCaptor.capture(), s3ClientCaptor.capture());

        try
        {
            // Call the method under test.
            s3DaoImpl.restoreObjects(s3FileTransferRequestParamsDto, EXPIRATION_IN_DAYS, Tier.Standard.toString());

            // If this is not a restore already in progress exception message (409) then we should have caught an exception.
            // Else if this is a restore already in progress message (409) then continue as usual.
            if (!exceptionMessage.equals(RESTORE_ALREADY_IN_PROGRESS_EXCEPTION_MESSAGE))
            {
                // Should not be here. Fail!
                fail();
            }
            else
            {
                RestoreObjectRequest requestStore = requestStoreCaptor.getValue();
                assertEquals(S3_BUCKET_NAME, s3BucketNameCaptor.getValue());
                assertEquals(TEST_FILE, keyCaptor.getValue());

                // Verify Bulk option is used when the option is not provided
                assertEquals(StringUtils.isNotEmpty(Tier.Standard.toString()) ? Tier.Standard.toString() : Tier.Bulk.toString(),
                    requestStore.getGlacierJobParameters().getTier());
            }
        }
        catch (IllegalStateException illegalStateException)
        {
            assertEquals(String.format(
                "Failed to initiate a restore request for \"%s\" key in \"%s\" bucket. " + "Reason: com.amazonaws.services.s3.model.AmazonS3Exception: %s " +
                    "(Service: null; Status Code: %s; Error Code: null; Request ID: null; S3 Extended Request ID: null; Proxy: null), S3 Extended Request ID: null",
                TEST_FILE, S3_BUCKET_NAME, exceptionMessage, statusCode), illegalStateException.getMessage());
        }

        // Verify the external calls
        verify(awsS3ClientFactory).getAmazonS3Client(any(S3FileTransferRequestParamsDto.class));
        verify(s3Operations).getObjectMetadata(anyString(), anyString(), any(AmazonS3Client.class));
        verify(s3Operations).restoreObject(any(RestoreObjectRequest.class), any(AmazonS3Client.class));
        verify(s3Client).shutdown();
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Run restore objects method
     *
     * @param archiveRetrievalOption the archive retrieval option
     */
    private void runRestoreObjects(String archiveRetrievalOption, StorageClass storageClass)
    {
        List<File> files = Collections.singletonList(new File(TEST_FILE));

        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(S3_KEY_PREFIX);
        s3FileTransferRequestParamsDto.setFiles(files);

        // Create an Object Metadata
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setOngoingRestore(false);
        objectMetadata.setHeader(Headers.STORAGE_CLASS, storageClass);

        ArgumentCaptor<AmazonS3Client> s3ClientCaptor = ArgumentCaptor.forClass(AmazonS3Client.class);
        ArgumentCaptor<String> s3BucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RestoreObjectRequest> requestStoreCaptor = ArgumentCaptor.forClass(RestoreObjectRequest.class);

        // Mock the external calls.
        when(awsS3ClientFactory.getAmazonS3Client(any(S3FileTransferRequestParamsDto.class))).thenReturn(s3Client);
        when(s3Operations.getObjectMetadata(s3BucketNameCaptor.capture(), keyCaptor.capture(), s3ClientCaptor.capture())).thenReturn(objectMetadata);
        doNothing().when(s3Operations).restoreObject(requestStoreCaptor.capture(), s3ClientCaptor.capture());

        s3DaoImpl.restoreObjects(s3FileTransferRequestParamsDto, EXPIRATION_IN_DAYS, archiveRetrievalOption);

        RestoreObjectRequest requestStore = requestStoreCaptor.getValue();
        assertEquals(S3_BUCKET_NAME, s3BucketNameCaptor.getValue());
        assertEquals(TEST_FILE, keyCaptor.getValue());
        assertEquals(s3Client, s3ClientCaptor.getValue());

        // Verify Bulk option is used when the option is not provided
        assertEquals(StringUtils.isNotEmpty(archiveRetrievalOption) ? archiveRetrievalOption : Tier.Bulk.toString(),
            requestStore.getGlacierJobParameters().getTier());

        // Verify the external calls
        verify(awsS3ClientFactory).getAmazonS3Client(any(S3FileTransferRequestParamsDto.class));
        verify(s3Operations).getObjectMetadata(anyString(), anyString(), any(AmazonS3Client.class));
        verify(s3Operations).restoreObject(any(RestoreObjectRequest.class), any(AmazonS3Client.class));
        verify(s3Client).shutdown();
        verifyNoMoreInteractionsHelper();
    }

    private void runTagObjectsTest()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        s3FileTransferRequestParamsDto.setAwsRegionName(AWS_REGION_NAME_US_EAST_1);

        // Create an S3 object tagger role parameters DTO.
        S3ObjectTaggerRoleParamsDto s3ObjectTaggerRoleParamsDto =
            new S3ObjectTaggerRoleParamsDto(S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, S3_OBJECT_TAGGER_ROLE_SESSION_DURATION_SECONDS);

        // Create an S3 object summary.
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(S3_KEY);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Create a client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Create a get object tagging result.
        GetObjectTaggingResult getObjectTaggingResult = new GetObjectTaggingResult(null);

        // Create a set object tagging result.
        SetObjectTaggingResult setObjectTaggingResult = new SetObjectTaggingResult();

        // Create mock of the s3 client used specifically for tagging
        AmazonS3Client taggerS3client = mock(AmazonS3Client.class);

        // Mock the external calls.
        when(awsS3ClientFactory.getAmazonS3Client(any(S3FileTransferRequestParamsDto.class))).thenReturn(s3Client);
        when(awsS3ClientFactory.getAmazonS3Client(any(S3FileTransferRequestParamsDto.class), any(AWSCredentialsProvider.class))).thenReturn(taggerS3client);
        when(awsHelper.getClientConfiguration(s3FileTransferRequestParamsDto)).thenReturn(clientConfiguration);
        when(s3Operations.getObjectTagging(any(GetObjectTaggingRequest.class), any(AmazonS3Client.class))).thenReturn(getObjectTaggingResult);
        when(s3Operations.setObjectTagging(any(SetObjectTaggingRequest.class), any(AmazonS3Client.class))).thenReturn(setObjectTaggingResult);

        // Call the method under test.
        s3DaoImpl.tagObjects(s3FileTransferRequestParamsDto, s3ObjectTaggerRoleParamsDto, Collections.singletonList(s3ObjectSummary), tag);

        // Verify the external calls.
        verify(awsS3ClientFactory).getAmazonS3Client(any(S3FileTransferRequestParamsDto.class));
        verify(awsS3ClientFactory).getAmazonS3Client(any(S3FileTransferRequestParamsDto.class), any(AWSCredentialsProvider.class));
        verify(awsHelper).getClientConfiguration(s3FileTransferRequestParamsDto);
        verify(s3Operations).getObjectTagging(any(GetObjectTaggingRequest.class), any(AmazonS3Client.class));
        verify(s3Operations).setObjectTagging(any(SetObjectTaggingRequest.class), any(AmazonS3Client.class));
        verify(s3Client).shutdown();
        verifyNoMoreInteractionsHelper();
    }

    private void runTagVersionsTest()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        s3FileTransferRequestParamsDto.setAwsRegionName(AWS_REGION_NAME_US_EAST_1);

        // Create an S3 object tagger role parameters DTO.
        S3ObjectTaggerRoleParamsDto s3ObjectTaggerRoleParamsDto =
            new S3ObjectTaggerRoleParamsDto(S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, S3_OBJECT_TAGGER_ROLE_SESSION_DURATION_SECONDS);

        // Create an S3 version summary.
        S3VersionSummary s3VersionSummary = new S3VersionSummary();
        s3VersionSummary.setKey(S3_KEY);
        s3VersionSummary.setVersionId(S3_VERSION_ID);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Create mock of the s3 client used specifically for tagging
        AmazonS3Client taggerS3client = mock(AmazonS3Client.class);

        // Create a client configuration.
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Create a get object tagging result.
        GetObjectTaggingResult getObjectTaggingResult = new GetObjectTaggingResult(null);

        // Create a set object tagging result.
        SetObjectTaggingResult setObjectTaggingResult = new SetObjectTaggingResult();

        // Mock the external calls.
        when(awsS3ClientFactory.getAmazonS3Client(any(S3FileTransferRequestParamsDto.class))).thenReturn(s3Client);
        when(awsS3ClientFactory.getAmazonS3Client(any(S3FileTransferRequestParamsDto.class), any(AWSCredentialsProvider.class))).thenReturn(taggerS3client);
        when(awsHelper.getClientConfiguration(s3FileTransferRequestParamsDto)).thenReturn(clientConfiguration);
        when(s3Operations.getObjectTagging(any(GetObjectTaggingRequest.class), any(AmazonS3Client.class))).thenReturn(getObjectTaggingResult);
        when(s3Operations.setObjectTagging(any(SetObjectTaggingRequest.class), any(AmazonS3Client.class))).thenReturn(setObjectTaggingResult);

        // Call the method under test.
        s3DaoImpl.tagVersions(s3FileTransferRequestParamsDto, s3ObjectTaggerRoleParamsDto, Collections.singletonList(s3VersionSummary), tag);

        // Verify the external calls.
        verify(awsS3ClientFactory).getAmazonS3Client(any(S3FileTransferRequestParamsDto.class));
        verify(awsS3ClientFactory).getAmazonS3Client(any(S3FileTransferRequestParamsDto.class), any(AWSCredentialsProvider.class));
        verify(awsHelper).getClientConfiguration(s3FileTransferRequestParamsDto);
        verify(s3Operations).getObjectTagging(any(GetObjectTaggingRequest.class), any(AmazonS3Client.class));
        verify(s3Operations).setObjectTagging(any(SetObjectTaggingRequest.class), any(AmazonS3Client.class));
        verify(s3Client).shutdown();
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(awsS3ClientFactory, awsHelper, batchHelper, javaPropertiesHelper, jsonHelper, s3Operations, s3Client);
    }
}
