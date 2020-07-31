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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
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
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingResult;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.Tier;
import com.amazonaws.services.s3.model.VersionListing;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.RetryPolicyFactory;
import org.finra.herd.dao.S3Operations;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;

/**
 * This class tests functionality within the S3 DAO implementation.
 */
public class S3DaoImplTest extends AbstractDaoTest
{

    private static final String OTHER_EXCEPTION_MESSAGE = "OtherExceptionMessage";

    private static final String RESTORE_ALREADY_IN_PROGRESS_EXCEPTION_MESSAGE = "RestoreAlreadyInProgress";

    private static final String TEST_FILE = "UT_S3DaoImplTest_Test_File";

    @Mock
    private AwsHelper awsHelper;

    @Mock
    private JavaPropertiesHelper javaPropertiesHelper;

    @Mock
    private RetryPolicyFactory retryPolicyFactory;

    @InjectMocks
    private S3DaoImpl s3DaoImpl;

    @Mock
    private S3Operations s3Operations;

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

        // Create a retry policy.
        RetryPolicy retryPolicy =
            new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, INTEGER_VALUE, true);

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
        when(retryPolicyFactory.getRetryPolicy()).thenReturn(retryPolicy);
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
                    "(Service: null; Status Code: 0; Error Code: null; Request ID: null; S3 Extended Request ID: null; Proxy: null)", S3_KEY_PREFIX, S3_BUCKET_NAME),
                e.getMessage());
        }

        // Verify the external calls.
        verify(retryPolicyFactory, times(2)).getRetryPolicy();
        verify(s3Operations).listVersions(any(ListVersionsRequest.class), any(AmazonS3Client.class));
        verify(s3Operations).deleteObjects(any(DeleteObjectsRequest.class), any(AmazonS3Client.class));
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

        // Create a retry policy.
        RetryPolicy retryPolicy =
            new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, INTEGER_VALUE, true);

        // Create an Object Metadata with DeepArchive storage class.
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setOngoingRestore(false);
        objectMetadata.setHeader(Headers.STORAGE_CLASS, StorageClass.DeepArchive);

        ArgumentCaptor<AmazonS3Client> s3ClientCaptor = ArgumentCaptor.forClass(AmazonS3Client.class);
        ArgumentCaptor<String> s3BucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);

        // Mock the external calls.
        when(retryPolicyFactory.getRetryPolicy()).thenReturn(retryPolicy);
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

        // Create a retry policy.
        RetryPolicy retryPolicy =
            new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, INTEGER_VALUE, true);

        // Create an empty version listing.
        VersionListing versionListing = new VersionListing();

        // Mock the external calls.
        when(retryPolicyFactory.getRetryPolicy()).thenReturn(retryPolicy);
        when(s3Operations.listVersions(any(ListVersionsRequest.class), any(AmazonS3Client.class))).thenReturn(versionListing);

        // Call the method under test.
        s3DaoImpl.deleteDirectory(s3FileTransferRequestParamsDto);

        // Verify the external calls.
        verify(retryPolicyFactory).getRetryPolicy();
        verify(s3Operations).listVersions(any(ListVersionsRequest.class), any(AmazonS3Client.class));
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

        // Create an S3 file transfer request parameters DTO to tag S3 objects.
        S3FileTransferRequestParamsDto s3ObjectTaggerParamsDto = new S3FileTransferRequestParamsDto();
        s3ObjectTaggerParamsDto.setAwsAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        s3ObjectTaggerParamsDto.setAwsSecretKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        s3ObjectTaggerParamsDto.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Call the method under test with a list of S3 object summaries passed as null.
        s3DaoImpl.tagObjects(s3FileTransferRequestParamsDto, s3ObjectTaggerParamsDto, null, tag);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testTagObjectsS3ClientCreationFails()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);

        // Create an S3 file transfer request parameters DTO to tag S3 objects.
        S3FileTransferRequestParamsDto s3ObjectTaggerParamsDto = new S3FileTransferRequestParamsDto();
        s3ObjectTaggerParamsDto.setAwsAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        s3ObjectTaggerParamsDto.setAwsSecretKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        s3ObjectTaggerParamsDto.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);

        // Create an S3 object summary.
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(S3_KEY);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Mock the external calls.
        when(retryPolicyFactory.getRetryPolicy()).thenThrow(new AmazonServiceException(ERROR_MESSAGE));

        // Try to call the method under test.
        try
        {
            s3DaoImpl.tagObjects(s3FileTransferRequestParamsDto, s3ObjectTaggerParamsDto, Collections.singletonList(s3ObjectSummary), tag);
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Failed to tag S3 object with \"%s\" key and \"null\" version id in \"%s\" bucket. " +
                "Reason: %s (Service: null; Status Code: 0; Error Code: null; Request ID: null; Proxy: null)", S3_KEY, S3_BUCKET_NAME, ERROR_MESSAGE), e.getMessage());
        }

        // Verify the external calls.
        verify(retryPolicyFactory).getRetryPolicy();
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

        // Create an S3 file transfer request parameters DTO to tag S3 objects.
        S3FileTransferRequestParamsDto s3ObjectTaggerParamsDto = new S3FileTransferRequestParamsDto();
        s3ObjectTaggerParamsDto.setAwsAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        s3ObjectTaggerParamsDto.setAwsSecretKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        s3ObjectTaggerParamsDto.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Call the method under test with a list of S3 version summaries passed as null.
        s3DaoImpl.tagVersions(s3FileTransferRequestParamsDto, s3ObjectTaggerParamsDto, null, tag);

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testTagVersionsOrphanS3DeleteMarker()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);

        // Create an S3 file transfer request parameters DTO to tag S3 objects.
        S3FileTransferRequestParamsDto s3ObjectTaggerParamsDto = new S3FileTransferRequestParamsDto();
        s3ObjectTaggerParamsDto.setAwsAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        s3ObjectTaggerParamsDto.setAwsSecretKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        s3ObjectTaggerParamsDto.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);

        // Create an S3 version summary for an S3 delete marker.
        S3VersionSummary s3VersionSummary = new S3VersionSummary();
        s3VersionSummary.setKey(S3_KEY);
        s3VersionSummary.setVersionId(S3_VERSION_ID);
        s3VersionSummary.setIsDeleteMarker(true);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Call the method under test.
        s3DaoImpl.tagVersions(s3FileTransferRequestParamsDto, s3ObjectTaggerParamsDto, Collections.singletonList(s3VersionSummary), tag);

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

    private void testRestoreObjectsWithS3Exception(String exceptionMessage, int statusCode)
    {
        List<File> files = Collections.singletonList(new File(TEST_FILE));

        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(S3_KEY_PREFIX);
        s3FileTransferRequestParamsDto.setFiles(files);

        // Create a retry policy.
        RetryPolicy retryPolicy =
            new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, INTEGER_VALUE, true);

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
        when(retryPolicyFactory.getRetryPolicy()).thenReturn(retryPolicy);
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
                assertEquals(StringUtils.isNotEmpty(Tier.Standard.toString())
                    ? Tier.Standard.toString() : Tier.Bulk.toString(), requestStore.getGlacierJobParameters().getTier());
            }
        }
        catch (IllegalStateException illegalStateException)
        {
            assertEquals(String.format("Failed to initiate a restore request for \"%s\" key in \"%s\" bucket. " +
                    "Reason: com.amazonaws.services.s3.model.AmazonS3Exception: %s " +
                    "(Service: null; Status Code: %s; Error Code: null; Request ID: null; S3 Extended Request ID: null; Proxy: null), S3 Extended Request ID: null",
                TEST_FILE, S3_BUCKET_NAME, exceptionMessage, statusCode), illegalStateException.getMessage());
        }

        // Verify the external calls
        verify(retryPolicyFactory).getRetryPolicy();
        verify(s3Operations).getObjectMetadata(anyString(), anyString(), any(AmazonS3Client.class));
        verify(s3Operations).restoreObject(any(RestoreObjectRequest.class), any(AmazonS3Client.class));
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

        // Create a retry policy.
        RetryPolicy retryPolicy =
            new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, INTEGER_VALUE, true);

        // Create an Object Metadata
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setOngoingRestore(false);
        objectMetadata.setHeader(Headers.STORAGE_CLASS, storageClass);

        ArgumentCaptor<AmazonS3Client> s3ClientCaptor = ArgumentCaptor.forClass(AmazonS3Client.class);
        ArgumentCaptor<String> s3BucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RestoreObjectRequest> requestStoreCaptor = ArgumentCaptor.forClass(RestoreObjectRequest.class);

        // Mock the external calls.
        when(retryPolicyFactory.getRetryPolicy()).thenReturn(retryPolicy);
        when(s3Operations.getObjectMetadata(s3BucketNameCaptor.capture(), keyCaptor.capture(), s3ClientCaptor.capture())).thenReturn(objectMetadata);
        doNothing().when(s3Operations).restoreObject(requestStoreCaptor.capture(), s3ClientCaptor.capture());

        s3DaoImpl.restoreObjects(s3FileTransferRequestParamsDto, EXPIRATION_IN_DAYS, archiveRetrievalOption);

        RestoreObjectRequest requestStore = requestStoreCaptor.getValue();
        assertEquals(S3_BUCKET_NAME, s3BucketNameCaptor.getValue());
        assertEquals(TEST_FILE, keyCaptor.getValue());

        // Verify Bulk option is used when the option is not provided
        assertEquals(StringUtils.isNotEmpty(archiveRetrievalOption)
            ? archiveRetrievalOption : Tier.Bulk.toString(), requestStore.getGlacierJobParameters().getTier());

        // Verify the external calls
        verify(retryPolicyFactory).getRetryPolicy();
        verify(s3Operations).getObjectMetadata(anyString(), anyString(), any(AmazonS3Client.class));
        verify(s3Operations).restoreObject(any(RestoreObjectRequest.class), any(AmazonS3Client.class));
        verifyNoMoreInteractionsHelper();
    }

    private void runTagObjectsTest()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);

        // Create an S3 file transfer request parameters DTO to tag S3 objects.
        S3FileTransferRequestParamsDto s3ObjectTaggerParamsDto = new S3FileTransferRequestParamsDto();
        s3ObjectTaggerParamsDto.setAwsAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        s3ObjectTaggerParamsDto.setAwsSecretKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        s3ObjectTaggerParamsDto.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);

        // Create an S3 object summary.
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(S3_KEY);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Create a retry policy.
        RetryPolicy retryPolicy =
            new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, INTEGER_VALUE, true);

        // Create a get object tagging result.
        GetObjectTaggingResult getObjectTaggingResult = new GetObjectTaggingResult(null);

        // Create a set object tagging result.
        SetObjectTaggingResult setObjectTaggingResult = new SetObjectTaggingResult();

        // Mock the external calls.
        when(retryPolicyFactory.getRetryPolicy()).thenReturn(retryPolicy);
        when(s3Operations.getObjectTagging(any(GetObjectTaggingRequest.class), any(AmazonS3Client.class))).thenReturn(getObjectTaggingResult);
        when(s3Operations.setObjectTagging(any(SetObjectTaggingRequest.class), any(AmazonS3Client.class))).thenReturn(setObjectTaggingResult);

        // Call the method under test.
        s3DaoImpl.tagObjects(s3FileTransferRequestParamsDto, s3ObjectTaggerParamsDto, Collections.singletonList(s3ObjectSummary), tag);

        // Verify the external calls.
        verify(retryPolicyFactory, times(2)).getRetryPolicy();
        verify(s3Operations).getObjectTagging(any(GetObjectTaggingRequest.class), any(AmazonS3Client.class));
        verify(s3Operations).setObjectTagging(any(SetObjectTaggingRequest.class), any(AmazonS3Client.class));
        verifyNoMoreInteractionsHelper();
    }

    private void runTagVersionsTest()
    {
        // Create an S3 file transfer request parameters DTO to access S3 objects.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(S3_BUCKET_NAME);

        // Create an S3 file transfer request parameters DTO to tag S3 objects.
        S3FileTransferRequestParamsDto s3ObjectTaggerParamsDto = new S3FileTransferRequestParamsDto();
        s3ObjectTaggerParamsDto.setAwsAccessKeyId(AWS_ASSUMED_ROLE_ACCESS_KEY);
        s3ObjectTaggerParamsDto.setAwsSecretKey(AWS_ASSUMED_ROLE_SECRET_KEY);
        s3ObjectTaggerParamsDto.setSessionToken(AWS_ASSUMED_ROLE_SESSION_TOKEN);

        // Create an S3 version summary.
        S3VersionSummary s3VersionSummary = new S3VersionSummary();
        s3VersionSummary.setKey(S3_KEY);
        s3VersionSummary.setVersionId(S3_VERSION_ID);

        // Create an S3 object tag.
        Tag tag = new Tag(S3_OBJECT_TAG_KEY, S3_OBJECT_TAG_VALUE);

        // Create a retry policy.
        RetryPolicy retryPolicy =
            new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, INTEGER_VALUE, true);

        // Create a get object tagging result.
        GetObjectTaggingResult getObjectTaggingResult = new GetObjectTaggingResult(null);

        // Create a set object tagging result.
        SetObjectTaggingResult setObjectTaggingResult = new SetObjectTaggingResult();

        // Mock the external calls.
        when(retryPolicyFactory.getRetryPolicy()).thenReturn(retryPolicy);
        when(s3Operations.getObjectTagging(any(GetObjectTaggingRequest.class), any(AmazonS3Client.class))).thenReturn(getObjectTaggingResult);
        when(s3Operations.setObjectTagging(any(SetObjectTaggingRequest.class), any(AmazonS3Client.class))).thenReturn(setObjectTaggingResult);

        // Call the method under test.
        s3DaoImpl.tagVersions(s3FileTransferRequestParamsDto, s3ObjectTaggerParamsDto, Collections.singletonList(s3VersionSummary), tag);

        // Verify the external calls.
        verify(retryPolicyFactory, times(2)).getRetryPolicy();
        verify(s3Operations).getObjectTagging(any(GetObjectTaggingRequest.class), any(AmazonS3Client.class));
        verify(s3Operations).setObjectTagging(any(SetObjectTaggingRequest.class), any(AmazonS3Client.class));
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(awsHelper, javaPropertiesHelper, retryPolicyFactory, s3Operations);
    }
}
