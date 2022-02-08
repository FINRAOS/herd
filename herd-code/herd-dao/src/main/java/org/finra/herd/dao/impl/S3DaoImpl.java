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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.GlacierJobParameters;
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
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.Tier;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.CreateJobRequest;
import com.amazonaws.services.s3control.model.CreateJobResult;
import com.amazonaws.services.s3control.model.DescribeJobRequest;
import com.amazonaws.services.s3control.model.DescribeJobResult;
import com.amazonaws.services.s3control.model.JobProgressSummary;
import com.amazonaws.services.s3control.model.JobStatus;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.AwsS3ClientFactory;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.S3Operations;
import org.finra.herd.dao.exception.S3BatchJobIncompleteException;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.helper.S3BatchHelper;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.dto.BatchJobConfigDto;
import org.finra.herd.model.dto.BatchJobManifestDto;
import org.finra.herd.model.dto.S3FileCopyRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferResultsDto;
import org.finra.herd.model.dto.S3ObjectTaggerRoleParamsDto;

/**
 * The S3 DAO implementation.
 */
// TODO: Refactor S3 Dao implementation and remove the PMD suppress warning statement below.
@SuppressWarnings("PMD.TooManyMethods")
@Repository
public class S3DaoImpl implements S3Dao
{
    private static final long DEFAULT_SLEEP_INTERVAL_MILLIS = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(S3DaoImpl.class);

    private static final int MAX_KEYS_PER_DELETE_REQUEST = 1000;

    private static final List<JobStatus> FINAL_BATCH_PROCESSING_STATES =
        Arrays.asList(JobStatus.Complete, JobStatus.Failed, JobStatus.Cancelled, JobStatus.Suspended);

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private AwsS3ClientFactory awsS3ClientFactory;

    @Autowired
    private S3BatchHelper batchHelper;

    @Autowired
    private JavaPropertiesHelper javaPropertiesHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private S3Operations s3Operations;

    private long sleepIntervalsMillis = DEFAULT_SLEEP_INTERVAL_MILLIS;

    @Override
    public int abortMultipartUploads(S3FileTransferRequestParamsDto params, Date thresholdDate)
    {
        // Create an Amazon S3 client.
        AmazonS3Client s3Client = awsS3ClientFactory.getAmazonS3Client(params);
        int abortedMultipartUploadsCount = 0;

        try
        {
            // List upload markers. Null implies initial list request.
            String uploadIdMarker = null;
            String keyMarker = null;

            boolean truncated;
            do
            {
                // Create the list multipart request, optionally using the last markers.
                ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(params.getS3BucketName());
                request.setUploadIdMarker(uploadIdMarker);
                request.setKeyMarker(keyMarker);

                // Request the multipart upload listing.
                MultipartUploadListing uploadListing = s3Operations.listMultipartUploads(TransferManager.appendSingleObjectUserAgent(request), s3Client);

                for (MultipartUpload upload : uploadListing.getMultipartUploads())
                {
                    if (upload.getInitiated().compareTo(thresholdDate) < 0)
                    {
                        // Abort the upload.
                        s3Operations.abortMultipartUpload(TransferManager.appendSingleObjectUserAgent(
                            new AbortMultipartUploadRequest(params.getS3BucketName(), upload.getKey(), upload.getUploadId())), s3Client);

                        // Log the information about the aborted multipart upload.
                        LOGGER.info("Aborted S3 multipart upload. s3Key=\"{}\" s3BucketName=\"{}\" s3MultipartUploadInitiatedDate=\"{}\"", upload.getKey(),
                            params.getS3BucketName(), upload.getInitiated());

                        // Increment the counter.
                        abortedMultipartUploadsCount++;
                    }
                }

                // Determine whether there are more uploads to list.
                truncated = uploadListing.isTruncated();
                if (truncated)
                {
                    // Record the list markers.
                    uploadIdMarker = uploadListing.getNextUploadIdMarker();
                    keyMarker = uploadListing.getNextKeyMarker();
                }
            }
            while (truncated);
        }
        finally
        {
            // Shutdown the Amazon S3 client instance to release resources.
            s3Client.shutdown();
        }

        return abortedMultipartUploadsCount;
    }

    @Override
    public S3FileTransferResultsDto copyFile(final S3FileCopyRequestParamsDto params) throws InterruptedException
    {
        LOGGER.info("Copying S3 object... sourceS3Key=\"{}\" sourceS3BucketName=\"{}\" targetS3Key=\"{}\" targetS3BucketName=\"{}\"",
            params.getSourceObjectKey(), params.getSourceBucketName(), params.getTargetObjectKey(), params.getTargetBucketName());

        // Perform the copy.
        S3FileTransferResultsDto results = performTransfer(params, new Transferer()
        {
            @Override
            public Transfer performTransfer(TransferManager transferManager)
            {
                // Create a copy request.
                CopyObjectRequest copyObjectRequest =
                    new CopyObjectRequest(params.getSourceBucketName(), params.getSourceObjectKey(), params.getTargetBucketName(), params.getTargetObjectKey());

                // If KMS Key ID is specified, set the AWS Key Management System parameters to be used to encrypt the object.
                if (StringUtils.isNotBlank(params.getKmsKeyId()))
                {
                    copyObjectRequest.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(params.getKmsKeyId()));
                }
                // Otherwise, specify the server-side encryption algorithm for encrypting the object using AWS-managed keys.
                else
                {
                    ObjectMetadata metadata = new ObjectMetadata();
                    metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                    copyObjectRequest.setNewObjectMetadata(metadata);
                }

                return s3Operations.copyFile(copyObjectRequest, transferManager);
            }
        });

        LOGGER.info("Copied S3 object. sourceS3Key=\"{}\" sourceS3BucketName=\"{}\" targetS3Key=\"{}\" targetS3BucketName=\"{}\" " +
                "totalBytesTransferred={} transferDuration=\"{}\"", params.getSourceObjectKey(), params.getSourceBucketName(), params.getTargetObjectKey(),
            params.getTargetBucketName(), results.getTotalBytesTransferred(), HerdDateUtils.formatDuration(results.getDurationMillis()));

        logOverallTransferRate(results);

        return results;
    }

    @Override
    public void createDirectory(final S3FileTransferRequestParamsDto params)
    {
        createDirectory(params, false);
    }

    @Override
    public void createEmptyDirectory(final S3FileTransferRequestParamsDto params)
    {
        createDirectory(params, true);
    }

    @Override
    public void deleteDirectory(final S3FileTransferRequestParamsDto params)
    {
        LOGGER.info("Deleting keys/key versions from S3... s3KeyPrefix=\"{}\" s3BucketName=\"{}\"", params.getS3KeyPrefix(), params.getS3BucketName());

        Assert.isTrue(!isRootKeyPrefix(params.getS3KeyPrefix()), "Deleting from root directory is not allowed.");

        try
        {
            // List S3 versions.
            List<S3VersionSummary> s3VersionSummaries = listVersions(params);
            LOGGER.info("Found keys/key versions in S3 for deletion. s3KeyCount={} s3KeyPrefix=\"{}\" s3BucketName=\"{}\"", s3VersionSummaries.size(),
                params.getS3KeyPrefix(), params.getS3BucketName());

            // In order to avoid a MalformedXML AWS exception, we send delete request only when we have any key versions to delete.
            if (CollectionUtils.isNotEmpty(s3VersionSummaries))
            {
                // Create an S3 client.
                AmazonS3Client s3Client = awsS3ClientFactory.getAmazonS3Client(params);

                // Build a list of objects to be deleted.
                List<DeleteObjectsRequest.KeyVersion> keyVersions = new ArrayList<>();
                for (S3VersionSummary s3VersionSummary : s3VersionSummaries)
                {
                    keyVersions.add(new DeleteObjectsRequest.KeyVersion(s3VersionSummary.getKey(), s3VersionSummary.getVersionId()));
                }

                try
                {
                    // Delete the key versions.
                    deleteKeyVersions(s3Client, params.getS3BucketName(), keyVersions);
                }
                finally
                {
                    s3Client.shutdown();
                }
            }
        }
        catch (AmazonClientException e)
        {
            throw new IllegalStateException(
                String.format("Failed to delete keys/key versions with prefix \"%s\" from bucket \"%s\". Reason: %s", params.getS3KeyPrefix(),
                    params.getS3BucketName(), e.getMessage()), e);
        }
    }

    @Override
    public void deleteFileList(final S3FileTransferRequestParamsDto params)
    {
        LOGGER.info("Deleting a list of objects from S3... s3BucketName=\"{}\" s3KeyCount={}", params.getS3BucketName(), params.getFiles().size());

        try
        {
            // In order to avoid a MalformedXML AWS exception, we send delete request only when we have any keys to delete.
            if (!params.getFiles().isEmpty())
            {
                // Create an S3 client.
                AmazonS3Client s3Client = awsS3ClientFactory.getAmazonS3Client(params);

                try
                {
                    // Build a list of keys to be deleted.
                    List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
                    for (File file : params.getFiles())
                    {
                        keys.add(new DeleteObjectsRequest.KeyVersion(file.getPath().replaceAll("\\\\", "/")));
                    }

                    // Delete the keys.
                    deleteKeyVersions(s3Client, params.getS3BucketName(), keys);
                }
                finally
                {
                    s3Client.shutdown();
                }
            }
        }
        catch (Exception e)
        {
            throw new IllegalStateException(
                String.format("Failed to delete a list of keys from bucket \"%s\". Reason: %s", params.getS3BucketName(), e.getMessage()), e);
        }
    }

    @Override
    public S3FileTransferResultsDto downloadDirectory(final S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        LOGGER.info("Downloading S3 directory to the local system... s3KeyPrefix=\"{}\" s3BucketName=\"{}\" localDirectory=\"{}\"", params.getS3KeyPrefix(),
            params.getS3BucketName(), params.getLocalPath());

        // Note that the directory download always recursively copies sub-directories.
        // To not recurse, we would have to list the files on S3 (AmazonS3Client.html#listObjects) and manually copy them one at a time.

        // Perform the transfer.
        S3FileTransferResultsDto results = performTransfer(params, new Transferer()
        {
            @Override
            public Transfer performTransfer(TransferManager transferManager)
            {
                return s3Operations.downloadDirectory(params.getS3BucketName(), params.getS3KeyPrefix(), new File(params.getLocalPath()), transferManager);
            }
        });

        LOGGER.info("Downloaded S3 directory to the local system. " +
                "s3KeyPrefix=\"{}\" s3BucketName=\"{}\" localDirectory=\"{}\" s3KeyCount={} totalBytesTransferred={} transferDuration=\"{}\"",
            params.getS3KeyPrefix(), params.getS3BucketName(), params.getLocalPath(), results.getTotalFilesTransferred(), results.getTotalBytesTransferred(),
            HerdDateUtils.formatDuration(results.getDurationMillis()));

        logOverallTransferRate(results);

        return results;
    }

    @Override
    public S3FileTransferResultsDto downloadFile(final S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        LOGGER.info("Downloading S3 file... s3Key=\"{}\" s3BucketName=\"{}\" localPath=\"{}\"", params.getS3KeyPrefix(), params.getS3BucketName(),
            params.getLocalPath());

        // Perform the transfer.
        S3FileTransferResultsDto results = performTransfer(params, new Transferer()
        {
            @Override
            public Transfer performTransfer(TransferManager transferManager)
            {
                return s3Operations.download(params.getS3BucketName(), params.getS3KeyPrefix(), new File(params.getLocalPath()), transferManager);
            }
        });

        LOGGER.info(
            "Downloaded S3 file to the local system. s3Key=\"{}\" s3BucketName=\"{}\" localPath=\"{}\" totalBytesTransferred={} transferDuration=\"{}\"",
            params.getS3KeyPrefix(), params.getS3BucketName(), params.getLocalPath(), results.getTotalBytesTransferred(),
            HerdDateUtils.formatDuration(results.getDurationMillis()));

        logOverallTransferRate(results);

        return results;
    }

    @Override
    public String generateGetObjectPresignedUrl(String bucketName, String key, Date expiration, S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto)
    {
        GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, key, HttpMethod.GET);
        generatePresignedUrlRequest.setExpiration(expiration);
        AmazonS3Client s3 = awsS3ClientFactory.getAmazonS3Client(s3FileTransferRequestParamsDto);
        try
        {
            return s3Operations.generatePresignedUrl(generatePresignedUrlRequest, s3).toString();
        }
        finally
        {
            s3.shutdown();
        }
    }

    @Override
    public ObjectMetadata getObjectMetadata(final S3FileTransferRequestParamsDto params)
    {
        AmazonS3Client s3Client = awsS3ClientFactory.getAmazonS3Client(params);

        try
        {
            return s3Operations.getObjectMetadata(params.getS3BucketName(), params.getS3KeyPrefix(), s3Client);
        }
        catch (AmazonServiceException e)
        {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND)
            {
                return null;
            }

            throw new IllegalStateException(
                String.format("Failed to get S3 metadata for object key \"%s\" from bucket \"%s\". Reason: %s", params.getS3KeyPrefix(),
                    params.getS3BucketName(), e.getMessage()), e);
        }
        finally
        {
            // Shutdown the AmazonS3Client instance to release resources.
            s3Client.shutdown();
        }
    }

    @Override
    public Properties getProperties(String bucketName, String key, S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto)
    {
        AmazonS3Client s3Client = awsS3ClientFactory.getAmazonS3Client(s3FileTransferRequestParamsDto);

        try
        {
            S3Object s3Object = getS3Object(s3Client, bucketName, key, true);
            return javaPropertiesHelper.getProperties(s3Object.getObjectContent());
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException("The properties file in S3 bucket '" + bucketName + "' and key '" + key + "' is invalid.", e);
        }
        finally
        {
            s3Client.shutdown();
        }
    }

    @Override
    public List<S3ObjectSummary> listDirectory(final S3FileTransferRequestParamsDto params)
    {
        // By default, we do not ignore 0 byte objects that represent S3 directories.
        return listDirectory(params, false);
    }

    @Override
    public List<S3ObjectSummary> listDirectory(final S3FileTransferRequestParamsDto params, boolean ignoreZeroByteDirectoryMarkers)
    {
        Assert.isTrue(!isRootKeyPrefix(params.getS3KeyPrefix()), "Listing of S3 objects from root directory is not allowed.");

        AmazonS3Client s3Client = awsS3ClientFactory.getAmazonS3Client(params);
        List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();

        try
        {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(params.getS3BucketName()).withPrefix(params.getS3KeyPrefix());
            ObjectListing objectListing;

            do
            {
                objectListing = s3Operations.listObjects(listObjectsRequest, s3Client);

                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries())
                {
                    // Ignore 0 byte objects that represent S3 directories.
                    if (!(ignoreZeroByteDirectoryMarkers && objectSummary.getKey().endsWith("/") && objectSummary.getSize() == 0L))
                    {
                        s3ObjectSummaries.add(objectSummary);
                    }
                }

                listObjectsRequest.setMarker(objectListing.getNextMarker());
            }
            while (objectListing.isTruncated());
        }
        catch (AmazonS3Exception amazonS3Exception)
        {
            if (S3Operations.ERROR_CODE_NO_SUCH_BUCKET.equals(amazonS3Exception.getErrorCode()))
            {
                throw new IllegalArgumentException("The specified bucket '" + params.getS3BucketName() + "' does not exist.", amazonS3Exception);
            }
            throw new IllegalStateException("Error accessing S3", amazonS3Exception);
        }
        catch (AmazonClientException e)
        {
            throw new IllegalStateException(
                String.format("Failed to list keys with prefix \"%s\" from bucket \"%s\". Reason: %s", params.getS3KeyPrefix(), params.getS3BucketName(),
                    e.getMessage()), e);
        }
        finally
        {
            // Shutdown the AmazonS3Client instance to release resources.
            s3Client.shutdown();
        }

        return s3ObjectSummaries;
    }

    @Override
    public List<S3VersionSummary> listVersions(final S3FileTransferRequestParamsDto params)
    {
        Assert.isTrue(!isRootKeyPrefix(params.getS3KeyPrefix()), "Listing of S3 versions from root directory is not allowed.");

        AmazonS3Client s3Client = awsS3ClientFactory.getAmazonS3Client(params);
        List<S3VersionSummary> s3VersionSummaries = new ArrayList<>();

        try
        {
            ListVersionsRequest listVersionsRequest = new ListVersionsRequest().withBucketName(params.getS3BucketName()).withPrefix(params.getS3KeyPrefix());
            VersionListing versionListing;

            do
            {
                versionListing = s3Operations.listVersions(listVersionsRequest, s3Client);
                s3VersionSummaries.addAll(versionListing.getVersionSummaries());
                listVersionsRequest.setKeyMarker(versionListing.getNextKeyMarker());
                listVersionsRequest.setVersionIdMarker(versionListing.getNextVersionIdMarker());
            }
            while (versionListing.isTruncated());
        }
        catch (AmazonS3Exception amazonS3Exception)
        {
            if (S3Operations.ERROR_CODE_NO_SUCH_BUCKET.equals(amazonS3Exception.getErrorCode()))
            {
                throw new IllegalArgumentException("The specified bucket '" + params.getS3BucketName() + "' does not exist.", amazonS3Exception);
            }
            throw new IllegalStateException("Error accessing S3", amazonS3Exception);
        }
        catch (AmazonClientException e)
        {
            throw new IllegalStateException(
                String.format("Failed to list S3 versions with prefix \"%s\" from bucket \"%s\". Reason: %s", params.getS3KeyPrefix(), params.getS3BucketName(),
                    e.getMessage()), e);
        }
        finally
        {
            // Shutdown the AmazonS3Client instance to release resources.
            s3Client.shutdown();
        }

        return s3VersionSummaries;
    }

    @Override
    public void restoreObjects(final S3FileTransferRequestParamsDto params, int expirationInDays, String archiveRetrievalOption)
    {
        LOGGER.info("Restoring a list of objects in S3... s3KeyPrefix=\"{}\" s3BucketName=\"{}\" s3KeyCount={}", params.getS3KeyPrefix(),
            params.getS3BucketName(), params.getFiles().size());

        if (CollectionUtils.isEmpty(params.getFiles()))
        {
            return;
        }

        // Initialize a key value pair for the error message in the catch block.
        String key = params.getFiles().get(0).getPath().replaceAll("\\\\", "/");

        try
        {
            // Create an S3 client.
            AmazonS3Client s3Client = awsS3ClientFactory.getAmazonS3Client(params);

            // Create a restore object request.
            RestoreObjectRequest requestRestore = new RestoreObjectRequest(params.getS3BucketName(), null, expirationInDays);
            // Make Bulk the default archive retrieval option if the option is not provided
            requestRestore.setGlacierJobParameters(
                new GlacierJobParameters().withTier(StringUtils.isNotEmpty(archiveRetrievalOption) ? archiveRetrievalOption : Tier.Bulk.toString()));

            try
            {
                for (File file : params.getFiles())
                {
                    key = file.getPath().replaceAll("\\\\", "/");
                    ObjectMetadata objectMetadata = s3Operations.getObjectMetadata(params.getS3BucketName(), key, s3Client);

                    // Request a restore for objects that are not already being restored.
                    if (BooleanUtils.isNotTrue(objectMetadata.getOngoingRestore()))
                    {
                        requestRestore.setKey(key);

                        try
                        {
                            // Try the S3 restore operation on this file.
                            s3Operations.restoreObject(requestRestore, s3Client);
                        }
                        catch (AmazonS3Exception amazonS3Exception)
                        {
                            // If this exception has a status code of 409, log the information and continue to the next file.
                            if (amazonS3Exception.getStatusCode() == HttpStatus.SC_CONFLICT)
                            {
                                LOGGER.info("Restore already in progress for file with s3Key=\"{}\".", key);
                            }
                            // Else, we need to propagate the exception to the next level of try/catch block.
                            else
                            {
                                throw new Exception(amazonS3Exception);
                            }
                        }
                    }
                }
            }
            finally
            {
                s3Client.shutdown();
            }
        }
        catch (Exception e)
        {
            if (StringUtils.contains(e.getMessage(), "Retrieval option is not supported by this storage class"))
            {
                throw new IllegalArgumentException(
                    String.format("Failed to initiate a restore request for \"%s\" key in \"%s\" bucket. Reason: %s", key, params.getS3BucketName(),
                        e.getMessage()), e);
            }
            else
            {
                throw new IllegalStateException(
                    String.format("Failed to initiate a restore request for \"%s\" key in \"%s\" bucket. Reason: %s", key, params.getS3BucketName(),
                        e.getMessage()), e);
            }
        }
    }

    @Override
    public boolean s3FileExists(S3FileTransferRequestParamsDto params) throws RuntimeException
    {
        AmazonS3Client s3Client = awsS3ClientFactory.getAmazonS3Client(params);

        try
        {
            S3Object s3Object = getS3Object(s3Client, params.getS3BucketName(), params.getS3KeyPrefix(), false);
            return (s3Object != null);
        }
        finally
        {
            s3Client.shutdown();
        }
    }

    @Override
    public void tagObjects(final S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, final S3ObjectTaggerRoleParamsDto s3ObjectTaggerRoleParamsDto,
        final List<S3ObjectSummary> s3ObjectSummaries, final Tag tag)
    {
        LOGGER.info("Tagging objects in S3... s3BucketName=\"{}\" s3KeyPrefix=\"{}\" s3KeyCount={} s3ObjectTagKey=\"{}\" s3ObjectTagValue=\"{}\"",
            s3FileTransferRequestParamsDto.getS3BucketName(), s3FileTransferRequestParamsDto.getS3KeyPrefix(), CollectionUtils.size(s3ObjectSummaries),
            tag.getKey(), tag.getValue());

        if (!CollectionUtils.isEmpty(s3ObjectSummaries))
        {
            // Convert a list of S3 object summaries to S3 version summaries without version identifiers.
            List<S3VersionSummary> s3VersionSummaries = new ArrayList<>();
            for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries)
            {
                S3VersionSummary s3VersionSummary = new S3VersionSummary();
                s3VersionSummary.setBucketName(s3ObjectSummary.getBucketName());
                s3VersionSummary.setKey(s3ObjectSummary.getKey());
                s3VersionSummaries.add(s3VersionSummary);
            }

            // Tag S3 objects.
            tagVersionsHelper(s3FileTransferRequestParamsDto, s3ObjectTaggerRoleParamsDto, s3VersionSummaries, tag);

            // Log a list of files tagged in the S3 bucket.
            if (LOGGER.isInfoEnabled())
            {
                LOGGER.info("Successfully tagged files in S3 bucket. " +
                        "s3BucketName=\"{}\" s3KeyPrefix=\"{}\" s3KeyCount={} s3ObjectTagKey=\"{}\" s3ObjectTagValue=\"{}\"",
                    s3FileTransferRequestParamsDto.getS3BucketName(), s3FileTransferRequestParamsDto.getS3KeyPrefix(), CollectionUtils.size(s3ObjectSummaries),
                    tag.getKey(), tag.getValue());

                for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries)
                {
                    LOGGER.info("s3Key=\"{}\"", s3ObjectSummary.getKey());
                }
            }
        }
    }

    @Override
    public void tagVersions(final S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, final S3ObjectTaggerRoleParamsDto s3ObjectTaggerRoleParamsDto,
        final List<S3VersionSummary> s3VersionSummaries, final Tag tag)
    {
        // Eliminate delete markers from the list of version summaries to be tagged.
        List<S3VersionSummary> s3VersionSummariesWithoutDeleteMarkers = null;
        if (CollectionUtils.isNotEmpty(s3VersionSummaries))
        {
            s3VersionSummariesWithoutDeleteMarkers =
                s3VersionSummaries.stream().filter(s3VersionSummary -> !s3VersionSummary.isDeleteMarker()).collect(Collectors.toList());
        }

        LOGGER.info("Tagging versions in S3... s3BucketName=\"{}\" s3KeyPrefix=\"{}\" s3VersionCount={} s3ObjectTagKey=\"{}\" s3ObjectTagValue=\"{}\" " +
                "Excluding from tagging S3 delete markers... s3DeleteMarkerCount={}", s3FileTransferRequestParamsDto.getS3BucketName(),
            s3FileTransferRequestParamsDto.getS3KeyPrefix(), CollectionUtils.size(s3VersionSummariesWithoutDeleteMarkers), tag.getKey(), tag.getValue(),
            CollectionUtils.size(s3VersionSummaries) - CollectionUtils.size(s3VersionSummariesWithoutDeleteMarkers));

        if (CollectionUtils.isNotEmpty(s3VersionSummariesWithoutDeleteMarkers))
        {
            // Tag S3 versions.
            tagVersionsHelper(s3FileTransferRequestParamsDto, s3ObjectTaggerRoleParamsDto, s3VersionSummariesWithoutDeleteMarkers, tag);

            // Log a list of S3 versions that got tagged.
            if (LOGGER.isInfoEnabled())
            {
                LOGGER.info("Successfully tagged versions in S3 bucket. " +
                        "s3BucketName=\"{}\" s3KeyPrefix=\"{}\" s3VersionCount={} s3ObjectTagKey=\"{}\" s3ObjectTagValue=\"{}\"",
                    s3FileTransferRequestParamsDto.getS3BucketName(), s3FileTransferRequestParamsDto.getS3KeyPrefix(),
                    s3VersionSummariesWithoutDeleteMarkers.size(), tag.getKey(), tag.getValue());

                for (S3VersionSummary s3VersionSummary : s3VersionSummariesWithoutDeleteMarkers)
                {
                    LOGGER.info("s3Key=\"{}\" s3VersionId=\"{}\"", s3VersionSummary.getKey(), s3VersionSummary.getVersionId());
                }
            }
        }
    }

    @Override
    public void batchTagVersions(final S3FileTransferRequestParamsDto params, final BatchJobConfigDto batchJobConfig,
        final List<S3VersionSummary> s3VersionSummaries, final Tag tag)
    {
        LOGGER.info("Batch tagging a list of object version in S3... s3KeyPrefix=\"{}\" s3BucketName=\"{}\" s3KeyCount={} s3ObjectVersionsCount={} tag=\"{}\"",
            params.getS3KeyPrefix(), params.getS3BucketName(), params.getFiles().size(), s3VersionSummaries.size(), new JsonHelper().objectToJson(tag));

        // Do nothing if no file versions to tag.
        if (CollectionUtils.isEmpty(s3VersionSummaries))
        {
            return;
        }

        // Create S3 Batch tagging job
        String jobId = createBatchVersionsTaggingJob(params, batchJobConfig, s3VersionSummaries, tag);

        // Template class that simplifies the execution of operations with retry semantics executed by spring framework.
        RetryTemplate template = new RetryTemplate();

        // This policy determine how many repetitions this retry operation is going to do and which exceptions should be considered as repeatable.
        SimpleRetryPolicy policy = new SimpleRetryPolicy(batchJobConfig.getMaxAttempts(), Collections.singletonMap(S3BatchJobIncompleteException.class, true));
        template.setRetryPolicy(policy);

        // This policy is used to wait fixed amount of time before making another retry.
        FixedBackOffPolicy backoffPolicy = new FixedBackOffPolicy();

        // Reading backoff timeout value from configuration and assign to policy.
        backoffPolicy.setBackOffPeriod(batchJobConfig.getBackoffPeriod());
        template.setBackOffPolicy(backoffPolicy);

        // Turn off automatic re-throw of the exception after last repetition.
        template.setThrowLastExceptionOnExhausted(false);

        // Retry running provided lambda according to the retry and backoff policies assigned earlier.
        DescribeJobResult result = template.execute((RetryCallback<DescribeJobResult, S3BatchJobIncompleteException>) context -> {
            // Read current state of S3 Batch job.
            DescribeJobResult retryResult = getBatchJobDescription(params, batchJobConfig, jobId);

            // Read and check current status of the job.
            JobStatus jobStatus = JobStatus.fromValue(retryResult.getJob().getStatus());
            if (!FINAL_BATCH_PROCESSING_STATES.contains(jobStatus))
            {
                // If the job is still not finished (successfully or not) throw exception, which serves as a signal to make another retry
                throw new S3BatchJobIncompleteException(retryResult);
            }

            return retryResult;
        }, context -> {
            // If last retry finished with still incomplete status, extract the job descriptor and return it as a result
            if (context.getLastThrowable() instanceof S3BatchJobIncompleteException)
            {
                return ((S3BatchJobIncompleteException) context.getLastThrowable()).getJobDescriptor();
            }
            else
            {
                // If last describe job finished with different error re-throw it further
                throw new IllegalStateException(context.getLastThrowable());
            }
        });

        // Fail with exception if unable to retrieve descriptor of the batch job from AWS S3 after several retries
        if (result == null || result.getJob() == null || result.getJob().getStatus() == null)
        {
            throw new IllegalStateException("Unable to retrieve descriptor of the batch job");
        }

        // if after configured number of retries the job is still not complete - throw IllegalStateException
        JobStatus jobStatus = JobStatus.fromValue(result.getJob().getStatus());
        if (jobStatus != JobStatus.Complete)
        {
            throw new IllegalStateException(String.format("S3 batch job was not complete. Detailed descriptor: %s ", result));
        }

        // check if all tasks in the job completed successfully
        JobProgressSummary progressSummary = result.getJob().getProgressSummary();
        if (progressSummary.getNumberOfTasksFailed() > 0)
        {
            throw new IllegalStateException(
                String.format("S3 batch job was complete with errors. Job report includes detailed results for each failed task. " + "Detailed descriptor: %s ",
                    result));
        }
    }

    @Override
    public S3FileTransferResultsDto uploadDirectory(final S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        LOGGER.info("Uploading local directory to S3... localDirectory=\"{}\" s3KeyPrefix=\"{}\" s3BucketName=\"{}\"", params.getLocalPath(),
            params.getS3KeyPrefix(), params.getS3BucketName());

        // Perform the transfer.
        S3FileTransferResultsDto results = performTransfer(params, new Transferer()
        {
            @Override
            public Transfer performTransfer(TransferManager transferManager)
            {
                return s3Operations.uploadDirectory(params.getS3BucketName(), params.getS3KeyPrefix(), new File(params.getLocalPath()), params.isRecursive(),
                    new ObjectMetadataProvider()
                    {
                        @Override
                        public void provideObjectMetadata(File file, ObjectMetadata metadata)
                        {
                            prepareMetadata(params, metadata);
                        }
                    }, transferManager);
            }
        });

        LOGGER.info("Uploaded local directory to S3. " +
                "localDirectory=\"{}\" s3KeyPrefix=\"{}\" s3BucketName=\"{}\" s3KeyCount={} totalBytesTransferred={} transferDuration=\"{}\"",
            params.getLocalPath(), params.getS3KeyPrefix(), params.getS3BucketName(), results.getTotalFilesTransferred(), results.getTotalBytesTransferred(),
            HerdDateUtils.formatDuration(results.getDurationMillis()));

        logOverallTransferRate(results);

        return results;
    }

    @Override
    public S3FileTransferResultsDto uploadFile(final S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        LOGGER.info("Uploading local file to S3... localPath=\"{}\" s3Key=\"{}\" s3BucketName=\"{}\"", params.getLocalPath(), params.getS3KeyPrefix(),
            params.getS3BucketName());

        // Perform the transfer.
        S3FileTransferResultsDto results = performTransfer(params, new Transferer()
        {
            @Override
            public Transfer performTransfer(TransferManager transferManager)
            {
                // Get a handle to the local file.
                File localFile = new File(params.getLocalPath());

                // Create and prepare the metadata.
                ObjectMetadata metadata = new ObjectMetadata();
                prepareMetadata(params, metadata);

                // Create a put request and a transfer manager with the parameters and the metadata.
                PutObjectRequest putObjectRequest = new PutObjectRequest(params.getS3BucketName(), params.getS3KeyPrefix(), localFile);
                putObjectRequest.setMetadata(metadata);

                return s3Operations.upload(putObjectRequest, transferManager);
            }
        });

        LOGGER.info("Uploaded local file to the S3. localPath=\"{}\" s3Key=\"{}\" s3BucketName=\"{}\" totalBytesTransferred={} transferDuration=\"{}\"",
            params.getLocalPath(), params.getS3KeyPrefix(), params.getS3BucketName(), results.getTotalBytesTransferred(),
            HerdDateUtils.formatDuration(results.getDurationMillis()));

        logOverallTransferRate(results);

        return results;
    }

    @Override
    public S3FileTransferResultsDto uploadFileList(final S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        LOGGER.info("Uploading a list of files from the local directory to S3... localDirectory=\"{}\" s3KeyPrefix=\"{}\" s3BucketName=\"{}\" s3KeyCount={}",
            params.getLocalPath(), params.getS3KeyPrefix(), params.getS3BucketName(), params.getFiles().size());

        if (LOGGER.isInfoEnabled())
        {
            for (File file : params.getFiles())
            {
                LOGGER.info("s3Key=\"{}\"", file.getPath());
            }
        }

        // Perform the transfer.
        S3FileTransferResultsDto results = performTransfer(params, new Transferer()
        {
            @Override
            public Transfer performTransfer(TransferManager transferManager)
            {
                return s3Operations.uploadFileList(params.getS3BucketName(), params.getS3KeyPrefix(), new File(params.getLocalPath()), params.getFiles(),
                    new ObjectMetadataProvider()
                    {
                        @Override
                        public void provideObjectMetadata(File file, ObjectMetadata metadata)
                        {
                            prepareMetadata(params, metadata);
                        }
                    }, transferManager);
            }
        });

        LOGGER.info("Uploaded list of files from the local directory to S3. " +
                "localDirectory=\"{}\" s3KeyPrefix=\"{}\" s3BucketName=\"{}\" s3KeyCount={} totalBytesTransferred={} transferDuration=\"{}\"",
            params.getLocalPath(), params.getS3KeyPrefix(), params.getS3BucketName(), results.getTotalFilesTransferred(), results.getTotalBytesTransferred(),
            HerdDateUtils.formatDuration(results.getDurationMillis()));

        logOverallTransferRate(results);

        return results;
    }

    @Override
    public void validateGlacierS3FilesRestored(S3FileTransferRequestParamsDto params) throws RuntimeException
    {
        LOGGER.info("Checking for already restored Glacier or DeepArchive storage class objects... s3KeyPrefix=\"{}\" s3BucketName=\"{}\" s3KeyCount={}",
            params.getS3KeyPrefix(), params.getS3BucketName(), params.getFiles().size());

        if (!CollectionUtils.isEmpty(params.getFiles()))
        {
            // Initialize a key value pair for the error message in the catch block.
            String key = params.getFiles().get(0).getPath().replaceAll("\\\\", "/");

            try
            {
                // Create an S3 client.
                AmazonS3Client s3Client = awsS3ClientFactory.getAmazonS3Client(params);

                try
                {
                    for (File file : params.getFiles())
                    {
                        key = file.getPath().replaceAll("\\\\", "/");
                        ObjectMetadata objectMetadata = s3Operations.getObjectMetadata(params.getS3BucketName(), key, s3Client);

                        // Fail if a not already restored object is detected.
                        if (BooleanUtils.isNotFalse(objectMetadata.getOngoingRestore()))
                        {
                            throw new IllegalArgumentException(
                                String.format("Archived S3 file \"%s\" is not restored. StorageClass {%s}, OngoingRestore flag {%s}, S3 bucket name {%s}", key,
                                    objectMetadata.getStorageClass(), objectMetadata.getOngoingRestore(), params.getS3BucketName()));
                        }
                    }
                }
                finally
                {
                    s3Client.shutdown();
                }
            }
            catch (AmazonServiceException e)
            {
                throw new IllegalStateException(
                    String.format("Fail to check restore status for \"%s\" key in \"%s\" bucket. Reason: %s", key, params.getS3BucketName(), e.getMessage()),
                    e);
            }
        }
    }

    @Override
    public void validateS3File(S3FileTransferRequestParamsDto params, Long fileSizeInBytes) throws RuntimeException
    {
        ObjectMetadata objectMetadata = getObjectMetadata(params);

        if (objectMetadata == null)
        {
            throw new ObjectNotFoundException(String.format("File not found at s3://%s/%s location.", params.getS3BucketName(), params.getS3KeyPrefix()));
        }

        Assert.isTrue(fileSizeInBytes == null || Objects.equals(fileSizeInBytes, objectMetadata.getContentLength()),
            String.format("Specified file size (%d bytes) does not match to the actual file size (%d bytes) reported by S3 for s3://%s/%s file.",
                fileSizeInBytes, objectMetadata.getContentLength(), params.getS3BucketName(), params.getS3KeyPrefix()));
    }

    @Override
    public void batchRestoreObjects(final S3FileTransferRequestParamsDto params, BatchJobConfigDto batchJobConfig, int expirationInDays,
        String archiveRetrievalOption)
    {
        LOGGER.info("Batch restoring a list of objects in S3... s3KeyPrefix=\"{}\" s3BucketName=\"{}\" s3KeyCount={}", params.getS3KeyPrefix(),
            params.getS3BucketName(), params.getFiles().size());

        // Do nothing if no files to restore.
        if (CollectionUtils.isEmpty(params.getFiles()))
        {
            return;
        }

        // Create S3 Batch restore job, which supposed to be automatically executed by S3
        String jobId = createBatchRestoreJob(params, batchJobConfig, expirationInDays, archiveRetrievalOption);

        // Template class that simplifies the execution of operations with retry semantics executed by spring framework.
        RetryTemplate template = new RetryTemplate();

        // This policy determine how many repetitions this retry operation is going to do and which exceptions should be considered as repeatable.
        SimpleRetryPolicy policy = new SimpleRetryPolicy(batchJobConfig.getMaxAttempts(), Collections.singletonMap(S3BatchJobIncompleteException.class, true));
        template.setRetryPolicy(policy);

        // This policy is used to wait fixed amount of time before making another retry.
        FixedBackOffPolicy backoffPolicy = new FixedBackOffPolicy();

        // Reading backoff timeout value from configuration and assign to policy.
        backoffPolicy.setBackOffPeriod(batchJobConfig.getBackoffPeriod());
        template.setBackOffPolicy(backoffPolicy);

        // Turn off automatic re-throw of the exception after last repetition.
        template.setThrowLastExceptionOnExhausted(false);

        // Retry running provided lambda according to the retry and backoff policies assigned earlier.
        DescribeJobResult result = template.execute((RetryCallback<DescribeJobResult, S3BatchJobIncompleteException>) context -> {
            // Read current state of S3 Batch restore job.
            DescribeJobResult retryResult = getBatchJobDescription(params, batchJobConfig, jobId);

            // Read and check current status of the restore job.
            JobStatus jobStatus = JobStatus.fromValue(retryResult.getJob().getStatus());
            if (!FINAL_BATCH_PROCESSING_STATES.contains(jobStatus))
            {
                // If the job is still not finished (successfully or not) throw exception, which serves as a signal to make another retry
                throw new S3BatchJobIncompleteException(retryResult);
            }

            return retryResult;
        }, context -> {
            // If last retry finished with still incomplete status, extract the job descriptor and return it as a result
            if (context.getLastThrowable() instanceof S3BatchJobIncompleteException)
            {
                return ((S3BatchJobIncompleteException) context.getLastThrowable()).getJobDescriptor();
            }
            else
            {
                // If last describe job finished with different error re-throw it further
                throw new IllegalStateException(context.getLastThrowable());
            }
        });

        // Fail with exception if unable to retrieve descriptor of the batch job from AWS S3 after several retries
        if (result == null || result.getJob() == null || result.getJob().getStatus() == null)
        {
            throw new IllegalStateException("Unable to retrieve descriptor of the batch job");
        }

        // if after configured number of retries the job is still not complete - throw IllegalStateException
        JobStatus jobStatus = JobStatus.fromValue(result.getJob().getStatus());
        if (jobStatus != JobStatus.Complete)
        {
            throw new IllegalStateException(String.format("S3 batch job was not complete. Detailed descriptor: %s ", result));
        }

        // check if all tasks in the job completed successfully
        JobProgressSummary progressSummary = result.getJob().getProgressSummary();
        if (progressSummary.getNumberOfTasksFailed() > 0)
        {
            throw new IllegalStateException(
                String.format("S3 batch job was complete with errors. Job report includes detailed results for each failed task. " + "Detailed descriptor: %s ",
                    result));
        }
    }

    /**
     * Creates S3 batch job
     *
     * @param paramsDto              the S3 file transfer request parameters. The S3 bucket name and the file list identify the S3 objects to be restored every
     *                               object in the manifest.
     * @param batchJobConfig         the configuration parameters used to create batch job
     * @param expirationInDays       the time, in days, between when an object is restored to the bucket and when it expires
     * @param archiveRetrievalOption the archive retrieval option when restoring an archived object
     * @return S3 batch job id
     */
    String createBatchRestoreJob(final S3FileTransferRequestParamsDto paramsDto, BatchJobConfigDto batchJobConfig, int expirationInDays,
        String archiveRetrievalOption)
    {
        // All information regarding processing of this request going to be logged with this ID
        // and easily accessible in using Splunk smart field batchJobId
        String jobId = UUID.randomUUID().toString();
        LOGGER.info("Creating restore batch job... batchJobId=\"{}\", batchJobConfig={}", jobId, jsonHelper.objectToJson(batchJobConfig));

        AWSS3Control s3ControlClient = null;
        try
        {

            // Generating dto object to combine info related to S3 Batch operation manifest
            BatchJobManifestDto manifest = batchHelper.createCSVBucketKeyManifest(jobId, paramsDto.getS3BucketName(), paramsDto.getFiles(), batchJobConfig);
            LOGGER.info("Manifest created... batchJobId=\"{}\", manifestBucketName=\"{}\", manifestS3Key=\"{}\", manifestS3Etag=\"{}\"", jobId,
                manifest.getBucketName(), manifest.getKey(), manifest.getEtag());

            // Uploading manifest file to S3 before executing Batch Operation.
            // In this case manifest it CSV file with bucketName and file name S3 key to restore
            performTransfer(paramsDto, transferManager -> {
                // Create and prepare the metadata.
                ObjectMetadata metadata = new ObjectMetadata();
                prepareMetadata(paramsDto, metadata);

                // Create a put request with the parameters and the metadata.
                PutObjectRequest putObjectRequest = new PutObjectRequest(manifest.getBucketName(), manifest.getKey(),
                    new ByteArrayInputStream(manifest.getContent().getBytes(StandardCharsets.UTF_8)), metadata);

                // Upload file
                return s3Operations.upload(putObjectRequest, transferManager);
            });

            // Generate request to create S3 batch job to restore files
            CreateJobRequest createRestoreJobRequest =
                batchHelper.generateCreateRestoreJobRequest(manifest, jobId, expirationInDays, archiveRetrievalOption, batchJobConfig);

            LOGGER.info("Create restore job request generated... batchJobId=\"{}\", createRestoreJobRequest={}", jobId,
                jsonHelper.objectToJson(createRestoreJobRequest));


            // Create S3 control client which is going to execute actual call to s3
            s3ControlClient = awsS3ClientFactory.getAmazonS3Control(paramsDto);

            // Execute create job request and capture response from S3
            CreateJobResult createJobResult = s3Operations.createBatchJob(createRestoreJobRequest, s3ControlClient);

            LOGGER.info("Create job request executed... batchJobId=\"{}\", createJobResult={}", jobId, jsonHelper.objectToJson(createJobResult));

            return createJobResult.getJobId();
        }
        catch (Exception e)
        {
            throw new IllegalStateException(
                String.format("Failed to initiate a restore job... batchJobId=\"%s\", bucket=\"%s\"", jobId, paramsDto.getS3BucketName()), e);
        }
        finally
        {
            if (s3ControlClient != null)
            {
                s3ControlClient.shutdown();
            }
        }
    }

    /**
     * Creates S3 batch job to put the tag on listed S3 files versions (replacing existing ones)
     *
     * @param paramsDto          the S3 file transfer request parameters. The S3 bucket name and the file list identify the S3 objects to be restored every
     *                           object in the manifest.
     * @param batchJobConfig     the configuration parameters used to create batch job
     * @param s3VersionSummaries the list of S3 versions to be tagged
     * @param tag                the S3 object tag
     * @return S3 batch job id
     */
    String createBatchVersionsTaggingJob(final S3FileTransferRequestParamsDto paramsDto, BatchJobConfigDto batchJobConfig,
        final List<S3VersionSummary> s3VersionSummaries, final Tag tag)
    {
        // All information regarding processing of this request going to be logged with this ID
        // and easily accessible in using Splunk smart field batchJobId
        String jobId = UUID.randomUUID().toString();
        LOGGER.info("Creating tagging batch job... batchJobId=\"{}\", batchJobConfig={}", jobId, jsonHelper.objectToJson(batchJobConfig));

        AWSS3Control s3ControlClient = null;

        try
        {
            // Generating dto object to combine info related to S3 Batch operation manifest
            BatchJobManifestDto manifest =
                batchHelper.createCSVBucketKeyVersionManifest(jobId, paramsDto.getS3BucketName(), s3VersionSummaries, batchJobConfig);
            LOGGER.info("Manifest created... batchJobId=\"{}\", manifestBucketName=\"{}\", manifestS3Key=\"{}\", manifestS3Etag=\"{}\"", jobId,
                manifest.getBucketName(), manifest.getKey(), manifest.getEtag());

            // Uploading manifest file to S3 before executing Batch Operation.
            // In this case manifest is CSV file with bucketName, object key and object version id
            performTransfer(paramsDto, transferManager -> {
                // Create and prepare the metadata.
                ObjectMetadata metadata = new ObjectMetadata();
                prepareMetadata(paramsDto, metadata);

                // Create a put request with the parameters and the metadata.
                PutObjectRequest putObjectRequest = new PutObjectRequest(manifest.getBucketName(), manifest.getKey(),
                    new ByteArrayInputStream(manifest.getContent().getBytes(StandardCharsets.UTF_8)), metadata);

                // Upload file
                return s3Operations.upload(putObjectRequest, transferManager);
            });

            // Generate request to create S3 batch job
            CreateJobRequest createJobRequest = batchHelper.generateCreatePutObjectTaggingJobRequest(manifest, jobId, batchJobConfig, tag);

            LOGGER.info("Create tagging job request generated... batchJobId=\"{}\", createJobRequest={}", jobId, jsonHelper.objectToJson(createJobRequest));

            // Create S3 control client which is going to execute actual call to s3
            s3ControlClient = awsS3ClientFactory.getAmazonS3Control(paramsDto);

            // Execute create job request and capture response from S3
            CreateJobResult createJobResult = s3Operations.createBatchJob(createJobRequest, s3ControlClient);

            LOGGER.info("Create job request executed... batchJobId=\"{}\", createJobResult={}", jobId, jsonHelper.objectToJson(createJobResult));

            return createJobResult.getJobId();
        }
        catch (Exception e)
        {
            throw new IllegalStateException(
                String.format("Failed to initiate a tagging job... batchJobId=\"%s\", bucket=\"%s\"", jobId, paramsDto.getS3BucketName()), e);
        }
        finally
        {
            if (s3ControlClient != null)
            {
                s3ControlClient.shutdown();
            }
        }
    }

    /****
     * Get S3 batch job configuration and status information
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters
     * @param batchJobConfig the configuration parameters used to create batch job
     * @param jobId The ID for the S3 batch job
     *
     * @return A container element for the job configuration and status information
     */
    DescribeJobResult getBatchJobDescription(final S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto, BatchJobConfigDto batchJobConfig,
        String jobId)
    {
        // Generate request to get S3 batch job current descriptor
        DescribeJobRequest describeJobRequest = batchHelper.generateDescribeJobRequest(jobId, batchJobConfig);
        LOGGER.info("Describe job request generated... batchJobId=\"{}\", describeJobRequest={}", jobId, jsonHelper.objectToJson(describeJobRequest));

        // Create S3 control client which is going to execute actual call to s3
        AWSS3Control s3ControlClient = awsS3ClientFactory.getAmazonS3Control(s3FileTransferRequestParamsDto);
        try
        {
            // Execute describe job request and capture response from S3
            DescribeJobResult response = s3Operations.describeBatchJob(describeJobRequest, s3ControlClient);
            LOGGER.info("DescribeBatchJob call complete... batchJobId=\"{}\", response={}", jobId, jsonHelper.objectToJson(response));
            return response;
        }
        finally
        {
            s3ControlClient.shutdown();
        }
    }

    /**
     * Returns true is S3 key prefix is a root.
     *
     * @param s3KeyPrefix the S3 key prefix to be validated
     * @return true if S3 key prefix is a root; false otherwise
     */
    protected boolean isRootKeyPrefix(String s3KeyPrefix)
    {
        return StringUtils.isBlank(s3KeyPrefix) || s3KeyPrefix.equals("/");
    }

    /**
     * Creates an S3 object of 0 byte size that represents a directory.
     *
     * @param params           the S3 file transfer request parameters. The S3 bucket name and S3 key prefix identify the S3 object to be created.
     * @param isEmptyDirectory a boolean flag that will determine if we are creating an empty directory.
     */
    private void createDirectory(final S3FileTransferRequestParamsDto params, final boolean isEmptyDirectory)
    {
        // Create metadata for the directory marker and set content-length to 0 bytes.
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        prepareMetadata(params, metadata);

        // Create empty content.
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

        // Create a PutObjectRequest passing the folder name suffixed by '/'.
        String suffix = isEmptyDirectory ? "" : "/";
        String directoryName = StringUtils.appendIfMissing(params.getS3KeyPrefix(), suffix);
        PutObjectRequest putObjectRequest = new PutObjectRequest(params.getS3BucketName(), directoryName, emptyContent, metadata);
        // KMS key ID is being set through prepareMetadata()

        AmazonS3Client s3Client = awsS3ClientFactory.getAmazonS3Client(params);

        try
        {
            s3Operations.putObject(putObjectRequest, s3Client);
        }
        catch (AmazonServiceException e)
        {
            throw new IllegalStateException(
                String.format("Failed to create 0 byte S3 object with \"%s\" key in bucket \"%s\". Reason: %s", directoryName, params.getS3BucketName(),
                    e.getMessage()), e);
        }
        finally
        {
            // Shutdown the AmazonS3Client instance to release resources.
            s3Client.shutdown();
        }
    }

    /**
     * Deletes a list of keys/key versions from the specified S3 bucket.
     *
     * @param s3Client     the S3 client
     * @param s3BucketName the S3 bucket name
     * @param keyVersions  the list of S3 keys/key versions
     */
    private void deleteKeyVersions(AmazonS3Client s3Client, String s3BucketName, List<DeleteObjectsRequest.KeyVersion> keyVersions)
    {
        // Create a request to delete multiple objects in the specified bucket.
        DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(s3BucketName);

        // The Multi-Object Delete request can contain a list of up to 1000 keys.
        for (int i = 0; i < keyVersions.size() / MAX_KEYS_PER_DELETE_REQUEST + 1; i++)
        {
            List<DeleteObjectsRequest.KeyVersion> keysSubList =
                keyVersions.subList(i * MAX_KEYS_PER_DELETE_REQUEST, Math.min(keyVersions.size(), (i + 1) * MAX_KEYS_PER_DELETE_REQUEST));
            multiObjectDeleteRequest.setKeys(keysSubList);
            try
            {
                s3Operations.deleteObjects(multiObjectDeleteRequest, s3Client);
            }
            catch (MultiObjectDeleteException multiObjectDeleteException)
            {
                logMultiObjectDeleteException(multiObjectDeleteException);
                throw multiObjectDeleteException;
            }

            LOGGER.info("Successfully requested the deletion of the listed below keys/key versions from the S3 bucket. s3KeyCount={} s3BucketName=\"{}\"",
                keysSubList.size(), s3BucketName);

            for (DeleteObjectsRequest.KeyVersion keyVersion : keysSubList)
            {
                LOGGER.info("s3Key=\"{}\" s3VersionId=\"{}\"", keyVersion.getKey(), keyVersion.getVersion());
            }
        }
    }

    /**
     * Retrieves an S3 object.
     *
     * @param s3Client         the S3 client
     * @param bucketName       the S3 bucket name
     * @param key              the S3 object key
     * @param errorOnNoSuchKey true to throw an error when the object key is not found, otherwise return null
     * @return the S3 object
     * @throws ObjectNotFoundException when specified bucket or key does not exist or access to bucket or key is denied
     */
    private S3Object getS3Object(AmazonS3Client s3Client, String bucketName, String key, boolean errorOnNoSuchKey)
    {
        try
        {
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
            return s3Operations.getS3Object(getObjectRequest, s3Client);
        }
        catch (AmazonServiceException amazonServiceException)
        {
            String errorCode = amazonServiceException.getErrorCode();

            if (S3Operations.ERROR_CODE_ACCESS_DENIED.equals(errorCode))
            {
                throw new ObjectNotFoundException(
                    "Application does not have access to the specified S3 object at bucket '" + bucketName + "' and key '" + key + "'.",
                    amazonServiceException);
            }
            else if (S3Operations.ERROR_CODE_NO_SUCH_BUCKET.equals(errorCode))
            {
                throw new ObjectNotFoundException("Specified S3 bucket '" + bucketName + "' does not exist.", amazonServiceException);
            }
            else if (S3Operations.ERROR_CODE_NO_SUCH_KEY.equals(errorCode))
            {
                if (errorOnNoSuchKey)
                {
                    throw new ObjectNotFoundException("Specified S3 object key '" + key + "' does not exist.", amazonServiceException);
                }
                else
                {
                    return null;
                }
            }
            else
            {
                throw amazonServiceException;
            }
        }
    }


    /**
     * Logs the given MultiObjectDeleteException.
     *
     * @param multiObjectDeleteException The exception to log
     */
    private void logMultiObjectDeleteException(MultiObjectDeleteException multiObjectDeleteException)
    {
        // Create and initialize a string buffer. The initialization is required here in order to avoid an InsufficientStringBufferDeclaration PMD violation.
        StringBuilder builder = new StringBuilder(128);
        builder.append(String.format("Error deleting multiple objects. Below are the list of objects which failed to delete.%n"));
        List<DeleteError> deleteErrors = multiObjectDeleteException.getErrors();
        for (DeleteError deleteError : deleteErrors)
        {
            String key = deleteError.getKey();
            String versionId = deleteError.getVersionId();
            String code = deleteError.getCode();
            String message = deleteError.getMessage();
            builder.append(
                String.format("s3Key=\"%s\" s3VersionId=\"%s\" s3DeleteErrorCode=\"%s\" s3DeleteErrorMessage=\"%s\"%n", key, versionId, code, message));
        }
        LOGGER.error(builder.toString());
    }

    /**
     * Logs overall transfer rate for an S3 file transfer operation.
     *
     * @param s3FileTransferResultsDto the DTO for the S3 file transfer operation results
     */
    private void logOverallTransferRate(S3FileTransferResultsDto s3FileTransferResultsDto)
    {
        if (LOGGER.isInfoEnabled())
        {
            NumberFormat formatter = new DecimalFormat("#0.00");

            LOGGER.info("overallTransferRateKiloBytesPerSecond={} overallTransferRateMegaBitsPerSecond={}", formatter.format(
                awsHelper.getTransferRateInKilobytesPerSecond(s3FileTransferResultsDto.getTotalBytesTransferred(),
                    s3FileTransferResultsDto.getDurationMillis())), formatter.format(
                awsHelper.getTransferRateInMegabitsPerSecond(s3FileTransferResultsDto.getTotalBytesTransferred(),
                    s3FileTransferResultsDto.getDurationMillis())));
        }
    }

    /**
     * Logs transfer progress for an S3 file transfer operation.
     *
     * @param transferProgress the progress of an S3 transfer operation
     */
    private void logTransferProgress(TransferProgress transferProgress)
    {
        // If the total bytes to transfer is set to 0, we do not log the transfer progress.
        if (LOGGER.isInfoEnabled() && transferProgress.getTotalBytesToTransfer() > 0)
        {
            NumberFormat formatter = new DecimalFormat("#0.0");

            LOGGER.info("progressBytesTransferred={} totalBytesToTransfer={} progressPercentTransferred={}", transferProgress.getBytesTransferred(),
                transferProgress.getTotalBytesToTransfer(), formatter.format(transferProgress.getPercentTransferred()));
        }
    }

    /**
     * Performs a file/directory transfer.
     *
     * @param params     the parameters.
     * @param transferer a transferer that knows how to perform the transfer.
     * @return the results.
     * @throws InterruptedException if a problem is encountered.
     */
    private S3FileTransferResultsDto performTransfer(final S3FileTransferRequestParamsDto params, Transferer transferer) throws InterruptedException
    {
        // Create a transfer manager.
        TransferManager transferManager = awsS3ClientFactory.getTransferManager(params);

        try
        {
            // Start a stop watch to keep track of how long the transfer takes.
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            // Perform the transfer.
            Transfer transfer = transferer.performTransfer(transferManager);
            TransferProgress transferProgress = transfer.getProgress();

            logTransferProgress(transferProgress);

            long stepCount = 0;

            // Loop until the transfer is complete.
            do
            {
                Thread.sleep(sleepIntervalsMillis);
                stepCount++;

                // Log progress status every 30 seconds and when transfer is complete.
                if (transfer.isDone() || stepCount % 300 == 0)
                {
                    logTransferProgress(transferProgress);
                }
            }
            while (!transfer.isDone());

            // Stop the stop watch and create a results object.
            stopWatch.stop();

            // If the transfer failed, throw the underlying AWS exception if we can determine one. Otherwise, throw our own exception.
            TransferState transferState = transfer.getState();
            if (transferState == TransferState.Failed)
            {
                // The waitForException method should return the underlying AWS exception since the state is "Failed". It should not block since the
                // transfer is already "done" per previous code checking "isDone".
                AmazonClientException amazonClientException = transfer.waitForException();

                // If the returned exception is null, we weren't able to get the underlying AWS exception so just throw our own exception.
                // This is unlikely since the transfer failed, but it's better to handle the possibility just in case.
                if (amazonClientException == null)
                {
                    throw new IllegalStateException("The transfer operation \"" + transfer.getDescription() + "\" failed for an unknown reason.");
                }

                // Throw the Amazon underlying exception.
                throw amazonClientException;
            }
            // Ensure the transfer completed. If not, throw an exception.
            else if (transferState != TransferState.Completed)
            {
                throw new IllegalStateException(
                    "The transfer operation \"" + transfer.getDescription() + "\" did not complete successfully. Current state: \"" + transferState + "\".");
            }

            // TransferProgress.getBytesTransferred() are not populated for S3 Copy objects.
            if (!(transfer instanceof Copy))
            {
                // Sanity check for the number of bytes transferred.
                Assert.isTrue(transferProgress.getBytesTransferred() >= transferProgress.getTotalBytesToTransfer(),
                    String.format("Actual number of bytes transferred is less than expected (actual: %d bytes; expected: %d bytes).",
                        transferProgress.getBytesTransferred(), transferProgress.getTotalBytesToTransfer()));
            }

            // Create the results object and populate it with the standard data.
            S3FileTransferResultsDto results = new S3FileTransferResultsDto();
            results.setDurationMillis(stopWatch.getTime());
            results.setTotalBytesTransferred(transfer.getProgress().getBytesTransferred());
            results.setTotalFilesTransferred(1L);

            if (transfer instanceof MultipleFileUpload)
            {
                // For upload directory, we need to calculate the total number of files transferred differently.
                results.setTotalFilesTransferred((long) ((MultipleFileUpload) transfer).getSubTransfers().size());
            }
            else if (transfer instanceof MultipleFileDownload)
            {
                // For download directory, we need to calculate the total number of files differently.
                results.setTotalFilesTransferred((long) listDirectory(params).size());
            }

            // Return the results.
            return results;
        }
        finally
        {
            // Shutdown the transfer manager to release resources. If this isn't done, the JVM may delay upon exiting.
            transferManager.shutdownNow();
        }
    }

    /**
     * Prepares the object metadata for server side encryption and reduced redundancy storage.
     *
     * @param params   the parameters.
     * @param metadata the metadata to prepare.
     */
    private void prepareMetadata(final S3FileTransferRequestParamsDto params, ObjectMetadata metadata)
    {
        // Set the server side encryption
        if (params.getKmsKeyId() != null)
        {
            /*
             * TODO Use proper way to set KMS once AWS provides a way.
             * We are modifying the raw headers directly since TransferManager's uploadFileList operation does not provide a way to set a KMS key ID.
             * This would normally cause some issues when uploading where an MD5 checksum validation exception will be thrown, even though the object is
             * correctly uploaded.
             * To get around this, a system property defined at
             * com.amazonaws.services.s3.internal.SkipMd5CheckStrategy.DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY must be set.
             */
            metadata.setSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm());
            metadata.setHeader(Headers.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, params.getKmsKeyId().trim());
        }
        else
        {
            metadata.setSSEAlgorithm(SSEAlgorithm.AES256.getAlgorithm());
        }

        // If specified, set the metadata to use RRS.
        if (Boolean.TRUE.equals(params.isUseRrs()))
        {
            // TODO: For upload File, we can set RRS on the putObjectRequest. For uploadDirectory, this is the only
            // way to do it. However, setHeader() is flagged as For Internal Use Only
            metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.ReducedRedundancy.toString());
        }
    }

    /**
     * Tags S3 versions with the specified S3 object tag.
     *
     * @param s3FileTransferRequestParamsDto the S3 file transfer request parameters. This set of parameters contains the S3 bucket name
     * @param s3ObjectTaggerRoleParamsDto    the S3 objects tagger role parameters DTO
     * @param s3VersionSummaries             the list of S3 versions to be tagged
     * @param tag                            the S3 object tag
     */
    private void tagVersionsHelper(final S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto,
        final S3ObjectTaggerRoleParamsDto s3ObjectTaggerRoleParamsDto, final List<S3VersionSummary> s3VersionSummaries, final Tag tag)
    {
        // Initialize an S3 version for the error message in the catch block.
        S3VersionSummary currentS3VersionSummary = s3VersionSummaries.get(0);

        // Amazon S3 client to access S3 objects.
        AmazonS3Client s3Client = null;

        // Amazon STS client to assume S3 tagger role.
        AWSSecurityTokenService securityTokenService = null;

        // Amazon S3 client for S3 object tagging.
        AmazonS3Client s3ObjectTaggerClient = null;

        try
        {
            // Log tagger role parameters.
            LOGGER.info("Getting AWS temporary security credentials... sessionName={}, roleArn={}, awsRoleDurationSeconds={}",
                s3ObjectTaggerRoleParamsDto.getS3ObjectTaggerRoleSessionName(), s3ObjectTaggerRoleParamsDto.getS3ObjectTaggerRoleArn(),
                s3ObjectTaggerRoleParamsDto.getS3ObjectTaggerRoleSessionDurationSeconds());

            // Create an S3 client to access S3 objects.
            s3Client = awsS3ClientFactory.getAmazonS3Client(s3FileTransferRequestParamsDto);

            // Create an STS client.
            securityTokenService =
                AWSSecurityTokenServiceClientBuilder.standard().withClientConfiguration(awsHelper.getClientConfiguration(s3FileTransferRequestParamsDto))
                    .withRegion(s3FileTransferRequestParamsDto.getAwsRegionName()).build();

            // Create credentials provider for S3 object tagging operation.
            STSAssumeRoleSessionCredentialsProvider credentialsProvider =
                new STSAssumeRoleSessionCredentialsProvider.Builder(s3ObjectTaggerRoleParamsDto.getS3ObjectTaggerRoleArn(),
                    s3ObjectTaggerRoleParamsDto.getS3ObjectTaggerRoleSessionName()).withRoleSessionDurationSeconds(
                    s3ObjectTaggerRoleParamsDto.getS3ObjectTaggerRoleSessionDurationSeconds()).withStsClient(securityTokenService).build();

            // Create an S3 client for S3 object tagging.
            s3ObjectTaggerClient = awsS3ClientFactory.getAmazonS3Client(s3FileTransferRequestParamsDto, credentialsProvider);

            // Create a get object tagging request.
            GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(s3FileTransferRequestParamsDto.getS3BucketName(), null, null);

            // Create a set object tagging request.
            SetObjectTaggingRequest setObjectTaggingRequest = new SetObjectTaggingRequest(s3FileTransferRequestParamsDto.getS3BucketName(), null, null, null);

            for (S3VersionSummary s3VersionSummary : s3VersionSummaries)
            {
                // Set the current S3 version summary.
                currentS3VersionSummary = s3VersionSummary;

                // Retrieve the current tagging information for the S3 version.
                getObjectTaggingRequest.setKey(s3VersionSummary.getKey());
                getObjectTaggingRequest.setVersionId(s3VersionSummary.getVersionId());
                GetObjectTaggingResult getObjectTaggingResult = s3Operations.getObjectTagging(getObjectTaggingRequest, s3Client);

                // Update the list of tags to include the specified S3 object tag.
                List<Tag> updatedTags = new ArrayList<>();
                updatedTags.add(tag);
                if (CollectionUtils.isNotEmpty(getObjectTaggingResult.getTagSet()))
                {
                    for (Tag currentTag : getObjectTaggingResult.getTagSet())
                    {
                        if (!StringUtils.equals(tag.getKey(), currentTag.getKey()))
                        {
                            updatedTags.add(currentTag);
                        }
                    }
                }

                // Update tagging information for the S3 version.
                setObjectTaggingRequest.setKey(s3VersionSummary.getKey());
                setObjectTaggingRequest.setVersionId(s3VersionSummary.getVersionId());
                setObjectTaggingRequest.setTagging(new ObjectTagging(updatedTags));
                s3Operations.setObjectTagging(setObjectTaggingRequest, s3ObjectTaggerClient);
            }
        }
        catch (Exception e)
        {
            throw new IllegalStateException(
                String.format("Failed to tag S3 object with \"%s\" key and \"%s\" version id in \"%s\" bucket. Reason: %s", currentS3VersionSummary.getKey(),
                    currentS3VersionSummary.getVersionId(), s3FileTransferRequestParamsDto.getS3BucketName(), e.getMessage()), e);
        }
        finally
        {
            if (s3Client != null)
            {
                s3Client.shutdown();
            }

            if (securityTokenService != null)
            {
                securityTokenService.shutdown();
            }

            if (s3ObjectTaggerClient != null)
            {
                s3ObjectTaggerClient.shutdown();
            }
        }
    }

    /**
     * An object that can perform a transfer using a transform manager.
     */
    private interface Transferer
    {
        /**
         * Perform a transfer using the specified transfer manager.
         *
         * @param transferManager the transfer manager.
         * @return the transfer information for the transfer. This will typically be returned from an operation on the transfer manager (e.g. upload).
         */
        Transfer performTransfer(TransferManager transferManager);
    }
}
