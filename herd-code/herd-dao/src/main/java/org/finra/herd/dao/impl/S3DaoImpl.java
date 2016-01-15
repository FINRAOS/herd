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
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.S3Operations;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JavaPropertiesHelper;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.HerdAWSCredentialsProvider;
import org.finra.herd.model.dto.S3FileCopyRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferResultsDto;

/**
 * The S3 DAO implementation.
 */
@Repository
public class S3DaoImpl implements S3Dao
{
    /**
     * A {@link AWSCredentialsProvider} which delegates to its wrapped {@link HerdAWSCredentialsProvider}
     */
    private static class HerdAwsCredentialsProviderWrapper implements AWSCredentialsProvider
    {
        private HerdAWSCredentialsProvider herdAWSCredentialsProvider;

        public HerdAwsCredentialsProviderWrapper(HerdAWSCredentialsProvider herdAWSCredentialsProvider)
        {
            this.herdAWSCredentialsProvider = herdAWSCredentialsProvider;
        }

        @Override
        public void refresh()
        {
            // No need to implement this. AWS doesn't use this.
        }

        @Override
        public AWSCredentials getCredentials()
        {
            AwsCredential herdAwsCredential = herdAWSCredentialsProvider.getAwsCredential();
            return new BasicSessionCredentials(herdAwsCredential.getAwsAccessKey(), herdAwsCredential.getAwsSecretKey(),
                herdAwsCredential.getAwsSessionToken());
        }
    }

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private S3Operations s3Operations;

    private static final Logger LOGGER = Logger.getLogger(S3DaoImpl.class);

    private static final int MAX_KEYS_PER_DELETE_REQUEST = 1000;

    private static final long SLEEP_INTERVAL_MILLIS = 100;

    @Autowired
    private JavaPropertiesHelper javaPropertiesHelper;

    @Override
    public ObjectMetadata getObjectMetadata(final S3FileTransferRequestParamsDto params)
    {
        AmazonS3Client s3Client = null;

        try
        {
            s3Client = getAmazonS3(params);

            return s3Operations.getObjectMetadata(params.getS3BucketName(), params.getS3KeyPrefix(), s3Client);
        }
        catch (AmazonServiceException e)
        {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND)
            {
                return null;
            }

            throw new IllegalStateException(String
                .format("Failed to get S3 metadata for object key \"%s\" from bucket \"%s\". Reason: %s", params.getS3KeyPrefix(), params.getS3BucketName(),
                    e.getMessage()), e);
        }
        finally
        {
            // Shutdown the AmazonS3Client instance to release resources.
            if (s3Client != null)
            {
                s3Client.shutdown();
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

        Assert.isTrue(fileSizeInBytes == null || fileSizeInBytes.compareTo(objectMetadata.getContentLength()) == 0, String
            .format("Specified file size (%d bytes) does not match to the actual file size (%d bytes) reported by S3 for s3://%s/%s file.", fileSizeInBytes,
                objectMetadata.getContentLength(), params.getS3BucketName(), params.getS3KeyPrefix()));
    }

    @Override
    public void createDirectory(final S3FileTransferRequestParamsDto params)
    {
        // Create metadata for the directory marker and set content-length to 0 bytes.
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        prepareMetadata(params, metadata);

        // Create empty content.
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

        // Create a PutObjectRequest passing the folder name suffixed by '/'.
        String directoryName = params.getS3KeyPrefix() + (params.getS3KeyPrefix().endsWith("/") ? "" : "/");
        PutObjectRequest putObjectRequest = new PutObjectRequest(params.getS3BucketName(), directoryName, emptyContent, metadata);
        // KMS key ID is being set through prepareMetadata()

        AmazonS3Client s3Client = null;

        try
        {
            s3Client = getAmazonS3(params);
            s3Operations.putObject(putObjectRequest, s3Client);
        }
        catch (AmazonServiceException e)
        {
            throw new IllegalStateException(String
                .format("Failed to create 0 byte S3 object with \"%s\" key in bucket \"%s\". Reason: %s", directoryName, params.getS3BucketName(),
                    e.getMessage()), e);
        }
        finally
        {
            // Shutdown the AmazonS3Client instance to release resources.
            if (s3Client != null)
            {
                s3Client.shutdown();
            }
        }
    }

    @Override
    public List<StorageFile> listDirectory(final S3FileTransferRequestParamsDto params)
    {
        // By default, we do not ignore 0 byte objects that represent S3 directories.
        return listDirectory(params, false);
    }

    @Override
    public List<StorageFile> listDirectory(final S3FileTransferRequestParamsDto params, boolean ignoreZeroByteDirectoryMarkers)
    {
        return listObjectsMatchingKeyPrefix(params, ignoreZeroByteDirectoryMarkers);
    }

    @Override
    public S3FileTransferResultsDto uploadFile(final S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        LOGGER.info(String.format("Uploading %s local file to s3://%s/%s ...", params.getLocalPath(), params.getS3BucketName(), params.getS3KeyPrefix()));

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

        LOGGER.info("Local file \"" + params.getLocalPath() + "\" contains " + results.getTotalBytesTransferred() +
            " byte(s) which was successfully transferred to S3 key prefix \"" + params.getS3KeyPrefix() + "\" in bucket \"" +
            params.getS3BucketName() + "\" in " + HerdDateUtils.formatDuration(results.getDurationMillis(), true));

        LOGGER.info(String.format("Overall transfer rate: %.2f kBytes/s (%.2f Mbits/s)",
            awsHelper.getTransferRateInKilobytesPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis()),
            awsHelper.getTransferRateInMegabitsPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis())));

        return results;
    }

    @Override
    public S3FileTransferResultsDto uploadFileList(final S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        LOGGER.info(String
            .format("Uploading %d files from %s local directory to s3://%s/%s ...", params.getFiles().size(), params.getLocalPath(), params.getS3BucketName(),
                params.getS3KeyPrefix()));

        if (LOGGER.isInfoEnabled())
        {
            for (File file : params.getFiles())
            {
                LOGGER.info(String.format("    %s", file.getPath()));
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

        LOGGER.info(
            "List of files relative to the common local parent directory \"" + params.getLocalPath() + "\" contains " + results.getTotalFilesTransferred() +
                " file(s) and " +
                results.getTotalBytesTransferred() + " byte(s) which was successfully transferred to S3 key prefix \"" + params.getS3KeyPrefix() +
                "\" in bucket \"" +
                params.getS3BucketName() + "\" in " + HerdDateUtils.formatDuration(results.getDurationMillis(), true));

        LOGGER.info(String.format("Overall transfer rate: %.2f kBytes/s (%.2f Mbits/s)",
            awsHelper.getTransferRateInKilobytesPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis()),
            awsHelper.getTransferRateInMegabitsPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis())));

        return results;
    }

    @Override
    public S3FileTransferResultsDto copyFile(final S3FileCopyRequestParamsDto params) throws InterruptedException
    {
        LOGGER.info(String
            .format("Copying S3 object from s3://%s/%s to s3://%s/%s...", params.getSourceBucketName(), params.getS3KeyPrefix(), params.getTargetBucketName(),
                params.getS3KeyPrefix()));

        // Perform the copy.
        S3FileTransferResultsDto results = performTransfer(params, new Transferer()
        {
            @Override
            public Transfer performTransfer(TransferManager transferManager)
            {
                // Create a copy request.
                CopyObjectRequest copyObjectRequest =
                    new CopyObjectRequest(params.getSourceBucketName(), params.getS3KeyPrefix(), params.getTargetBucketName(), params.getS3KeyPrefix());
                copyObjectRequest.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(params.getKmsKeyId()));

                return s3Operations.copyFile(copyObjectRequest, transferManager);
            }
        });

        LOGGER.info("File \"" + params.getS3KeyPrefix() + "\" contains " + results.getTotalBytesTransferred() +
            " byte(s) which was successfully copied from source bucket:\"" + params.getSourceBucketName() + "\" to target bucket:\"" +
            params.getTargetBucketName() + "\" in " + HerdDateUtils.formatDuration(results.getDurationMillis(), true));

        LOGGER.info(String.format("Overall transfer rate: %.2f kBytes/s (%.2f Mbits/s)",
            awsHelper.getTransferRateInKilobytesPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis()),
            awsHelper.getTransferRateInMegabitsPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis())));

        return results;
    }

    @Override
    public S3FileTransferResultsDto uploadDirectory(final S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        LOGGER.info(String.format("Uploading %s local directory to s3://%s/%s ...", params.getLocalPath(), params.getS3BucketName(), params.getS3KeyPrefix()));

        // Perform the transfer.
        S3FileTransferResultsDto results = performTransfer(params, new Transferer()
        {
            @Override
            public Transfer performTransfer(TransferManager transferManager)
            {
                return s3Operations.uploadDirectory(params.getS3BucketName(), params.getS3KeyPrefix(), new File(params.getLocalPath()), params.getRecursive(),
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

        LOGGER.info("Local directory \"" + params.getLocalPath() + "\" contains " + results.getTotalFilesTransferred() + " file(s) and " +
            results.getTotalBytesTransferred() + " byte(s) which was successfully transferred to S3 key prefix \"" + params.getS3KeyPrefix() +
            "\" in bucket \"" +
            params.getS3BucketName() + "\" in " + HerdDateUtils.formatDuration(results.getDurationMillis(), true));

        LOGGER.info(String.format("Overall transfer rate: %.2f kBytes/s (%.2f Mbits/s)",
            awsHelper.getTransferRateInKilobytesPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis()),
            awsHelper.getTransferRateInMegabitsPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis())));

        return results;
    }

    @Override
    public void deleteFile(final S3FileTransferRequestParamsDto params)
    {
        AmazonS3Client s3Client = getAmazonS3(params);

        s3Operations.deleteFile(params.getS3BucketName(), params.getS3KeyPrefix(), s3Client);
    }

    @Override
    public void deleteFileList(final S3FileTransferRequestParamsDto params)
    {
        AmazonS3Client s3Client = null;

        LOGGER.info(String.format("Deleting %d keys/objects from s3://%s ...", params.getFiles().size(), params.getS3BucketName()));

        try
        {
            // In order to avoid a MalformedXML AWS exception, we send delete request only when we have any keys to delete.
            if (!params.getFiles().isEmpty())
            {
                // Build a list of keys to be deleted.
                List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
                for (File file : params.getFiles())
                {
                    keys.add(new DeleteObjectsRequest.KeyVersion(file.getPath().replaceAll("\\\\", "/")));
                }

                DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(params.getS3BucketName());
                s3Client = getAmazonS3(params);

                // The Multi-Object Delete request can contain a list of up to 1000 keys.
                for (int i = 0; i < keys.size() / MAX_KEYS_PER_DELETE_REQUEST + 1; i++)
                {
                    List<DeleteObjectsRequest.KeyVersion> keysSubList =
                        keys.subList(i * MAX_KEYS_PER_DELETE_REQUEST, Math.min(keys.size(), (i + 1) * MAX_KEYS_PER_DELETE_REQUEST));
                    multiObjectDeleteRequest.setKeys(keysSubList);
                    s3Operations.deleteObjects(multiObjectDeleteRequest, s3Client);

                    LOGGER.info(String.format("Successfully requested the deletion of the following %d keys/objects from bucket \"%s\":", keysSubList.size(),
                        params.getS3BucketName()));

                    for (DeleteObjectsRequest.KeyVersion keyVersion : keysSubList)
                    {
                        LOGGER.info(String.format("    s3://%s/%s", params.getS3BucketName(), keyVersion.getKey()));
                    }
                }
            }
        }
        catch (Exception e)
        {
            throw new IllegalStateException(
                String.format("Failed to delete a list of keys/objects from bucket \"%s\". Reason: %s", params.getS3BucketName(), e.getMessage()), e);
        }
        finally
        {
            // Shutdown the AmazonS3Client instance to release resources.
            if (s3Client != null)
            {
                s3Client.shutdown();
            }
        }
    }

    @Override
    public void deleteDirectory(final S3FileTransferRequestParamsDto params)
    {
        AmazonS3Client s3Client = null;

        LOGGER.info(String.format("Deleting keys/objects from s3://%s/%s ...", params.getS3BucketName(), params.getS3KeyPrefix()));

        Assert.hasText(params.getS3KeyPrefix(), "Deleting from root directory is not allowed.");

        try
        {
            // List S3 object including any 0 byte objects that represent S3 directories.
            List<StorageFile> storageFiles = listObjectsMatchingKeyPrefix(params, false);
            LOGGER.info(String.format("Found %d keys/objects in s3://%s/%s ...", storageFiles.size(), params.getS3BucketName(), params.getS3KeyPrefix()));

            // In order to avoid a MalformedXML AWS exception, we send delete request only when we have any keys to delete.
            if (!storageFiles.isEmpty())
            {
                DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(params.getS3BucketName());
                s3Client = getAmazonS3(params);

                // The Multi-Object Delete request can contain a list of up to 1000 keys.
                for (int i = 0; i < storageFiles.size() / MAX_KEYS_PER_DELETE_REQUEST + 1; i++)
                {
                    // Prepare a list of S3 object keys to be deleted.
                    List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
                    for (StorageFile storageFile : storageFiles
                        .subList(i * MAX_KEYS_PER_DELETE_REQUEST, Math.min(storageFiles.size(), (i + 1) * MAX_KEYS_PER_DELETE_REQUEST)))
                    {
                        keys.add(new DeleteObjectsRequest.KeyVersion(storageFile.getFilePath()));
                    }

                    // Delete the S3 objects.
                    multiObjectDeleteRequest.setKeys(keys);
                    s3Operations.deleteObjects(multiObjectDeleteRequest, s3Client);

                    LOGGER.info(String.format("Successfully deleted the following %d keys/objects with prefix \"%s\" from bucket \"%s\":", keys.size(),
                        params.getS3KeyPrefix(), params.getS3BucketName()));

                    for (DeleteObjectsRequest.KeyVersion keyVersion : keys)
                    {
                        LOGGER.info(String.format("    s3://%s/%s", params.getS3BucketName(), keyVersion.getKey()));
                    }
                }
            }
        }
        catch (AmazonClientException e)
        {
            throw new IllegalStateException(String
                .format("Failed to delete keys/objects with prefix \"%s\" from bucket \"%s\". Reason: %s", params.getS3KeyPrefix(), params.getS3BucketName(),
                    e.getMessage()), e);
        }
        finally
        {
            // Shutdown the AmazonS3Client instance to release resources.
            if (s3Client != null)
            {
                s3Client.shutdown();
            }
        }
    }

    @Override
    public S3FileTransferResultsDto downloadFile(final S3FileTransferRequestParamsDto params) throws InterruptedException
    {
        // Perform the transfer.
        S3FileTransferResultsDto results = performTransfer(params, new Transferer()
        {
            @Override
            public Transfer performTransfer(TransferManager transferManager)
            {
                return s3Operations.download(params.getS3BucketName(), params.getS3KeyPrefix(), new File(params.getLocalPath()), transferManager);
            }
        });

        LOGGER
            .info("S3 file \"" + params.getS3KeyPrefix() + "\" in bucket \"" + params.getS3BucketName() + "\" contains " + results.getTotalBytesTransferred() +
                " byte(s) which was successfully transferred to local file \"" + params.getLocalPath() + "\" in " +
                HerdDateUtils.formatDuration(results.getDurationMillis(), true));

        LOGGER.info(String.format("Overall transfer rate: %.2f kBytes/s (%.2f Mbits/s)",
            awsHelper.getTransferRateInKilobytesPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis()),
            awsHelper.getTransferRateInMegabitsPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis())));

        return results;
    }

    @Override
    public S3FileTransferResultsDto downloadDirectory(final S3FileTransferRequestParamsDto params) throws InterruptedException
    {
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

        LOGGER.info(
            "S3 directory \"" + params.getS3KeyPrefix() + "\" in bucket \"" + params.getS3BucketName() + "\" contains " + results.getTotalBytesTransferred() +
                " byte(s) which was successfully transferred to local directory \"" + params.getLocalPath() + "\" in " +
                HerdDateUtils.formatDuration(results.getDurationMillis(), true));

        LOGGER.info(String.format("Overall transfer rate: %.2f kBytes/s (%.2f Mbits/s)",
            awsHelper.getTransferRateInKilobytesPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis()),
            awsHelper.getTransferRateInMegabitsPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis())));

        return results;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int abortMultipartUploads(S3FileTransferRequestParamsDto params, Date thresholdDate)
    {
        AmazonS3Client s3Client = null;
        int abortedMultipartUploadsCount = 0;

        try
        {
            // Create an Amazon S3 client.
            s3Client = getAmazonS3(params);

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
                        s3Operations.abortMultipartUpload(TransferManager
                            .appendSingleObjectUserAgent(new AbortMultipartUploadRequest(params.getS3BucketName(), upload.getKey(), upload.getUploadId())),
                            s3Client);

                        // Log the information about the aborted multipart upload.
                        LOGGER.info(String.format("Aborted S3 multipart upload for \"%s\" object key initiated at [%s] in \"%s\" S3 bucket.", upload.getKey(),
                            upload.getInitiated(), params.getS3BucketName()));

                        // Increment the counter.
                        abortedMultipartUploadsCount++;
                    }
                }

                // Determine whether there are more uploads to list.
                truncated = uploadListing.isTruncated();
                if (truncated)
                {
                    // Record the list markers.
                    uploadIdMarker = uploadListing.getUploadIdMarker();
                    keyMarker = uploadListing.getKeyMarker();
                }
            }
            while (truncated);
        }
        finally
        {
            // Shutdown the Amazon S3 client instance to release resources.
            if (s3Client != null)
            {
                s3Client.shutdown();
            }
        }

        return abortedMultipartUploadsCount;
    }

    /**
     * Prepares the object metadata for server side encryption and reduced redundancy storage.
     *
     * @param params the parameters.
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
        if (Boolean.TRUE.equals(params.getUseRrs()))
        {
            // TODO: For upload File, we can set RRS on the putObjectRequest. For uploadDirectory, this is the only
            // way to do it. However, setHeader() is flagged as For Internal Use Only
            metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.ReducedRedundancy.toString());
        }
    }

    /**
     * Gets a transfer manager with the specified parameters including proxy host, proxy port, S3 access key, S3 secret key, and max threads.
     *
     * @param params the parameters.
     *
     * @return a newly created transfer manager.
     */
    private TransferManager getTransferManager(final S3FileTransferRequestParamsDto params)
    {
        // We are returning a new transfer manager each time it is called. Although the Javadocs of TransferManager say to share a single instance
        // if possible, this could potentially be a problem if TransferManager.shutdown(true) is called and underlying resources are not present when needed
        // for subsequent transfers.
        if (params.getMaxThreads() == null)
        {
            // Create a transfer manager that will internally use an appropriate number of threads.
            return new TransferManager(getAmazonS3(params));
        }
        else
        {
            // Create a transfer manager with our own executor configured with the specified total threads.
            LOGGER.info("Creating a transfer manager with max threads: " + params.getMaxThreads());
            return new TransferManager(getAmazonS3(params), Executors.newFixedThreadPool(params.getMaxThreads()));
        }
    }

    /**
     * Gets a new S3 client based on the specified parameters. The HTTP proxy information will be added if the host and port are specified in the parameters.
     *
     * @param params the parameters.
     *
     * @return the Amazon S3 client.
     */
    private AmazonS3Client getAmazonS3(S3FileTransferRequestParamsDto params)
    {
        AmazonS3Client amazonS3Client;

        ClientConfiguration clientConfiguration = null;

        // Creates and sets proxy configuration if proxy is specified
        if (StringUtils.isNotBlank(params.getHttpProxyHost()) && StringUtils.isNotBlank(params.getHttpProxyPort().toString()))
        {
            clientConfiguration = new ClientConfiguration();
            clientConfiguration.setProxyHost(params.getHttpProxyHost());
            clientConfiguration.setProxyPort(params.getHttpProxyPort());
        }

        // Creates and sets signer override if signer override is specified
        if (StringUtils.isNotBlank(params.getSignerOverride()))
        {
            if (clientConfiguration == null)
            {
                clientConfiguration = new ClientConfiguration();
            }

            clientConfiguration.setSignerOverride(params.getSignerOverride());
        }

        AWSCredentialsProvider awsCredentialsProvider = getAWSCredentialsProvider(params);
        if (clientConfiguration != null)
        {
            // Create an S3 client with HTTP proxy information.
            amazonS3Client = new AmazonS3Client(awsCredentialsProvider, clientConfiguration);
        }
        else
        {
            // Create an S3 client with no proxy information.
            amazonS3Client = new AmazonS3Client(awsCredentialsProvider);
        }

        // Set the optional endpoint if configured.
        if (StringUtils.isNotBlank(params.getS3Endpoint()))
        {
            LOGGER.info("Configured S3 Endpoint: " + params.getS3Endpoint());
            amazonS3Client.setEndpoint(params.getS3Endpoint());
        }

        // Return the newly created client.
        return amazonS3Client;
    }

    /**
     * <p> Gets the {@link AWSCredentialsProvider} based on the credentials in the given parameters. </p> <p> Returns {@link DefaultAWSCredentialsProviderChain}
     * if either access or secret key is {@code null}. Otherwise returns a {@link StaticCredentialsProvider} with the credentials. </p>
     *
     * @param params - Access parameters
     *
     * @return AWS credentials provider implementation
     */
    private AWSCredentialsProvider getAWSCredentialsProvider(S3FileTransferRequestParamsDto params)
    {
        List<AWSCredentialsProvider> providers = new ArrayList<>();
        String accessKey = params.getS3AccessKey();
        String secretKey = params.getS3SecretKey();
        if (accessKey != null && secretKey != null)
        {
            providers.add(new StaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        }
        for (HerdAWSCredentialsProvider herdAWSCredentialsProvider : params.getAdditionalAwsCredentialsProviders())
        {
            providers.add(new HerdAwsCredentialsProviderWrapper(herdAWSCredentialsProvider));
        }
        providers.add(new DefaultAWSCredentialsProviderChain());
        return new AWSCredentialsProviderChain(providers.toArray(new AWSCredentialsProvider[providers.size()]));
    }

    /**
     * Performs a file/directory transfer.
     *
     * @param params the parameters.
     * @param transferer a transferer that knows how to perform the transfer.
     *
     * @return the results.
     * @throws InterruptedException if a problem is encountered.
     */
    private S3FileTransferResultsDto performTransfer(final S3FileTransferRequestParamsDto params, Transferer transferer) throws InterruptedException
    {
        // Create a transfer manager.
        TransferManager transferManager = null;

        try
        {
            // Create a transfer manager.
            transferManager = getTransferManager(params);

            // Start a stop watch to keep track of how long the transfer takes.
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            // Perform the transfer.
            Transfer transfer = transferer.performTransfer(transferManager);
            TransferProgress transferProgress = transfer.getProgress();

            LOGGER.info(String
                .format("%d bytes transferred out of %d (%.1f%%)", transferProgress.getBytesTransferred(), transferProgress.getTotalBytesToTransfer(),
                    transferProgress.getPercentTransferred()));

            long stepCount = 0;

            // Loop until the transfer is complete.
            do
            {
                Thread.sleep(SLEEP_INTERVAL_MILLIS);
                stepCount++;

                // Log progress status every 30 seconds and when transfer is complete.
                if (transfer.isDone() || stepCount % 300 == 0)
                {
                    LOGGER.info(String
                        .format("%d bytes transferred out of %d (%.1f%%)", transferProgress.getBytesTransferred(), transferProgress.getTotalBytesToTransfer(),
                            transferProgress.getPercentTransferred()));
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
                    "The transfer operation \"" + transfer.getDescription() + "\" did not complete successfully. Current state: \"" + transferState +
                        "\".");
            }

            // TransferProgress.getBytesTransferred() are not populated for S3 Copy objects.
            if (!(transfer instanceof Copy))
            {
                // Sanity check for the number of bytes transferred.
                Assert.isTrue(transferProgress.getBytesTransferred() >= transferProgress.getTotalBytesToTransfer(), String
                    .format("Actual number of bytes transferred is less than expected (actual: %d bytes; expected: %d bytes).",
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
            if (transferManager != null)
            {
                transferManager.shutdownNow();
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
         *
         * @return the transfer information for the transfer. This will typically be returned from an operation on the transfer manager (e.g. upload).
         */
        public Transfer performTransfer(TransferManager transferManager);
    }

    /**
     * Lists all S3 objects matching the S3 key prefix in the given bucket (S3 bucket name). The S3 bucket name and S3 key prefix that identify the S3 objects
     * to get listed are taken from the S3 file transfer request parameters DTO.
     *
     * @param params the S3 file transfer request parameters
     * @param ignoreZeroByteDirectoryMarkers specifies whether to ignore 0 byte objects that represent S3 directories
     *
     * @return the list of all S3 objects represented as storage files that match the prefix in the given bucket
     */
    private List<StorageFile> listObjectsMatchingKeyPrefix(final S3FileTransferRequestParamsDto params, boolean ignoreZeroByteDirectoryMarkers)
    {
        AmazonS3Client s3Client = null;
        List<StorageFile> storageFiles = new ArrayList<>();

        try
        {
            s3Client = getAmazonS3(params);
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
                        storageFiles.add(new StorageFile(objectSummary.getKey(), objectSummary.getSize(), null, null));
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
            throw new IllegalStateException(String
                .format("Failed to list keys/objects with prefix \"%s\" from bucket \"%s\". Reason: %s", params.getS3KeyPrefix(), params.getS3BucketName(),
                    e.getMessage()), e);
        }
        finally
        {
            // Shutdown the AmazonS3Client instance to release resources.
            if (s3Client != null)
            {
                s3Client.shutdown();
            }
        }

        return storageFiles;
    }

    @Override
    public S3Object getS3Object(GetObjectRequest getObjectRequest, S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto)
    {
        AmazonS3Client s3 = getAmazonS3(s3FileTransferRequestParamsDto);
        try
        {
            return s3Operations.getS3Object(getObjectRequest, s3);
        }
        catch (AmazonServiceException amazonServiceException)
        {
            String errorCode = amazonServiceException.getErrorCode();

            switch (errorCode)
            {
                case S3Operations.ERROR_CODE_ACCESS_DENIED:
                    throw new ObjectNotFoundException(
                        "Application does not have access to the specified S3 object at bucket '" + getObjectRequest.getBucketName() + "' and key '" +
                            getObjectRequest.getKey() + "'.", amazonServiceException);
                case S3Operations.ERROR_CODE_NO_SUCH_BUCKET:
                    throw new ObjectNotFoundException("Specified S3 bucket '" + getObjectRequest.getBucketName() + "' does not exist.", amazonServiceException);
                case S3Operations.ERROR_CODE_NO_SUCH_KEY:
                    throw new ObjectNotFoundException("Specified S3 object key '" + getObjectRequest.getKey() + "' does not exist.", amazonServiceException);
                default:
                    throw amazonServiceException;
            }
        }
    }

    @Override
    public Properties getProperties(String bucketName, String key, S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto)
    {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        S3Object s3Object = getS3Object(getObjectRequest, s3FileTransferRequestParamsDto);
        try (S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent())
        {
            return javaPropertiesHelper.getProperties(s3ObjectInputStream);
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException("The properties file in S3 bucket '" + bucketName + "' and key '" + key + "' is invalid.", e);
        }
        catch (IOException e)
        {
            // This exception can come when the try-with-resources attempts to close the stream.
            // Not covered as JUnit, unfortunately no way of producing an IO exception.
            throw new IllegalStateException("Error closing S3 object input stream. See cause for details.", e);
        }
    }

    @Override
    public String generateGetObjectPresignedUrl(String bucketName, String key, Date expiration, S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto)
    {
        GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, key, HttpMethod.GET);
        generatePresignedUrlRequest.setExpiration(expiration);
        AmazonS3Client s3 = getAmazonS3(s3FileTransferRequestParamsDto);
        return s3Operations.generatePresignedUrl(generatePresignedUrlRequest, s3).toString();
    }
}
