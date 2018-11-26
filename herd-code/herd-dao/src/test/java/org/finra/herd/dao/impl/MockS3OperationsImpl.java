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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingResult;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.internal.CopyImpl;
import com.amazonaws.services.s3.transfer.internal.DownloadImpl;
import com.amazonaws.services.s3.transfer.internal.MultipleFileDownloadImpl;
import com.amazonaws.services.s3.transfer.internal.MultipleFileUploadImpl;
import com.amazonaws.services.s3.transfer.internal.TransferMonitor;
import com.amazonaws.services.s3.transfer.internal.UploadImpl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.concurrent.BasicFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.S3Operations;

/**
 * Mock implementation of AWS S3 operations. Simulates S3 by storing objects in memory.
 * <p/>
 * Some operations use a series of predefined prefixes to hint the operation to behave in a certain manner (throwing exceptions, for example).
 * <p/>
 * Some operations which either put or list objects, will NOT throw an exception even when a specified bucket do not exist. This is because some tests are
 * assuming that the bucket already exists and test may not have permissions to create test buckets during unit tests when testing against real S3.
 */
public class MockS3OperationsImpl implements S3Operations
{
    /**
     * A mock KMS ID.
     */
    public static final String MOCK_KMS_ID = "mock_kms_id";

    /**
     * A mock KMS ID that will cause a canceled transfer.
     */
    public static final String MOCK_KMS_ID_CANCELED_TRANSFER = "mock_kms_id_canceled_transfer";

    /**
     * A mock KMS ID that will cause a failed transfer.
     */
    public static final String MOCK_KMS_ID_FAILED_TRANSFER = "mock_kms_id_failed_transfer";

    /**
     * A mock KMS ID that will cause a failed transfer with no underlying exception.
     */
    public static final String MOCK_KMS_ID_FAILED_TRANSFER_NO_EXCEPTION = "mock_kms_id_failed_transfer_no_exception";

    /**
     * A mock S3 bucket name which hints that method should throw an AmazonServiceException with a 403 status code.
     */
    public static final String MOCK_S3_BUCKET_NAME_ACCESS_DENIED = "MOCK_S3_BUCKET_NAME_ACCESS_DENIED";

    /**
     * A mock S3 bucket name which hints that method should throw an AmazonServiceException.
     */
    public static final String MOCK_S3_BUCKET_NAME_INTERNAL_ERROR = "MOCK_S3_BUCKET_NAME_INTERNAL_ERROR";

    /**
     * A mock S3 bucket name which hints that method should throw an AmazonServiceException with a 404 status code.
     */
    public static final String MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION = "MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION";

    /**
     * Suffix to hint operation to throw a AmazonServiceException
     */
    public static final String MOCK_S3_BUCKET_NAME_SERVICE_EXCEPTION = "mock_s3_bucket_name_service_exception";

    /**
     * Suffix to hint multipart listing to return a truncated list.
     */
    public static final String MOCK_S3_BUCKET_NAME_TRUNCATED_MULTIPART_LISTING = "mock_s3_bucket_name_truncated_multipart_listing";

    /**
     * Suffix to hint operation to treat the S3 bucket as S3 bucket with enabled versioning.
     */
    public static final String MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED = "mock_s3_bucket_name_versioning_enabled";

    /**
     * Suffix to hint operation to throw a AmazonServiceException
     */
    public static final String MOCK_S3_FILE_NAME_SERVICE_EXCEPTION = "mock_s3_file_name_service_exception";

    /**
     * The description for a mock transfer.
     */
    public static final String MOCK_TRANSFER_DESCRIPTION = "MockTransfer";

    /**
     * Logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MockS3OperationsImpl.class);

    /**
     * The buckets that are available in-memory.
     */
    private Map<String, MockS3Bucket> mockS3Buckets = new HashMap<>();

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation simulates abort multipart upload operation.
     */
    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest abortMultipartUploadRequest, AmazonS3 s3Client)
    {
        // This method does nothing.
    }

    /**
     * {@inheritDoc} <p/> <p> This implementation simulates a copyFile operation. </p> <p> This method copies files in-memory. </p> <p> The result {@link Copy}
     * has the following properties: <dl> <p/> <dt>description</dt> <dd>"MockTransfer"</dd> <p/> <dt>state</dt> <dd>{@link TransferState#Completed}</dd> <p/>
     * <dt>transferProgress.totalBytesToTransfer</dt> <dd>1024</dd> <p/> <dt>transferProgress.updateProgress</dt> <dd>1024</dd> <p/> </dl> <p/> All other
     * properties are set as default. </p> <p> This operation takes the following hints when suffixed in copyObjectRequest.sourceKey: <dl> <p/>
     * <dt>MOCK_S3_FILE_NAME_SERVICE_EXCEPTION</dt> <dd>Throws a AmazonServiceException</dd> <p/> </dl> </p>
     */
    @Override
    public Copy copyFile(final CopyObjectRequest copyObjectRequest, TransferManager transferManager)
    {
        LOGGER.debug(
            "copyFile(): copyObjectRequest.getSourceBucketName() = " + copyObjectRequest.getSourceBucketName() + ", copyObjectRequest.getSourceKey() = " +
                copyObjectRequest.getSourceKey() + ", copyObjectRequest.getDestinationBucketName() = " + copyObjectRequest.getDestinationBucketName() +
                ", copyObjectRequest.getDestinationKey() = " + copyObjectRequest.getDestinationKey());

        if (copyObjectRequest.getSourceKey().endsWith(MOCK_S3_FILE_NAME_SERVICE_EXCEPTION))
        {
            throw new AmazonServiceException(null);
        }

        String sourceBucketName = copyObjectRequest.getSourceBucketName();
        String sourceKey = copyObjectRequest.getSourceKey();

        MockS3Bucket mockSourceS3Bucket = getOrCreateBucket(sourceBucketName);
        MockS3Object mockSourceS3Object = mockSourceS3Bucket.getObjects().get(sourceKey);

        if (mockSourceS3Object == null)
        {
            AmazonServiceException amazonServiceException = new AmazonServiceException(S3Operations.ERROR_CODE_NO_SUCH_KEY);
            amazonServiceException.setErrorCode(S3Operations.ERROR_CODE_NO_SUCH_KEY);
            throw amazonServiceException;
        }

        // Set the result CopyImpl and TransferProgress.
        TransferProgress transferProgress = new TransferProgress();
        transferProgress.setTotalBytesToTransfer(mockSourceS3Object.getObjectMetadata().getContentLength());
        transferProgress.updateProgress(mockSourceS3Object.getObjectMetadata().getContentLength());
        CopyImpl copy = new CopyImpl(MOCK_TRANSFER_DESCRIPTION, transferProgress, null, null);
        copy.setState(TransferState.Completed);

        // If an invalid KMS Id was passed in, mark the transfer as failed and return an exception via the transfer monitor.
        if (copyObjectRequest.getSSEAwsKeyManagementParams() != null)
        {
            final String kmsId = copyObjectRequest.getSSEAwsKeyManagementParams().getAwsKmsKeyId();
            if (kmsId.startsWith(MOCK_KMS_ID_FAILED_TRANSFER))
            {
                copy.setState(TransferState.Failed);
                copy.setMonitor(new TransferMonitor()
                {
                    @Override
                    public Future<?> getFuture()
                    {
                        if (!kmsId.equals(MOCK_KMS_ID_FAILED_TRANSFER_NO_EXCEPTION))
                        {
                            throw new AmazonServiceException("Key '" + copyObjectRequest.getSSEAwsKeyManagementParams().getAwsKmsKeyId() +
                                "' does not exist (Service: Amazon S3; Status Code: 400; Error Code: KMS.NotFoundException; Request ID: 1234567890123456)");
                        }

                        // We don't want an exception to be thrown so return a basic future that won't throw an exception.
                        BasicFuture<?> future = new BasicFuture<Void>(null);
                        future.completed(null);
                        return future;
                    }

                    @Override
                    public boolean isDone()
                    {
                        return true;
                    }
                });
            }
            else if (kmsId.startsWith(MOCK_KMS_ID_CANCELED_TRANSFER))
            {
                // If the KMS indicates a cancelled transfer, just update the state to canceled.
                copy.setState(TransferState.Canceled);
            }
        }

        // If copy operation is marked as completed, perform the actual file copy in memory.
        if (copy.getState().equals(TransferState.Completed))
        {
            String destinationBucketName = copyObjectRequest.getDestinationBucketName();
            String destinationObjectKey = copyObjectRequest.getDestinationKey();
            String destinationObjectVersion = MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED.equals(destinationBucketName) ? UUID.randomUUID().toString() : null;
            String destinationObjectKeyVersion = destinationObjectKey + (destinationObjectVersion != null ? destinationObjectVersion : "");

            ObjectMetadata objectMetadata = copyObjectRequest.getNewObjectMetadata();
            MockS3Object mockDestinationS3Object = new MockS3Object();
            mockDestinationS3Object.setKey(destinationObjectKey);
            mockDestinationS3Object.setVersion(destinationObjectVersion);
            mockDestinationS3Object.setData(Arrays.copyOf(mockSourceS3Object.getData(), mockSourceS3Object.getData().length));
            mockDestinationS3Object.setObjectMetadata(objectMetadata);

            MockS3Bucket mockDestinationS3Bucket = getOrCreateBucket(destinationBucketName);
            mockDestinationS3Bucket.getObjects().put(destinationObjectKey, mockDestinationS3Object);
            mockDestinationS3Bucket.getVersions().put(destinationObjectKeyVersion, mockDestinationS3Object);
        }

        return copy;
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest, AmazonS3 s3Client)
    {
        LOGGER.debug("deleteObjects(): deleteObjectRequest.getBucketName() = " + deleteObjectsRequest.getBucketName() + ", deleteObjectRequest.getKeys() = " +
            deleteObjectsRequest.getKeys());

        List<DeletedObject> deletedObjects = new ArrayList<>();

        MockS3Bucket mockS3Bucket = mockS3Buckets.get(deleteObjectsRequest.getBucketName());

        for (KeyVersion keyVersion : deleteObjectsRequest.getKeys())
        {
            String s3ObjectKey = keyVersion.getKey();
            String s3ObjectVersion = keyVersion.getVersion();
            String s3ObjectKeyVersion = s3ObjectKey + (s3ObjectVersion != null ? s3ObjectVersion : "");

            mockS3Bucket.getObjects().remove(s3ObjectKey);

            if (mockS3Bucket.getVersions().remove(s3ObjectKeyVersion) != null)
            {
                DeletedObject deletedObject = new DeletedObject();
                deletedObject.setKey(s3ObjectKey);
                deletedObject.setVersionId(s3ObjectVersion);
                deletedObjects.add(deletedObject);
            }
        }

        return new DeleteObjectsResult(deletedObjects);
    }

    @Override
    public Download download(String bucket, String key, File file, TransferManager transferManager)
    {
        MockS3Bucket mockS3Bucket = mockS3Buckets.get(bucket);
        MockS3Object mockS3Object = mockS3Bucket.getObjects().get(key);
        try (FileOutputStream fileOutputStream = new FileOutputStream(file))
        {
            fileOutputStream.write(mockS3Object.getData());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing to file " + file, e);
        }

        TransferProgress progress = new TransferProgress();
        progress.setTotalBytesToTransfer(mockS3Object.getData().length);
        progress.updateProgress(mockS3Object.getData().length);

        DownloadImpl download =
            new DownloadImpl(null, progress, null, null, null, new GetObjectRequest(bucket, key), file, mockS3Object.getObjectMetadata(), false);
        download.setState(TransferState.Completed);

        return download;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation creates any directory that does not exist in the path to the destination directory.
     */
    @Override
    public MultipleFileDownload downloadDirectory(String bucketName, String keyPrefix, File destinationDirectory, TransferManager transferManager)
    {
        LOGGER.debug("downloadDirectory(): bucketName = " + bucketName + ", keyPrefix = " + keyPrefix + ", destinationDirectory = " + destinationDirectory);

        MockS3Bucket mockS3Bucket = mockS3Buckets.get(bucketName);

        List<Download> downloads = new ArrayList<>();
        long totalBytes = 0;

        if (mockS3Bucket != null)
        {
            for (MockS3Object mockS3Object : mockS3Bucket.getObjects().values())
            {
                if (mockS3Object.getKey().startsWith(keyPrefix))
                {
                    String filePath = destinationDirectory.getAbsolutePath() + "/" + mockS3Object.getKey();
                    File file = new File(filePath);
                    file.getParentFile().mkdirs(); // Create any directory in the path that does not exist.
                    try (FileOutputStream fileOutputStream = new FileOutputStream(file))
                    {
                        LOGGER.debug("downloadDirectory(): Writing file " + file);
                        fileOutputStream.write(mockS3Object.getData());
                        totalBytes += mockS3Object.getData().length;
                        downloads.add(new DownloadImpl(null, null, null, null, null, new GetObjectRequest(bucketName, mockS3Object.getKey()), file,
                            mockS3Object.getObjectMetadata(), false));
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException("Error writing to file " + file, e);
                    }
                }
            }
        }

        TransferProgress progress = new TransferProgress();
        progress.setTotalBytesToTransfer(totalBytes);
        progress.updateProgress(totalBytes);

        MultipleFileDownloadImpl multipleFileDownload = new MultipleFileDownloadImpl(null, progress, null, keyPrefix, bucketName, downloads);
        multipleFileDownload.setState(TransferState.Completed);
        return multipleFileDownload;
    }

    /**
     * {@inheritDoc} <p/> <p> A mock implementation which generates a URL which reflects the given request. </p> <p> The URL is composed as such: </p> <p/>
     * <pre>
     * https://{s3BucketName}/{s3ObjectKey}?{queryParams}
     * </pre>
     * <p> Where {@code queryParams} is the URL encoded list of parameters given in the request. </p> <p> The query params include: </p> TODO: list the query
     * params in the result.
     */
    @Override
    public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest, AmazonS3 s3)
    {
        String host = generatePresignedUrlRequest.getBucketName();
        StringBuilder file = new StringBuilder();
        file.append('/').append(generatePresignedUrlRequest.getKey());
        file.append("?method=").append(generatePresignedUrlRequest.getMethod());
        file.append("&expiration=").append(generatePresignedUrlRequest.getExpiration().getTime());
        try
        {
            return new URL("https", host, file.toString());
        }
        catch (MalformedURLException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ObjectMetadata getObjectMetadata(String s3BucketName, String s3Key, AmazonS3 s3Client)
    {
        return getMockS3Object(s3BucketName, s3Key).getObjectMetadata();
    }

    @Override
    public GetObjectTaggingResult getObjectTagging(GetObjectTaggingRequest getObjectTaggingRequest, AmazonS3 s3Client)
    {
        return new GetObjectTaggingResult(
            getMockS3Object(getObjectTaggingRequest.getBucketName(), getObjectTaggingRequest.getKey(), getObjectTaggingRequest.getVersionId()).getTags());
    }

    @Override
    public S3Object getS3Object(GetObjectRequest getObjectRequest, AmazonS3 s3)
    {
        MockS3Object mockS3Object = getMockS3Object(getObjectRequest.getBucketName(), getObjectRequest.getKey());

        S3Object s3Object = new S3Object();
        s3Object.setBucketName(getObjectRequest.getBucketName());
        s3Object.setKey(getObjectRequest.getKey());
        s3Object.setObjectContent(new ByteArrayInputStream(mockS3Object.getData()));
        s3Object.setObjectMetadata(mockS3Object.getObjectMetadata());

        return s3Object;
    }

    /**
     * {@inheritDoc} <p/> <p> Since a multipart upload in progress does not exist when in-memory, this method simply returns a preconfigured list. </p> <p>
     * Returns a mock {@link MultipartUploadListing} based on the parameters and hints provided. By default returns a mock listing as defiend by {@link
     * #getMultipartUploadListing()}. </p> <p> This operation takes the following hints when suffixed in listMultipartUploadsRequest.bucketName: <dl> <p/>
     * <dt>MOCK_S3_BUCKET_NAME_SERVICE_EXCEPTION</dt> <dd>Throws a AmazonServiceException</dd> <p/> <dt>MOCK_S3_BUCKET_NAME_TRUNCATED_MULTIPART_LISTING</dt>
     * <dd>Returns the listing as if it is truncated. See below for details.</dd> <p/> </dl> </p>
     */
    @Override
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest listMultipartUploadsRequest, AmazonS3 s3Client)
    {
        if (listMultipartUploadsRequest.getBucketName().equals(MOCK_S3_BUCKET_NAME_SERVICE_EXCEPTION))
        {
            throw new AmazonServiceException(null);
        }
        else if (listMultipartUploadsRequest.getBucketName().equals(MOCK_S3_BUCKET_NAME_TRUNCATED_MULTIPART_LISTING))
        {
            MultipartUploadListing multipartUploadListing = getMultipartUploadListing();

            // If listing request does not have upload ID marker set, mark the listing as truncated - this is done to truncate the multipart listing just once.
            if (listMultipartUploadsRequest.getUploadIdMarker() == null)
            {
                multipartUploadListing.setNextUploadIdMarker("TEST_UPLOAD_MARKER_ID");
                multipartUploadListing.setNextKeyMarker("TEST_KEY_MARKER_ID");
                multipartUploadListing.setTruncated(true);
            }

            return multipartUploadListing;
        }
        else
        {
            return getMultipartUploadListing();
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * If the bucket does not exist, returns a listing with an empty list. If a prefix is specified in listObjectsRequest, only keys starting with the prefix
     * will be returned.
     */
    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest, AmazonS3 s3Client)
    {
        LOGGER.debug("listObjects(): listObjectsRequest.getBucketName() = " + listObjectsRequest.getBucketName());

        String bucketName = listObjectsRequest.getBucketName();

        if (MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION.equals(bucketName))
        {
            AmazonS3Exception amazonS3Exception = new AmazonS3Exception(MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION);
            amazonS3Exception.setErrorCode("NoSuchBucket");
            throw amazonS3Exception;
        }

        ObjectListing objectListing = new ObjectListing();
        objectListing.setBucketName(bucketName);

        MockS3Bucket mockS3Bucket = mockS3Buckets.get(bucketName);
        if (mockS3Bucket != null)
        {
            for (MockS3Object mockS3Object : mockS3Bucket.getObjects().values())
            {
                String s3ObjectKey = mockS3Object.getKey();
                if (listObjectsRequest.getPrefix() == null || s3ObjectKey.startsWith(listObjectsRequest.getPrefix()))
                {
                    S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
                    s3ObjectSummary.setBucketName(bucketName);
                    s3ObjectSummary.setKey(s3ObjectKey);
                    s3ObjectSummary.setSize(mockS3Object.getData().length);
                    s3ObjectSummary.setStorageClass(mockS3Object.getObjectMetadata() != null ? mockS3Object.getObjectMetadata().getStorageClass() : null);

                    objectListing.getObjectSummaries().add(s3ObjectSummary);
                }
            }
        }

        return objectListing;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * If the bucket does not exist, returns a listing with an empty list. If a prefix is specified in listVersionsRequest, only versions starting with the
     * prefix will be returned.
     */
    @Override
    public VersionListing listVersions(ListVersionsRequest listVersionsRequest, AmazonS3 s3Client)
    {
        LOGGER.debug("listVersions(): listVersionsRequest.getBucketName() = " + listVersionsRequest.getBucketName());

        String bucketName = listVersionsRequest.getBucketName();

        if (MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION.equals(bucketName))
        {
            AmazonS3Exception amazonS3Exception = new AmazonS3Exception(MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION);
            amazonS3Exception.setErrorCode("NoSuchBucket");
            throw amazonS3Exception;
        }
        else if (MOCK_S3_BUCKET_NAME_INTERNAL_ERROR.equals(bucketName))
        {
            throw new AmazonServiceException(S3Operations.ERROR_CODE_INTERNAL_ERROR);
        }

        VersionListing versionListing = new VersionListing();
        versionListing.setBucketName(bucketName);

        MockS3Bucket mockS3Bucket = mockS3Buckets.get(bucketName);
        if (mockS3Bucket != null)
        {
            for (MockS3Object mockS3Object : mockS3Bucket.getVersions().values())
            {
                String s3ObjectKey = mockS3Object.getKey();
                if (listVersionsRequest.getPrefix() == null || s3ObjectKey.startsWith(listVersionsRequest.getPrefix()))
                {
                    S3VersionSummary s3VersionSummary = new S3VersionSummary();
                    s3VersionSummary.setBucketName(bucketName);
                    s3VersionSummary.setKey(s3ObjectKey);
                    s3VersionSummary.setVersionId(mockS3Object.getVersion());
                    s3VersionSummary.setSize(mockS3Object.getData().length);
                    s3VersionSummary.setStorageClass(mockS3Object.getObjectMetadata() != null ? mockS3Object.getObjectMetadata().getStorageClass() : null);

                    versionListing.getVersionSummaries().add(s3VersionSummary);
                }
            }
        }

        return versionListing;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation creates a new bucket if the bucket does not already exist.
     */
    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest, AmazonS3 s3Client)
    {
        LOGGER.debug("putObject(): putObjectRequest.getBucketName() = " + putObjectRequest.getBucketName() + ", putObjectRequest.getKey() = " +
            putObjectRequest.getKey());

        String s3BucketName = putObjectRequest.getBucketName();
        InputStream inputStream = putObjectRequest.getInputStream();

        ObjectMetadata metadata = putObjectRequest.getMetadata();
        if (metadata == null)
        {
            metadata = new ObjectMetadata();
        }

        File file = putObjectRequest.getFile();
        if (file != null)
        {
            try
            {
                inputStream = new FileInputStream(file);
                metadata.setContentLength(file.length());
            }
            catch (FileNotFoundException e)
            {
                throw new IllegalArgumentException("File not found " + file, e);
            }
        }

        String s3ObjectKey = putObjectRequest.getKey();
        String s3ObjectVersion = MOCK_S3_BUCKET_NAME_VERSIONING_ENABLED.equals(putObjectRequest.getBucketName()) ? UUID.randomUUID().toString() : null;
        String s3ObjectKeyVersion = s3ObjectKey + (s3ObjectVersion != null ? s3ObjectVersion : "");

        byte[] s3ObjectData;
        try
        {
            s3ObjectData = IOUtils.toByteArray(inputStream);
            metadata.setContentLength(s3ObjectData.length);
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Error converting input stream into byte array", e);
        }
        finally
        {
            try
            {
                inputStream.close();
            }
            catch (IOException e)
            {
                LOGGER.error("Error closing stream " + inputStream, e);
            }
        }

        // Update the Last-Modified header value. This value not being set causes NullPointerException in S3Dao download related unit tests.
        metadata.setLastModified(new Date());

        MockS3Bucket mockS3Bucket = getOrCreateBucket(s3BucketName);

        MockS3Object mockS3Object = new MockS3Object();
        mockS3Object.setKey(s3ObjectKey);
        mockS3Object.setVersion(s3ObjectVersion);
        mockS3Object.setData(s3ObjectData);
        mockS3Object.setObjectMetadata(metadata);

        if (putObjectRequest.getTagging() != null)
        {
            mockS3Object.setTags(putObjectRequest.getTagging().getTagSet());
        }

        mockS3Bucket.getObjects().put(s3ObjectKey, mockS3Object);
        mockS3Bucket.getVersions().put(s3ObjectKeyVersion, mockS3Object);

        return new PutObjectResult();
    }

    @Override
    public void restoreObject(RestoreObjectRequest requestRestore, AmazonS3 s3Client)
    {
        if (requestRestore.getKey().endsWith(MockAwsOperationsHelper.AMAZON_THROTTLING_EXCEPTION))
        {
            AmazonServiceException throttlingException = new AmazonServiceException("test throttling exception");
            throttlingException.setErrorCode("ThrottlingException");
            throw throttlingException;
        }
        else if (MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION.equals(requestRestore.getBucketName()))
        {
            AmazonServiceException amazonServiceException = new AmazonServiceException(S3Operations.ERROR_CODE_NO_SUCH_BUCKET);
            amazonServiceException.setStatusCode(404);
            throw amazonServiceException;
        }
        else if (MOCK_S3_BUCKET_NAME_ACCESS_DENIED.equals(requestRestore.getBucketName()))
        {
            AmazonServiceException amazonServiceException = new AmazonServiceException(S3Operations.ERROR_CODE_ACCESS_DENIED);
            amazonServiceException.setStatusCode(403);
            throw amazonServiceException;
        }
        else if (MOCK_S3_BUCKET_NAME_INTERNAL_ERROR.equals(requestRestore.getBucketName()) ||
            requestRestore.getKey().endsWith(MOCK_S3_FILE_NAME_SERVICE_EXCEPTION))
        {
            throw new AmazonServiceException(S3Operations.ERROR_CODE_INTERNAL_ERROR);
        }
        else
        {
            MockS3Bucket mockS3Bucket = getOrCreateBucket(requestRestore.getBucketName());
            MockS3Object mockS3Object = mockS3Bucket.getObjects().get(requestRestore.getKey());

            if (mockS3Object == null)
            {
                AmazonServiceException amazonServiceException = new AmazonServiceException(S3Operations.ERROR_CODE_NO_SUCH_KEY);
                amazonServiceException.setStatusCode(404);
                throw amazonServiceException;
            }

            // Get object metadata.
            ObjectMetadata objectMetadata = mockS3Object.getObjectMetadata();

            // Fail if the object is not in Glacier.
            if (!StorageClass.Glacier.toString().equals(objectMetadata.getStorageClass()))
            {
                AmazonServiceException amazonServiceException = new AmazonServiceException("object is not in Glacier");
                throw amazonServiceException;
            }

            // Fail if the object is already being restored.
            if (objectMetadata.getOngoingRestore())
            {
                AmazonServiceException amazonServiceException = new AmazonServiceException("object is already being restored");
                throw amazonServiceException;
            }

            // Update the object metadata to indicate that there is an ongoing restore request.
            objectMetadata.setOngoingRestore(true);
        }
    }

    @Override
    public void rollback()
    {
        // Clear all mock S3 buckets.
        mockS3Buckets.clear();
    }

    @Override
    public SetObjectTaggingResult setObjectTagging(SetObjectTaggingRequest setObjectTaggingRequest, AmazonS3 s3Client)
    {
        MockS3Object mockS3Object =
            getMockS3Object(setObjectTaggingRequest.getBucketName(), setObjectTaggingRequest.getKey(), setObjectTaggingRequest.getVersionId());

        if (setObjectTaggingRequest.getTagging() != null)
        {
            mockS3Object.setTags(setObjectTaggingRequest.getTagging().getTagSet());
        }
        else
        {
            mockS3Object.setTags(null);
        }

        return new SetObjectTaggingResult();
    }

    @Override
    public Upload upload(PutObjectRequest putObjectRequest, TransferManager transferManager)
    {
        LOGGER.debug(
            "upload(): putObjectRequest.getBucketName() = " + putObjectRequest.getBucketName() + ", putObjectRequest.getKey() = " + putObjectRequest.getKey());

        putObject(putObjectRequest, transferManager.getAmazonS3Client());

        long contentLength = putObjectRequest.getFile().length();
        TransferProgress progress = new TransferProgress();
        progress.setTotalBytesToTransfer(contentLength);
        progress.updateProgress(contentLength);

        UploadImpl upload = new UploadImpl(null, progress, null, null);
        upload.setState(TransferState.Completed);

        return upload;
    }

    @Override
    public MultipleFileUpload uploadDirectory(String bucketName, String virtualDirectoryKeyPrefix, File directory, boolean includeSubdirectories,
        ObjectMetadataProvider metadataProvider, TransferManager transferManager)
    {
        LOGGER.debug(
            "uploadDirectory(): bucketName = " + bucketName + ", virtualDirectoryKeyPrefix = " + virtualDirectoryKeyPrefix + ", directory = " + directory +
                ", includeSubdirectories = " + includeSubdirectories);

        List<File> files = new ArrayList<>();
        listFiles(directory, files, includeSubdirectories);

        return uploadFileList(bucketName, virtualDirectoryKeyPrefix, directory, files, metadataProvider, transferManager);
    }

    @Override
    public MultipleFileUpload uploadFileList(String bucketName, String virtualDirectoryKeyPrefix, File directory, List<File> files,
        ObjectMetadataProvider metadataProvider, TransferManager transferManager)
    {
        LOGGER.debug(
            "uploadFileList(): bucketName = " + bucketName + ", virtualDirectoryKeyPrefix = " + virtualDirectoryKeyPrefix + ", directory = " + directory +
                ", files = " + files);

        String directoryPath = directory.getAbsolutePath();

        long totalFileLength = 0;
        List<Upload> subTransfers = new ArrayList<>();
        for (File file : files)
        {
            // Get path to file relative to the specified directory
            String relativeFilePath = file.getAbsolutePath().substring(directoryPath.length());

            // Replace any backslashes (i.e. Windows separator) with a forward slash.
            relativeFilePath = relativeFilePath.replace("\\", "/");

            // Remove any leading slashes
            relativeFilePath = relativeFilePath.replaceAll("^/+", "");

            long fileLength = file.length();

            // Remove any trailing slashes
            virtualDirectoryKeyPrefix = virtualDirectoryKeyPrefix.replaceAll("/+$", "");

            String s3ObjectKey = virtualDirectoryKeyPrefix + "/" + relativeFilePath;
            totalFileLength += fileLength;

            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, s3ObjectKey, file);

            ObjectMetadata objectMetadata = new ObjectMetadata();
            metadataProvider.provideObjectMetadata(null, objectMetadata);
            putObjectRequest.setMetadata(objectMetadata);

            putObject(putObjectRequest, transferManager.getAmazonS3Client());

            subTransfers.add(new UploadImpl(null, null, null, null));
        }

        TransferProgress progress = new TransferProgress();
        progress.setTotalBytesToTransfer(totalFileLength);
        progress.updateProgress(totalFileLength);

        MultipleFileUploadImpl multipleFileUpload = new MultipleFileUploadImpl(null, progress, null, virtualDirectoryKeyPrefix, bucketName, subTransfers);
        multipleFileUpload.setState(TransferState.Completed);
        return multipleFileUpload;
    }

    /**
     * Gets a mock S3 object if one exists.
     *
     * @param s3BucketName the S3 bucket name
     * @param s3Key the S3 key
     *
     * @return the mock S3 object
     */
    private MockS3Object getMockS3Object(String s3BucketName, String s3Key)
    {
        return getMockS3Object(s3BucketName, s3Key, null);
    }

    /**
     * Gets a mock S3 object if one exists.
     *
     * @param s3BucketName the S3 bucket name
     * @param s3Key the S3 key
     * @param s3VersionId the S3 version identifier
     *
     * @return the mock S3 object
     */
    private MockS3Object getMockS3Object(String s3BucketName, String s3Key, String s3VersionId)
    {
        if (s3Key.endsWith(MockAwsOperationsHelper.AMAZON_THROTTLING_EXCEPTION))
        {
            AmazonServiceException throttlingException = new AmazonServiceException("test throttling exception");
            throttlingException.setErrorCode("ThrottlingException");
            throw throttlingException;
        }
        else if (MOCK_S3_BUCKET_NAME_NO_SUCH_BUCKET_EXCEPTION.equals(s3BucketName))
        {
            AmazonServiceException amazonServiceException = new AmazonServiceException(S3Operations.ERROR_CODE_NO_SUCH_BUCKET);
            amazonServiceException.setErrorCode(S3Operations.ERROR_CODE_NO_SUCH_BUCKET);
            amazonServiceException.setStatusCode(404);
            throw amazonServiceException;
        }
        else if (MOCK_S3_BUCKET_NAME_ACCESS_DENIED.equals(s3BucketName))
        {
            AmazonServiceException amazonServiceException = new AmazonServiceException(S3Operations.ERROR_CODE_ACCESS_DENIED);
            amazonServiceException.setErrorCode(S3Operations.ERROR_CODE_ACCESS_DENIED);
            amazonServiceException.setStatusCode(403);
            throw amazonServiceException;
        }
        else if (MOCK_S3_BUCKET_NAME_INTERNAL_ERROR.equals(s3BucketName) || s3Key.endsWith(MOCK_S3_FILE_NAME_SERVICE_EXCEPTION))
        {
            AmazonServiceException amazonServiceException = new AmazonServiceException(S3Operations.ERROR_CODE_INTERNAL_ERROR);
            amazonServiceException.setErrorCode(S3Operations.ERROR_CODE_INTERNAL_ERROR);
            throw amazonServiceException;
        }
        else
        {
            MockS3Bucket mockS3Bucket = getOrCreateBucket(s3BucketName);
            MockS3Object mockS3Object =
                StringUtils.isBlank(s3VersionId) ? mockS3Bucket.getObjects().get(s3Key) : mockS3Bucket.getVersions().get(s3Key + s3VersionId);

            if (mockS3Object == null)
            {
                AmazonServiceException amazonServiceException = new AmazonServiceException(S3Operations.ERROR_CODE_NO_SUCH_KEY);
                amazonServiceException.setErrorCode(S3Operations.ERROR_CODE_NO_SUCH_KEY);
                amazonServiceException.setStatusCode(404);
                throw amazonServiceException;
            }

            return mockS3Object;
        }
    }

    /**
     * Creates and returns a mock {@link MultipartUpload} with the given initiated timestamp.
     *
     * @param initiated the timestamp to set to initiate the object
     *
     * @return the mock object
     */

    private MultipartUpload getMultipartUpload(Date initiated)
    {
        MultipartUpload multipartUpload = new MultipartUpload();
        multipartUpload.setInitiated(initiated);
        return multipartUpload;
    }

    /**
     * <p> Returns a mock {@link MultipartUploadListing}. </p> <p> The return object has the following properties. <dl> <dt>multipartUploads</dt> <dd>Length 3
     * list</dd> <p/> <dt>multipartUploads[0].initiated</dt> <dd>5 minutes prior to the object creation time.</dd> <p/> <dt>multipartUploads[1].initiated</dt>
     * <dd>15 minutes prior to the object creation time.</dd> <p/> <dt>multipartUploads[2].initiated</dt> <dd>20 minutes prior to the object creation time.</dd>
     * </dl> <p/> All other properties as set to default as defined in the by {@link MultipartUploadListing} constructor. </p>
     *
     * @return a mock object
     */
    private MultipartUploadListing getMultipartUploadListing()
    {
        // Return 3 multipart uploads with 2 of them started more than 10 minutes ago.
        MultipartUploadListing multipartUploadListing = new MultipartUploadListing();
        List<MultipartUpload> multipartUploads = new ArrayList<>();
        multipartUploadListing.setMultipartUploads(multipartUploads);
        Date now = new Date();
        multipartUploads.add(getMultipartUpload(HerdDateUtils.addMinutes(now, -5)));
        multipartUploads.add(getMultipartUpload(HerdDateUtils.addMinutes(now, -15)));
        multipartUploads.add(getMultipartUpload(HerdDateUtils.addMinutes(now, -20)));
        return multipartUploadListing;
    }

    /**
     * Retrieves an existing mock S3 bucket or creates a new one and registers it if it does not exist.
     * <p/>
     * This method should only be used to retrieve mock bucket when a test assumes a bucket already exists.
     *
     * @param s3BucketName the S3 bucket name
     *
     * @return new or existing S3 mock bucket
     */
    private MockS3Bucket getOrCreateBucket(String s3BucketName)
    {
        MockS3Bucket mockS3Bucket = mockS3Buckets.get(s3BucketName);

        if (mockS3Bucket == null)
        {
            mockS3Bucket = new MockS3Bucket();
            mockS3Bucket.setName(s3BucketName);
            mockS3Buckets.put(s3BucketName, mockS3Bucket);
        }
        return mockS3Bucket;
    }

    /**
     * Lists files in the directory given and adds them to the result list passed in, optionally adding subdirectories recursively.</p>This implementation is
     * copied from {@link TransferManager#listFiles}.
     */
    private void listFiles(File dir, List<File> results, boolean includeSubDirectories)
    {
        File[] found = dir.listFiles();
        if (found != null)
        {
            for (File f : found)
            {
                if (f.isDirectory())
                {
                    if (includeSubDirectories)
                    {
                        listFiles(f, results, includeSubDirectories);
                    }
                }
                else
                {
                    results.add(f);
                }
            }
        }
    }
}
