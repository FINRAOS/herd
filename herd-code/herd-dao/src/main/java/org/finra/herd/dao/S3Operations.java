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

import java.io.File;
import java.net.URL;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingResult;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.model.CreateJobRequest;
import com.amazonaws.services.s3control.model.CreateJobResult;
import com.amazonaws.services.s3control.model.DescribeJobRequest;
import com.amazonaws.services.s3control.model.DescribeJobResult;

/**
 * AWS S3 Operations Service.
 */
public interface S3Operations
{
    public static final String ERROR_CODE_ACCESS_DENIED = "AccessDenied";
    public static final String ERROR_CODE_INTERNAL_ERROR = "InternalError";
    public static final String ERROR_CODE_NO_SUCH_BUCKET = "NoSuchBucket";
    public static final String ERROR_CODE_NO_SUCH_KEY = "NoSuchKey";

    /**
     * Aborts a multipart upload.
     *
     * @param abortMultipartUploadRequest the request object containing all the parameters for the operation
     * @param s3Client the {@link AmazonS3} implementation to use
     */
    public void abortMultipartUpload(AbortMultipartUploadRequest abortMultipartUploadRequest, AmazonS3 s3Client);

    /**
     * Schedules a new transfer to copy data from one Amazon S3 location to another Amazon S3 location.
     *
     * @param copyObjectRequest the request containing all the parameters for the copy
     * @param transferManager the transfer manager implementation to use
     */
    public Copy copyFile(CopyObjectRequest copyObjectRequest, TransferManager transferManager);

    /**
     * Deletes the specified S3 objects in the specified S3 bucket.
     *
     * @param deleteObjectsRequest the request object containing all the options for deleting multiple objects in a specified bucket
     * @param s3Client the {@link AmazonS3} implementation to use
     *
     * @return the successful response to the delete object request
     */
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest, AmazonS3 s3Client);

    /**
     * Schedules a new transfer to download data from Amazon S3 and save it to the specified file.
     *
     * @param s3BucketName the S3 bucket name
     * @param s3Key the S3 key
     * @param file the destination file
     * @param transferManager the transfer manager implementation to use
     *
     * @return the object that represents an asynchronous download from Amazon S3
     */
    public Download download(String s3BucketName, String s3Key, File file, TransferManager transferManager);

    /**
     * Downloads all objects in the virtual directory designated by the key prefix given to the destination directory given.
     *
     * @param s3BucketName the S3 bucket name
     * @param s3KeyPrefix the S3 key prefix
     * @param destinationDirectory the destination directory
     * @param transferManager the transfer manager implementation to use
     *
     * @return the object that represents an asynchronous multiple file download of an entire virtual directory
     */
    public MultipleFileDownload downloadDirectory(String s3BucketName, String s3KeyPrefix, File destinationDirectory, TransferManager transferManager);

    /**
     * Returns a pre-signed URL for accessing an Amazon S3 resource.
     *
     * @param generatePresignedUrlRequest the request object containing all the options for generating a pre-signed URL (bucket name, key, expiration date,
     * etc)
     * @param s3Client the {@link AmazonS3} implementation to use
     *
     * @return the pre-signed URL which expires at the specified time, and can be used to allow anyone to download the specified object from S3, without
     * exposing the owner's AWS secret access key
     */
    public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest, AmazonS3 s3Client);

    /**
     * Gets the metadata for the specified Amazon S3 object without actually fetching the object itself.
     *
     * @param s3BucketName the S3 bucket name
     * @param s3Key the S3 key
     * @param s3Client the {@link AmazonS3} implementation to use
     *
     * @return the Amazon S3 object metadata for the specified object
     */
    public ObjectMetadata getObjectMetadata(String s3BucketName, String s3Key, AmazonS3 s3Client);

    /**
     * Returns all S3 object tags for the specified object.
     *
     * @param getObjectTaggingRequest the request object containing all the options on how to retrieve the Amazon S3 object tags
     * @param s3Client the {@link AmazonS3} implementation to use
     *
     * @return the set of S3 object tags
     */
    public GetObjectTaggingResult getObjectTagging(GetObjectTaggingRequest getObjectTaggingRequest, AmazonS3 s3Client);

    /**
     * Gets the object stored in Amazon S3 under the specified bucket and key.
     *
     * @param getObjectRequest the request object containing all the options for downloading an Amazon S3 object
     * @param s3Client the {@link AmazonS3} implementation to use
     *
     * @return the object stored in Amazon S3
     */
    public S3Object getS3Object(GetObjectRequest getObjectRequest, AmazonS3 s3Client);

    /**
     * Lists in-progress multipart uploads. An in-progress multipart upload is a multipart upload that has been initiated, but has not yet been completed or
     * aborted. This operation returns at most 1,000 multipart uploads in the response by default.
     *
     * @param listMultipartUploadsRequest the request object that specifies all the parameters of this operation
     * @param s3Client the {@link AmazonS3} implementation to use
     *
     * @return the listing of multipart uploads from Amazon S3
     */
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest listMultipartUploadsRequest, AmazonS3 s3Client);

    /**
     * Returns a list of summary information about the objects in the specified bucket.
     *
     * @param listObjectsRequest the request object containing all options for listing the objects in a specified bucket
     * @param s3Client the {@link AmazonS3} implementation to use
     *
     * @return the listing of the objects in the specified bucket
     */
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest, AmazonS3 s3Client);

    /**
     * Returns a list of summary information about the versions in the specified bucket.
     *
     * @param listVersionsRequest the request object containing all options for listing the versions in a specified bucket
     * @param s3Client the {@link AmazonS3} implementation to use
     *
     * @return the listing of the versions in the specified bucket
     */
    public VersionListing listVersions(ListVersionsRequest listVersionsRequest, AmazonS3 s3Client);

    /**
     * Uploads a new object to the specified Amazon S3 bucket.
     *
     * @param putObjectRequest the request object containing all the parameters to upload a new object to Amazon S3
     * @param s3Client the {@link AmazonS3} implementation to use
     *
     * @return the object containing the information returned by Amazon S3 for the newly created object
     */
    public PutObjectResult putObject(PutObjectRequest putObjectRequest, AmazonS3 s3Client);

    /**
     * Requests to restore an object, which was transitioned to Amazon Glacier from Amazon S3 when it was expired, into Amazon S3 again.
     *
     * @param requestRestore the request object containing all the options for restoring an object
     * @param s3Client the {@link AmazonS3} implementation to use
     */
    public void restoreObject(RestoreObjectRequest requestRestore, AmazonS3 s3Client);

    /**
     * Rolls back any updates to S3 within a given session. Whether rollback is supported or not is implementation dependent.
     */
    public void rollback();

    /**
     * Set the tags for the specified object.
     *
     * @param setObjectTaggingRequest the request object containing all the options for setting the tags for the specified object
     * @param s3Client the {@link AmazonS3} implementation to use
     *
     * @return the result of the set operation
     */
    public SetObjectTaggingResult setObjectTagging(SetObjectTaggingRequest setObjectTaggingRequest, AmazonS3 s3Client);

    /**
     * Schedules a new transfer to upload data to Amazon S3.
     *
     * @param putObjectRequest the request containing all the parameters for the upload
     * @param transferManager the transfer manager implementation to use
     *
     * @return the object to use to check the state of the upload, listen for progress notifications, and otherwise manage the upload
     */
    public Upload upload(PutObjectRequest putObjectRequest, TransferManager transferManager);

    /**
     * Uploads all files in the directory given to the bucket named, optionally recursing for all subdirectories.
     *
     * @param s3BucketName the S3 bucket name
     * @param virtualDirectoryKeyPrefix the key prefix of the virtual directory to upload to
     * @param directory the directory to upload
     * @param includeSubdirectories specified whether to include subdirectories in the upload. If true, files found in subdirectories will be included with an
     * appropriate concatenation to the key prefix
     * @param metadataProvider the callback of type <code>ObjectMetadataProvider</code> which is used to provide metadata for each file being uploaded
     * @param transferManager the transfer manager implementation to use
     *
     * @return the multiple file upload information
     */
    public MultipleFileUpload uploadDirectory(String s3BucketName, String virtualDirectoryKeyPrefix, File directory, boolean includeSubdirectories,
        ObjectMetadataProvider metadataProvider, TransferManager transferManager);

    /**
     * Uploads all specified files to the bucket named, constructing relative keys depending on the common parent directory given.
     *
     * @param s3BucketName the S3 bucket name
     * @param virtualDirectoryKeyPrefix the key prefix of the virtual directory to upload to
     * @param directory the common parent directory of files to upload. The keys of the files in the list of files are constructed relative to this directory
     * and the virtualDirectoryKeyPrefix
     * @param files the list of files to upload. The keys of the files are calculated relative to the common parent directory and the virtualDirectoryKeyPrefix
     * @param metadataProvider the callback of type <code>ObjectMetadataProvider</code> which is used to provide metadata for each file being uploaded
     * @param transferManager the transfer manager implementation to use
     *
     * @return the multiple file upload information
     */
    public MultipleFileUpload uploadFileList(String s3BucketName, String virtualDirectoryKeyPrefix, File directory, List<File> files,
        ObjectMetadataProvider metadataProvider, TransferManager transferManager);

    /**
     * This action creates a S3 Batch Operations job.
     *
     * @param createJobRequest the request object containing all the options for creating the S3 batch job
     * @param s3ControlClient service client for accessing AWS S3 Control
     *
     * @return container object that contains information of the created job
     */
    public CreateJobResult createBatchJob(CreateJobRequest createJobRequest, AWSS3Control s3ControlClient);

    /**
     * This action retrieves current state and settings of the S3 Batcj Job
     *
     * @param request the request object containing all the options for getting the S3 batch job description
     * @param s3ControlClient service client for accessing AWS S3 Control
     *
     * @return container object that contains settings and current state of the created job
     */
    public DescribeJobResult describeBatchJob(DescribeJobRequest request, AWSS3Control s3ControlClient);


}
