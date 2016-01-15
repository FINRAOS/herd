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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

/**
 * AWS S3 Operations Service.
 */
public interface S3Operations
{
    public static final String ERROR_CODE_NO_SUCH_BUCKET = "NoSuchBucket";
    public static final String ERROR_CODE_NO_SUCH_KEY = "NoSuchKey";
    public static final String ERROR_CODE_ACCESS_DENIED = "AccessDenied";
    public static final String ERROR_CODE_INTERNAL_ERROR = "InternalError";

    /**
     * Get the S3 object meta data.
     */
    public ObjectMetadata getObjectMetadata(String sourceBucketName, String filePath, AmazonS3Client s3Client);

    /**
     * Copy the S3 object from source to target S3 bucket.
     */
    public Copy copyFile(CopyObjectRequest copyObjectRequest, TransferManager transferManager);

    /**
     * Deletes the S3 file.
     */
    public void deleteFile(String bucketName, String key, AmazonS3Client s3Client);

    /**
     * Lists multipart uploads.
     */
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest listMultipartUploadsRequest, AmazonS3Client s3Client);

    /**
     * Aborts the specified multipart upload.
     */
    public void abortMultipartUpload(AbortMultipartUploadRequest abortMultipartUploadRequest, AmazonS3Client s3Client);

    /**
     * Delete the objects.
     */
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectRequest, AmazonS3Client s3Client);

    /**
     * List the objects.
     *
     * @return object listing.
     */
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest, AmazonS3Client s3Client);

    /**
     * Put an object.
     */
    public PutObjectResult putObject(PutObjectRequest putObjectRequest, AmazonS3Client s3Client);

    /**
     * Uploads a directory.
     *
     * @param bucketName the bucket name.
     * @param virtualDirectoryKeyPrefix the virtual directory key prefix.
     * @param directory the directory.
     * @param includeSubdirectories flag that determines whether to include sub-directories or not.
     * @param metadataProvider the metadata provider.
     * @param transferManager the transfer manager.
     *
     * @return the multiple file upload information.
     */
    public MultipleFileUpload uploadDirectory(String bucketName, String virtualDirectoryKeyPrefix, File directory, boolean includeSubdirectories,
        ObjectMetadataProvider metadataProvider, TransferManager transferManager);

    public MultipleFileUpload uploadFileList(String bucketName, String virtualDirectoryKeyPrefix, File directory, List<File> files,
        ObjectMetadataProvider metadataProvider, TransferManager transferManager);

    /**
     * Downloads a directory.
     *
     * @param bucketName the bucket name.
     * @param keyPrefix the S3 key prefix.
     * @param destinationDirectory the destination directory.
     * @param transferManager the transfer manager.
     *
     * @return the multiple file download information.
     */
    public MultipleFileDownload downloadDirectory(String bucketName, String keyPrefix, File destinationDirectory, TransferManager transferManager);

    /**
     * Uploads a single file.
     *
     * @param putObjectRequest the put object request.
     * @param transferManager the transfer manager.
     *
     * @return the upload information.
     * @throws AmazonServiceException if an Amazon service exception occurs.
     * @throws AmazonClientException if an Amazon client exception occurs.
     */
    public Upload upload(PutObjectRequest putObjectRequest, TransferManager transferManager) throws AmazonServiceException, AmazonClientException;

    public Download download(String bucket, String key, File file, TransferManager transferManager);

    /**
     * Rolls back any updates to S3 within a given session. Whether rollback is supported or not is implementation dependent.
     */
    public void rollback();

    public S3Object getS3Object(GetObjectRequest getObjectRequest, AmazonS3 s3);

    /**
     * {@link AmazonS3#generatePresignedUrl(GeneratePresignedUrlRequest)}
     *
     * @param generatePresignedUrlRequest The request object containing all the options for generating a pre-signed URL (bucket name, key, expiration date,
     * etc).
     * @param s3 The {@link AmazonS3} implementation to use
     *
     * @return A pre-signed URL that can be used to access an Amazon S3 resource without requiring the user of the URL to know the account's AWS security
     *         credentials.
     */
    public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest, AmazonS3 s3);
}