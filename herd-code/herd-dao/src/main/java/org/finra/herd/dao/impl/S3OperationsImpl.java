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

import org.finra.herd.dao.S3Operations;

public class S3OperationsImpl implements S3Operations
{
    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectMetadata getObjectMetadata(String sourceBucketName, String filePath, AmazonS3Client s3Client)
    {
        return s3Client.getObjectMetadata(sourceBucketName, filePath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Copy copyFile(CopyObjectRequest copyObjectRequest, TransferManager transferManager)
    {
        return transferManager.copy(copyObjectRequest);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteFile(String bucketName, String key, AmazonS3Client s3Client)
    {
        s3Client.deleteObject(bucketName, key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest listMultipartUploadsRequest, AmazonS3Client s3Client)
    {
        return s3Client.listMultipartUploads(listMultipartUploadsRequest);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest abortMultipartUploadRequest, AmazonS3Client s3Client)
    {
        s3Client.abortMultipartUpload(abortMultipartUploadRequest);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectRequest, AmazonS3Client s3Client)
    {
        return s3Client.deleteObjects(deleteObjectRequest);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest, AmazonS3Client s3Client)
    {
        return s3Client.listObjects(listObjectsRequest);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest, AmazonS3Client s3Client)
    {
        return s3Client.putObject(putObjectRequest);
    }

    /**
     * Implementation delegates to {@link TransferManager#uploadDirectory(String, String, File, boolean, ObjectMetadataProvider)}
     */
    @Override
    public MultipleFileUpload uploadDirectory(String bucketName, String virtualDirectoryKeyPrefix, File directory, boolean includeSubdirectories,
        ObjectMetadataProvider metadataProvider, TransferManager transferManager)
    {
        return transferManager.uploadDirectory(bucketName, virtualDirectoryKeyPrefix, directory, includeSubdirectories, metadataProvider);
    }

    /**
     * Implementation delegates to {@link TransferManager#downloadDirectory(String, String, File)}.
     */
    @Override
    public MultipleFileDownload downloadDirectory(String bucketName, String keyPrefix, File destinationDirectory, TransferManager transferManager)
    {
        return transferManager.downloadDirectory(bucketName, keyPrefix, destinationDirectory);
    }

    /**
     * Implementation delegates to {@link TransferManager#upload(PutObjectRequest)}.
     */
    @Override
    public Upload upload(PutObjectRequest putObjectRequest, TransferManager transferManager) throws AmazonServiceException, AmazonClientException
    {
        return transferManager.upload(putObjectRequest);
    }

    /**
     * Implementation delegates to {@link TransferManager#uploadFileList(String, String, File, List, ObjectMetadataProvider)}.
     */
    @Override
    public MultipleFileUpload uploadFileList(String bucketName, String virtualDirectoryKeyPrefix, File directory, List<File> files,
        ObjectMetadataProvider metadataProvider, TransferManager transferManager)
    {
        return transferManager.uploadFileList(bucketName, virtualDirectoryKeyPrefix, directory, files, metadataProvider);
    }

    /**
     * Implementation delegates to {@link TransferManager#download(String, String, File)}
     */
    @Override
    public Download download(String bucket, String key, File file, TransferManager transferManager)
    {
        return transferManager.download(bucket, key, file);
    }

    /**
     * Rollback is not supported.
     */
    @Override
    public void rollback()
    {
        // Rollback not supported
    }

    @Override
    public S3Object getS3Object(GetObjectRequest getObjectRequest, AmazonS3 s3)
    {
        return s3.getObject(getObjectRequest);
    }

    @Override
    public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest, AmazonS3 s3)
    {
        return s3.generatePresignedUrl(generatePresignedUrlRequest);
    }
}
