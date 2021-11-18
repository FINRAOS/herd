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

import org.finra.herd.dao.S3Operations;

public class S3OperationsImpl implements S3Operations
{
    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest abortMultipartUploadRequest, AmazonS3 s3Client)
    {
        s3Client.abortMultipartUpload(abortMultipartUploadRequest);
    }

    @Override
    public Copy copyFile(CopyObjectRequest copyObjectRequest, TransferManager transferManager)
    {
        return transferManager.copy(copyObjectRequest);
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest, AmazonS3 s3Client)
    {
        return s3Client.deleteObjects(deleteObjectsRequest);
    }

    @Override
    public Download download(String s3BucketName, String s3Key, File file, TransferManager transferManager)
    {
        return transferManager.download(s3BucketName, s3Key, file);
    }

    @Override
    public MultipleFileDownload downloadDirectory(String s3BucketName, String s3KeyPrefix, File destinationDirectory, TransferManager transferManager)
    {
        return transferManager.downloadDirectory(s3BucketName, s3KeyPrefix, destinationDirectory);
    }

    @Override
    public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest, AmazonS3 s3Client)
    {
        return s3Client.generatePresignedUrl(generatePresignedUrlRequest);
    }

    @Override
    public ObjectMetadata getObjectMetadata(String s3BucketName, String s3Key, AmazonS3 s3Client)
    {
        return s3Client.getObjectMetadata(s3BucketName, s3Key);
    }

    @Override
    public GetObjectTaggingResult getObjectTagging(GetObjectTaggingRequest getObjectTaggingRequest, AmazonS3 s3Client)
    {
        return s3Client.getObjectTagging(getObjectTaggingRequest);
    }

    @Override
    public S3Object getS3Object(GetObjectRequest getObjectRequest, AmazonS3 s3)
    {
        return s3.getObject(getObjectRequest);
    }

    @Override
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest listMultipartUploadsRequest, AmazonS3 s3Client)
    {
        return s3Client.listMultipartUploads(listMultipartUploadsRequest);
    }

    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest, AmazonS3 s3Client)
    {
        return s3Client.listObjects(listObjectsRequest);
    }

    @Override
    public VersionListing listVersions(ListVersionsRequest listVersionsRequest, AmazonS3 s3Client)
    {
        return s3Client.listVersions(listVersionsRequest);
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest, AmazonS3 s3Client)
    {
        return s3Client.putObject(putObjectRequest);
    }

    @Override
    public void restoreObject(RestoreObjectRequest requestRestore, AmazonS3 s3Client)
    {
        s3Client.restoreObject(requestRestore);
    }

    @Override
    public void rollback()
    {
        // Rollback not supported
    }

    @Override
    public SetObjectTaggingResult setObjectTagging(SetObjectTaggingRequest setObjectTaggingRequest, AmazonS3 s3Client)
    {
        return s3Client.setObjectTagging(setObjectTaggingRequest);
    }

    @Override
    public Upload upload(PutObjectRequest putObjectRequest, TransferManager transferManager)
    {
        return transferManager.upload(putObjectRequest);
    }

    @Override
    public MultipleFileUpload uploadDirectory(String s3BucketName, String virtualDirectoryKeyPrefix, File directory, boolean includeSubdirectories,
        ObjectMetadataProvider metadataProvider, TransferManager transferManager)
    {
        return transferManager.uploadDirectory(s3BucketName, virtualDirectoryKeyPrefix, directory, includeSubdirectories, metadataProvider);
    }

    @Override
    public MultipleFileUpload uploadFileList(String s3BucketName, String virtualDirectoryKeyPrefix, File directory, List<File> files,
        ObjectMetadataProvider metadataProvider, TransferManager transferManager)
    {
        return transferManager.uploadFileList(s3BucketName, virtualDirectoryKeyPrefix, directory, files, metadataProvider);
    }

    @Override
    public CreateJobResult createBatchJob(CreateJobRequest createJobRequest, AWSS3Control s3ControlClient)
    {
        return s3ControlClient.createJob(createJobRequest);
    }

    @Override
    public DescribeJobResult describeBatchJob(DescribeJobRequest request, AWSS3Control s3ControlClient)
    {
        return s3ControlClient.describeJob(request);
    }

}
