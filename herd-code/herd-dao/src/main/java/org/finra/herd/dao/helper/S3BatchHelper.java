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
package org.finra.herd.dao.helper;

import java.io.File;
import java.util.Collection;

import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3control.model.CreateJobRequest;
import com.amazonaws.services.s3control.model.DescribeJobRequest;
import com.amazonaws.services.s3control.model.JobManifest;
import com.amazonaws.services.s3control.model.JobManifestLocation;
import com.amazonaws.services.s3control.model.JobManifestSpec;
import com.amazonaws.services.s3control.model.JobOperation;
import com.amazonaws.services.s3control.model.JobReport;
import com.amazonaws.services.s3control.model.S3GlacierJobTier;
import com.amazonaws.services.s3control.model.S3InitiateRestoreObjectOperation;
import com.amazonaws.services.s3control.model.S3SetObjectTaggingOperation;
import com.amazonaws.services.s3control.model.S3Tag;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.S3BatchManifest;
import org.finra.herd.model.dto.BatchJobConfigDto;
import org.finra.herd.model.dto.BatchJobManifestDto;

@Component
public class S3BatchHelper
{
    /**
     * Create container object for S3 batch job manifest
     *
     * @param jobId Unique batch job id
     * @param bucketName The bucket name where target files located
     * @param files The list of files to process by batch job
     * @param batchJobConfig The batch job configuration parameters
     *
     * @return container object with job settings and manifest content
     */
    public BatchJobManifestDto createCSVBucketKeyManifest(String jobId, String bucketName, Collection<File> files, BatchJobConfigDto batchJobConfig)
    {
        // Get bucket name and location prefix from batch job configuration object
        String manifestBucketName = batchJobConfig.getManifestS3BucketName();
        String manifestLocation = batchJobConfig.getManifestS3Prefix();

        // Build manifest content as a 2 columns comma separated file, where each line looks like "bucket name, file path"
        StringBuilder manifestContentBuilder = new StringBuilder();
        files.stream().map(file -> file.getPath().replaceAll("\\\\", "/")).map(path -> String.format("%s,%s%n", bucketName, path))
            .forEach(manifestContentBuilder::append);
        String content = manifestContentBuilder.toString();

        // Build manifest dto object to pass manifest content and configuration for further processing
        S3BatchManifest manifest = new S3BatchManifest();
        BatchJobManifestDto.Builder<Void> builder = BatchJobManifestDto.builder()
            // S3 key of the manifest file
            .withKey(String.format("%s/%s.csv", manifestLocation, jobId))
            // Bucket name where manifest will be stored
            .withBucketName(manifestBucketName)
            // The manifest format, indicates which of the available formats the specified manifest uses
            // Valid Values: S3BatchOperations_CSV_20180820 | S3InventoryReport_CSV_20161130
            .withFormat("S3BatchOperations_CSV_20180820")
            // If the specified manifest object is in the S3BatchOperations_CSV_20180820 format, this element describes which columns contain the required data.
            // Valid Values: Ignore | Bucket | Key | VersionId
            .withFields("Bucket", "Key")
            // Manifest file content
            .withContent(content)
            // md5 checksum of the content of the manifest file
            .withEtag(DigestUtils.md5Hex(content));

        return builder.build();
    }

    /**
     * Create container object for S3 batch job manifest
     *
     * @param jobId Unique batch job id
     * @param bucketName The bucket name where target files located
     * @param objectVersions The collection of S3 objects version information to include in the manifest
     * @param batchJobConfig The batch job configuration parameters
     *
     * @return container object with job settings and manifest content
     */
    public BatchJobManifestDto createCSVBucketKeyVersionManifest(String jobId, String bucketName, Collection<S3VersionSummary> objectVersions,
        BatchJobConfigDto batchJobConfig)
    {
        // Get bucket name and location prefix from batch job configuration object
        String manifestBucketName = batchJobConfig.getManifestS3BucketName();
        String manifestLocation = batchJobConfig.getManifestS3Prefix();

        // Build manifest content as a 3 columns comma separated file, where each line looks like "bucket name, S3 object key, S3 object version"
        StringBuilder manifestContentBuilder = new StringBuilder();
        objectVersions.stream().map(file -> String.format("%s,%s,%s%n", bucketName, file.getKey(), file.getVersionId()))
            .forEach(manifestContentBuilder::append);
        String content = manifestContentBuilder.toString();

        // Build manifest dto object to pass manifest content and configuration for further processing
        //S3BatchManifest manifest = new S3BatchManifest();
        BatchJobManifestDto.Builder<Void> builder = BatchJobManifestDto.builder()
            // S3 key of the manifest file
            .withKey(String.format("%s/%s.csv", manifestLocation, jobId))
            // Bucket name where manifest will be stored
            .withBucketName(manifestBucketName)
            // The manifest format, indicates which of the available formats the specified manifest uses
            // Valid Values: S3BatchOperations_CSV_20180820 | S3InventoryReport_CSV_20161130
            .withFormat("S3BatchOperations_CSV_20180820")
            // If the specified manifest object is in the S3BatchOperations_CSV_20180820 format, this element describes which columns contain the required data.
            // Valid Values: Ignore | Bucket | Key | VersionId
            .withFields("Bucket", "Key", "VersionId")
            // Manifest file content
            .withContent(content)
            // md5 checksum of the content of the manifest file
            .withEtag(DigestUtils.md5Hex(content));

        return builder.build();
    }

    /**
     * Generate AWS SDK CreateJobRequest object to restore files using S3 Batch operation
     *
     * @param manifest the container object containing configuration parameters and content for the manifest.
     * @param jobId herd generated job id
     * @param expirationInDays This argument specifies how long the S3 Glacier or S3 Glacier Deep Archive object remains available in Amazon S3. S3 Initiate
     * Restore Object jobs that target S3 Glacier and S3 Glacier Deep Archive objects require ExpirationInDays set to 1 or greater.
     * @param archiveRetrievalOption the archive retrieval option when restoring an archived object. For Batch Restore only 2 options are valid at the moment:
     * BULK and STANDARD (case-sensitive). If not provided BULK option used by default.
     * @param batchJobConfig The batch job configuration parameters
     *
     * @return container object which contains the configuration parameters for an S3 Initiate Restore Object job.
     */
    public CreateJobRequest generateCreateRestoreJobRequest(BatchJobManifestDto manifest, String jobId, int expirationInDays, String archiveRetrievalOption,
        BatchJobConfigDto batchJobConfig)
    {
        // Create object to describe the batch restore operation in S3. The Restore operation initiates restore requests for archived objects on a list of
        // Amazon S3 objects that you specify.
        JobOperation jobOperation = new JobOperation().withS3InitiateRestoreObject(new S3InitiateRestoreObjectOperation().withExpirationInDays(expirationInDays)
            .withGlacierJobTier(StringUtils.isNotEmpty(archiveRetrievalOption) ? archiveRetrievalOption : S3GlacierJobTier.BULK.toString()));

        // Build manifest file s3 ARN
        String manifestLocationArn = String.format("arn:aws:s3:::%s/%s", manifest.getBucketName(), manifest.getKey());

        // Build JobManifest object, which contains the configuration information for a batch job's manifest.
        JobManifest jobManifest = new JobManifest().withSpec(new JobManifestSpec().withFormat(manifest.getFormat()).withFields(manifest.getFields()))
            .withLocation(new JobManifestLocation().withObjectArn(manifestLocationArn).withETag(manifest.getEtag()));

        // Build JobReport object, which indicates there is no need to create separate job report file on S3
        JobReport jobReport = new JobReport().withEnabled(false);

        // Build the request object
        return new CreateJobRequest().withAccountId(batchJobConfig.getAwsAccountId()).withOperation(jobOperation).withManifest(jobManifest)
            .withReport(jobReport).withPriority(10).withRoleArn(batchJobConfig.getS3BatchRoleArn()).withClientRequestToken(jobId)
            .withDescription(String.format("Restore batch job %s", jobId)).withConfirmationRequired(false);
    }

    /**
     * Generate AWS SDK CreateJobRequest object to restore files using S3 Batch operation
     *
     * @param manifest the container object containing configuration parameters and content for the manifest.
     * @param jobId herd generated job id
     * @param batchJobConfig The batch job configuration parameters
     * @param tag the S3 object tag
     *
     * @return container object which contains the configuration parameters for an S3 Initiate Restore Object job.
     */
    public CreateJobRequest generateCreatePutObjectTaggingJobRequest(BatchJobManifestDto manifest, String jobId, final BatchJobConfigDto batchJobConfig,
        final Tag tag)
    {
        // Create object to describe the batch restore operation in S3. The Restore operation initiates restore requests for archived objects on a list of
        // Amazon S3 objects that you specify.
        JobOperation jobOperation = new JobOperation().withS3PutObjectTagging(
            new S3SetObjectTaggingOperation().withTagSet(new S3Tag().withKey(tag.getKey()).withValue(tag.getValue())));

        // Build manifest file s3 ARN
        String manifestLocationArn = String.format("arn:aws:s3:::%s/%s", manifest.getBucketName(), manifest.getKey());

        // Build JobManifest object, which contains the configuration information for a batch job's manifest.
        JobManifest jobManifest = new JobManifest().withSpec(new JobManifestSpec().withFormat(manifest.getFormat()).withFields(manifest.getFields()))
            .withLocation(new JobManifestLocation().withObjectArn(manifestLocationArn).withETag(manifest.getEtag()));

        // Build JobReport object, which indicates there is no need to create separate job report file on S3
        JobReport jobReport = new JobReport().withEnabled(false);

        // Build the request object
        return new CreateJobRequest().withAccountId(batchJobConfig.getAwsAccountId()).withOperation(jobOperation).withManifest(jobManifest)
            .withReport(jobReport).withPriority(10).withRoleArn(batchJobConfig.getS3BatchRoleArn()).withClientRequestToken(jobId)
            .withDescription(String.format("Restore batch job %s", jobId)).withConfirmationRequired(false);
    }

    /**
     * Generate request object to retrieve the configuration parameters and status for a Batch Operations job.
     *
     * @param jobId the identifier of the specific batch job
     * @param batchJobConfig The batch job configuration parameters
     *
     * @return The container object which contains the configuration parameters for an S3 Describe Job call.
     */
    public DescribeJobRequest generateDescribeJobRequest(String jobId, BatchJobConfigDto batchJobConfig)
    {
        // Build DescribeJobRequest object using provided parameters
        return new DescribeJobRequest().withAccountId(batchJobConfig.getAwsAccountId()).withJobId(jobId);
    }
}
