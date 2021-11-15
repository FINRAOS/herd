package org.finra.herd.dao.helper;

import java.io.File;
import java.util.Collection;

import com.amazonaws.services.s3control.model.CreateJobRequest;
import com.amazonaws.services.s3control.model.JobManifest;
import com.amazonaws.services.s3control.model.JobManifestLocation;
import com.amazonaws.services.s3control.model.JobManifestSpec;
import com.amazonaws.services.s3control.model.JobOperation;
import com.amazonaws.services.s3control.model.JobReport;
import com.amazonaws.services.s3control.model.S3GlacierJobTier;
import com.amazonaws.services.s3control.model.S3InitiateRestoreObjectOperation;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.S3BatchManifest;
import org.finra.herd.model.dto.ConfigurationValue;

@Component
public class S3BatchHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(org.finra.herd.dao.helper.S3BatchHelper.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    public S3BatchManifest createCSVBucketKeyManifest(String jobId, String bucketName, Collection<File> files)
    {
        if (bucketName == null || bucketName.isEmpty()) return null;
        if (files == null || files.isEmpty()) return null;

        // Create manifest content
        StringBuilder manifestContentBuilder = new StringBuilder();
        files.stream()
            .map(file -> file.getPath().replaceAll("\\\\", "/"))
            .map(path -> String.format("%s,%s%n", bucketName, path))
            .forEach(manifestContentBuilder::append);

        S3BatchManifest manifest = new S3BatchManifest();
        manifest.setFilename(String.format("%s.csv", jobId));
        manifest.setArn(String.format("arn:aws:s3:::%s/%s", bucketName, manifest.getFilename()));
        manifest.setFormat("S3BatchOperations_CSV_20180820");
        manifest.setFields(new String[]{ "Bucket", "Key" });
        manifest.setContent(manifestContentBuilder.toString());

        LOGGER.debug("restoreJobId=\"{}\" manifest: {}", jobId, manifest.getETag());

        return manifest;
    }

    public CreateJobRequest generateCreateRestoreJobRequest(S3BatchManifest manifest, String jobId, int expirationInDays, String archiveRetrievalOption)
    {
        String account = configurationHelper.getPropertyAsString(ConfigurationValue.AWS_ACCOUNT_ID);
        String batchRole = configurationHelper.getPropertyAsString(ConfigurationValue.S3_BATCH_ROLE_ARN);

        JobOperation jobOperation = new JobOperation()
            .withS3InitiateRestoreObject(new S3InitiateRestoreObjectOperation()
                .withExpirationInDays(expirationInDays)
                .withGlacierJobTier(StringUtils.isNotEmpty(archiveRetrievalOption) ? archiveRetrievalOption : S3GlacierJobTier.BULK.toString()));

        JobManifest jobManifest = new JobManifest()
            .withSpec(new JobManifestSpec()
                .withFormat(manifest.getFormat())
                .withFields(manifest.getFields()))
            .withLocation(new JobManifestLocation()
                .withObjectArn(manifest.getArn())
                .withETag(manifest.getETag()));

        JobReport jobReport = new JobReport()
            .withEnabled(false);

        CreateJobRequest createRestoreJobRequest = new CreateJobRequest()
            .withAccountId(account)
            .withOperation(jobOperation)
            .withManifest(jobManifest)
            .withReport(jobReport)
            .withPriority(10)
            .withRoleArn(batchRole)
            .withClientRequestToken(jobId)
            .withDescription(String.format("Restore batch job %s", jobId))
            .withConfirmationRequired(false);

        LOGGER.info("Create restore job request: {}", createRestoreJobRequest.toString());

        return createRestoreJobRequest;
    }
}
