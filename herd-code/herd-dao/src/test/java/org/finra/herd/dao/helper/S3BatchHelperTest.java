package org.finra.herd.dao.helper;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import com.amazonaws.services.s3control.model.CreateJobRequest;
import com.amazonaws.services.s3control.model.JobManifestFormat;
import com.amazonaws.services.s3control.model.S3GlacierJobTier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.model.dto.BatchJobConfigDto;
import org.finra.herd.model.dto.BatchJobManifestDto;

public class S3BatchHelperTest extends AbstractDaoTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    protected String AWS_ACCOUNT_ID = "123456789012";
    protected String S3_BATCH_ROLE_ARN = "arn:aws:iam::123456789012:role/S3_BATCH_ROLE";
    protected String S3_BATCH_MANIFEST_BUCKET_NAME = "1234-5678-9012-batch-manifest-bucket";
    protected String S3_BATCH_MANIFEST_LOCATION_PREFIX = "sub/batch";
    protected Integer S3_BATCH_RESTORE_MAX_ATTEMPTS = 5;
    protected Integer S3_BATCH_RESTORE_BACKOFF_PERIOD = 2000;
    protected String TEST_JOB_ID = UUID.randomUUID().toString();
    protected String TEST_BUCKET_NAME = "1234-5678-9012-batch-job-bucket";
    protected List<String> defaultFileNames = Arrays.asList("testFile1.txt", "testFile2.txt");
    protected Integer expirationInDays = 555;
    protected String archiveRetrievalOption = S3GlacierJobTier.BULK.toString();
    protected String manifestFormat = JobManifestFormat.S3BatchOperations_CSV_20180820.toString();
    protected String manifestContent = "default manifest content";
    protected String manifestEtag = "test_manifest_etag";
    protected String[] manifestFields = new String[] {"test_field_1", "test_field_2"};
    @InjectMocks
    private S3BatchHelper s3BatchHelper;

    private BatchJobConfigDto getDefaultBatchJobDataDto()
    {
        return new BatchJobConfigDto(AWS_ACCOUNT_ID, S3_BATCH_ROLE_ARN, S3_BATCH_MANIFEST_BUCKET_NAME, S3_BATCH_MANIFEST_LOCATION_PREFIX,
            S3_BATCH_RESTORE_MAX_ATTEMPTS, S3_BATCH_RESTORE_BACKOFF_PERIOD);
    }

    private BatchJobManifestDto getDefaultBatchJobManifestDto()
    {
        return BatchJobManifestDto.builder().withKey(String.format("%s/%s.csv", S3_BATCH_MANIFEST_LOCATION_PREFIX, TEST_JOB_ID))
            .withBucketName(S3_BATCH_MANIFEST_BUCKET_NAME).withFormat(manifestFormat).withContent(manifestContent).withFields(manifestFields)
            .withEtag(manifestEtag).build();
    }

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateCSVBucketKeyManifest() throws IOException
    {
        List<File> files = createTemporaryFiles(defaultFileNames);

        BatchJobManifestDto manifest = s3BatchHelper.createCSVBucketKeyManifest(TEST_JOB_ID, TEST_BUCKET_NAME, files, getDefaultBatchJobDataDto());

        assertNotNull(manifest);
        assertEquals("S3BatchOperations_CSV_20180820", manifest.getFormat());
        assertEquals(2, manifest.getFields().size());
        assertEquals("Bucket", manifest.getFields().get(0));
        assertEquals("Key", manifest.getFields().get(1));
        assertNotNull(manifest.getContent());
        assertNotNull(manifest.getEtag());

        String[] lines = manifest.getContent().split("\\R");
        assertEquals(files.size(), lines.length);
        for (int i = 0; i < files.size(); i++)
        {
            String expectedValue = String.format("%s,%s", TEST_BUCKET_NAME, files.get(i).getAbsolutePath());
            assertEquals(expectedValue, lines[i]);
        }
    }

    @Test
    public void testGenerateCreateRestoreJobRequest()
    {
        BatchJobManifestDto manifest = getDefaultBatchJobManifestDto();

        // Execute target method
        CreateJobRequest request =
            s3BatchHelper.generateCreateRestoreJobRequest(manifest, TEST_JOB_ID, expirationInDays, archiveRetrievalOption, getDefaultBatchJobDataDto());

        // Check the result
        assertNotNull(request);
        assertNotNull(request.getPriority());
        assertNotNull(request.getDescription());
        assertNotNull(request.getOperation());
        assertNotNull(request.getManifest());
        assertNotNull(request.getReport());
        assertNotNull(request.getOperation().getS3InitiateRestoreObject());
        assertNotNull(request.getManifest().getSpec());
        assertNotNull(request.getManifest().getLocation());

        assertEquals(AWS_ACCOUNT_ID, request.getAccountId());
        assertEquals(S3_BATCH_ROLE_ARN, request.getRoleArn());
        assertEquals(TEST_JOB_ID, request.getClientRequestToken());
        assertFalse(request.getConfirmationRequired());

        assertEquals(expirationInDays, request.getOperation().getS3InitiateRestoreObject().getExpirationInDays());
        assertEquals(archiveRetrievalOption, request.getOperation().getS3InitiateRestoreObject().getGlacierJobTier());

        assertEquals(manifestFormat, request.getManifest().getSpec().getFormat());
        assertArrayEquals(manifestFields, request.getManifest().getSpec().getFields().toArray());

        String expectedLocation = String.format("arn:aws:s3:::%s/%s/%s.csv", S3_BATCH_MANIFEST_BUCKET_NAME, S3_BATCH_MANIFEST_LOCATION_PREFIX, TEST_JOB_ID);
        assertEquals(expectedLocation, request.getManifest().getLocation().getObjectArn());

        assertEquals(manifestEtag, request.getManifest().getLocation().getETag());

        assertFalse(request.getReport().getEnabled());
    }

    @Test
    public void testGenerateCreateRestoreJobRequestUseBulkByDefault()
    {
        BatchJobManifestDto manifest = getDefaultBatchJobManifestDto();

        CreateJobRequest request = s3BatchHelper.generateCreateRestoreJobRequest(manifest, TEST_JOB_ID, expirationInDays, null, getDefaultBatchJobDataDto());

        assertEquals(archiveRetrievalOption, request.getOperation().getS3InitiateRestoreObject().getGlacierJobTier());
    }

    private List<File> createTemporaryFiles(Collection<String> fileNames) throws IOException
    {
        List<File> files = new ArrayList<>();
        try
        {
            for (String fileName : fileNames)
            {
                files.add(folder.newFile(fileName));
            }
        }
        catch (IOException ioe)
        {
            System.err.println("Error creating temporary test file in " + this.getClass().getSimpleName());
            throw ioe;
        }
        return files;
    }
}
