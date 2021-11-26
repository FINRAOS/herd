package org.finra.herd.dao.helper;

import static org.finra.herd.model.dto.ConfigurationValue.S3_BATCH_MANIFEST_BUCKET_NAME;
import static org.finra.herd.model.dto.ConfigurationValue.S3_BATCH_MANIFEST_LOCATION;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3control.model.CreateJobRequest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.S3BatchManifest;
import org.finra.herd.model.dto.ConfigurationValue;

public class S3BatchHelperTest extends AbstractDaoTest
{
    @Mock
    private ConfigurationHelper configurationHelper;

    @InjectMocks
    private S3BatchHelper s3BatchHelper;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateCSVBucketKeyManifest()
    {
        List<File> files = new ArrayList<>();
        try {
            files.add(folder.newFile("testFile1.txt"));
            files.add(folder.newFile("testFile2.txt"));
        }
        catch( IOException ioe ) {
            System.err.println( "Error creating temporary test file in " + this.getClass().getSimpleName() );
        }

        when(configurationHelper.getProperty(S3_BATCH_MANIFEST_BUCKET_NAME, String.class)).thenReturn("testManifestBucket");
        when(configurationHelper.getProperty(S3_BATCH_MANIFEST_LOCATION, String.class)).thenReturn("testLocation");

        S3BatchManifest manifest = s3BatchHelper.createCSVBucketKeyManifest("testJobName", "testBucketName", files);

        assertNotNull(manifest);
        assertEquals("S3BatchOperations_CSV_20180820", manifest.getFormat());
        assertEquals(2, manifest.getFields().length);
        assertEquals("Bucket", manifest.getFields()[0]);
        assertEquals("Key", manifest.getFields()[1]);
        assertEquals("arn:aws:s3:::testManifestBucket/testLocation/testJobName.csv", manifest.getLocationArn());

        assertNotNull(manifest.getContent());
        assertNotNull(manifest.getETag());

        String[] lines = manifest.getContent().split("\\R");
        assertEquals(2, lines.length);
        assertTrue(lines[0].startsWith("testBucketName,"));
        assertTrue(lines[1].startsWith("testBucketName,"));
        assertTrue(lines[0].endsWith("testFile1.txt"));
        assertTrue(lines[1].endsWith("testFile2.txt"));
    }

    @Test
    public void testCreateCSVBucketKeyManifestNoLocation()
    {
        List<File> files = new ArrayList<>();
        try {
            files.add(folder.newFile("testFile1.txt"));
            files.add(folder.newFile("testFile2.txt"));
        }
        catch( IOException ioe ) {
            System.err.println( "Error creating temporary test file in " + this.getClass().getSimpleName() );
        }

        when(configurationHelper.getProperty(S3_BATCH_MANIFEST_BUCKET_NAME, String.class)).thenReturn("testManifestBucket");
        when(configurationHelper.getProperty(S3_BATCH_MANIFEST_LOCATION, String.class)).thenReturn(null);

        S3BatchManifest manifest = s3BatchHelper.createCSVBucketKeyManifest("testJobName", "testBucketName", files);

        assertNotNull(manifest);
        assertEquals("S3BatchOperations_CSV_20180820", manifest.getFormat());
        assertEquals(2, manifest.getFields().length);
        assertEquals("Bucket", manifest.getFields()[0]);
        assertEquals("Key", manifest.getFields()[1]);
        assertEquals("arn:aws:s3:::testManifestBucket/testJobName.csv", manifest.getLocationArn());

        assertNotNull(manifest.getContent());
        assertNotNull(manifest.getETag());

        String[] lines = manifest.getContent().split("\\R");
        assertEquals(2, lines.length);
        assertTrue(lines[0].startsWith("testBucketName,"));
        assertTrue(lines[1].startsWith("testBucketName,"));
        assertTrue(lines[0].endsWith("testFile1.txt"));
        assertTrue(lines[1].endsWith("testFile2.txt"));
    }


    @Test
    public void testCreateCSVBucketKeyManifestWithoutFiles()
    {
        List<File> files = new ArrayList<>();

        S3BatchManifest manifest = s3BatchHelper.createCSVBucketKeyManifest("testJobName", "testBucketName", files);

        assertNull(manifest);
    }

    @Test
    public void testCreateCSVBucketKeyManifestWithoutBucket()
    {
        List<File> files = new ArrayList<>();

        try {
            files.add(folder.newFile("testFile1.txt"));
            files.add(folder.newFile("testFile2.txt"));
        }
        catch( IOException ioe ) {
            System.err.println( "Error creating temporary test file in " + this.getClass().getSimpleName() );
        }

        S3BatchManifest manifest = s3BatchHelper.createCSVBucketKeyManifest("testJobName", null,  files);

        assertNull(manifest);
    }

    @Test
    public void testGenerateCreateRestoreJobRequest()
    {
        // Define test values
        String AWS_ACCOUNT_ID = "AWS_ACCOUNT_ID";
        String S3_BATCH_ROLE_ARN = "S3_BATCH_ROLE_ARN";
        String TEST_MANIFEST_FILE_NAME = "TEST_MANIFEST_FILE_NAME";
        String TEST_MANIFEST_ARN = "TEST_MANIFEST_ARN";
        String TEST_MANIFEST_FORMAT = "TEST_MANIFEST_FORMAT";
        String TEST_MANIFEST_CONTENT = "TEST_MANIFEST_CONTENT";
        String TEST_MANIFEST_ETAG = "TEST_MANIFEST_ETAG";
        String[] TEST_MANIFEST_FIELDS = new String[] {"TEST_MANIFEST_FIELD1", "TEST_MANIFEST_FIELD2"};
        String TEST_JOB_ID = "TEST_JOB_ID";
        String TEST_ARCHIVE_RETRIEVAL_OPTION = "TEST_ARCHIVE_RETRIEVAL_OPTION";
        Integer TEST_EXPIRATION = 555;

        // Mock the call to external methods
        when(configurationHelper.getPropertyAsString(ConfigurationValue.AWS_ACCOUNT_ID)).thenReturn(AWS_ACCOUNT_ID);
        when(configurationHelper.getPropertyAsString(ConfigurationValue.S3_BATCH_ROLE_ARN)).thenReturn(S3_BATCH_ROLE_ARN);

        S3BatchManifest manifest = mock(S3BatchManifest.class);
        when(manifest.getS3Key()).thenReturn(TEST_MANIFEST_FILE_NAME);
        when(manifest.getLocationArn()).thenReturn(TEST_MANIFEST_ARN);
        when(manifest.getFormat()).thenReturn(TEST_MANIFEST_FORMAT);
        when(manifest.getContent()).thenReturn(TEST_MANIFEST_CONTENT);
        when(manifest.getFields()).thenReturn(TEST_MANIFEST_FIELDS);
        when(manifest.getETag()).thenReturn(TEST_MANIFEST_ETAG);

        // Execute target method
        CreateJobRequest request = s3BatchHelper.generateCreateRestoreJobRequest(manifest, TEST_JOB_ID, TEST_EXPIRATION, TEST_ARCHIVE_RETRIEVAL_OPTION);

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

        assertEquals(TEST_EXPIRATION, request.getOperation().getS3InitiateRestoreObject().getExpirationInDays());
        assertEquals(TEST_ARCHIVE_RETRIEVAL_OPTION, request.getOperation().getS3InitiateRestoreObject().getGlacierJobTier());

        assertEquals(TEST_MANIFEST_FORMAT, request.getManifest().getSpec().getFormat());
        assertArrayEquals(TEST_MANIFEST_FIELDS, request.getManifest().getSpec().getFields().toArray());
        assertEquals(TEST_MANIFEST_ARN, request.getManifest().getLocation().getObjectArn());
        assertEquals(TEST_MANIFEST_ETAG, request.getManifest().getLocation().getETag());

        assertFalse(request.getReport().getEnabled());
    }

}
