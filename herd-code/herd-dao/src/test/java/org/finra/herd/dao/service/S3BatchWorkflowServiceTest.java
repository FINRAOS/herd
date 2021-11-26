package org.finra.herd.dao.service;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;

import com.amazonaws.services.s3control.model.BadRequestException;
import com.amazonaws.services.s3control.model.DescribeJobResult;
import com.amazonaws.services.s3control.model.JobDescriptor;
import com.amazonaws.services.s3control.model.JobStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;

public class S3BatchWorkflowServiceTest extends AbstractDaoTest
{
    @Mock
    private S3Dao s3Dao;

    @InjectMocks
    private S3BatchWorkflowServiceImpl service;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Before
    public void setup() throws IOException
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void batchRestoreObjectsSuccessTest()
    {
        String jobId = UUID.randomUUID().toString();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        DescribeJobResult describeJobResult = mock(DescribeJobResult.class);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);

        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_MAX_ATTEMPTS, Integer.class)).thenReturn(5);
        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_BACKOFF_PERIOD, Integer.class)).thenReturn(2000);

        when(s3Dao.createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION)).thenReturn(jobId);
        when(describeJobResult.getJob()).thenReturn(jobDescriptor);
        when(jobDescriptor.getStatus()).thenReturn(JobStatus.Complete.toString());
        when(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId)).thenReturn(describeJobResult);

        service.batchRestoreObjects(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);

        verify(s3Dao).createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
        verify(s3Dao).getBatchJobDescription(s3FileTransferRequestParamsDto, jobId);
    }

    @Test
    public void batchRestoreObjectsAwaitSuccessTest()
    {
        String jobId = UUID.randomUUID().toString();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        DescribeJobResult describeJobResult = mock(DescribeJobResult.class);
        JobDescriptor activeJobDescriptor = mock(JobDescriptor.class);
        JobDescriptor completedJobDescriptor = mock(JobDescriptor.class);

        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_MAX_ATTEMPTS, Integer.class)).thenReturn(5);
        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_BACKOFF_PERIOD, Integer.class)).thenReturn(2000);

        when(s3Dao.createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION)).thenReturn(jobId);

        when(activeJobDescriptor.getStatus()).thenReturn(JobStatus.Active.toString());
        when(completedJobDescriptor.getStatus()).thenReturn(JobStatus.Complete.toString());

        when(describeJobResult.getJob()).thenReturn(activeJobDescriptor, completedJobDescriptor);
        when(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId)).thenReturn(describeJobResult);

        service.batchRestoreObjects(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);

        verify(s3Dao).createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
        verify(s3Dao, times(2)).getBatchJobDescription(s3FileTransferRequestParamsDto, jobId);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void batchRestoreObjectsFailedTest()
    {
        String jobId = UUID.randomUUID().toString();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        DescribeJobResult describeJobResult = mock(DescribeJobResult.class);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);

        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_MAX_ATTEMPTS, Integer.class)).thenReturn(5);
        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_BACKOFF_PERIOD, Integer.class)).thenReturn(2000);

        when(s3Dao.createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION)).thenReturn(jobId);
        when(describeJobResult.getJob()).thenReturn(jobDescriptor);

        when(jobDescriptor.getStatus()).thenReturn(JobStatus.Failed.toString());
        when(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId)).thenReturn(describeJobResult);

        try
        {
            service.batchRestoreObjects(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains(describeJobResult.toString()));
        }

        verify(s3Dao).createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
        verify(s3Dao).getBatchJobDescription(s3FileTransferRequestParamsDto, jobId);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void batchRestoreObjectsAwaitFailedTest()
    {
        String jobId = UUID.randomUUID().toString();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        DescribeJobResult describeJobResult = mock(DescribeJobResult.class);
        JobDescriptor activeJobDescriptor = mock(JobDescriptor.class);
        JobDescriptor completedJobDescriptor = mock(JobDescriptor.class);

        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_MAX_ATTEMPTS, Integer.class)).thenReturn(5);
        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_BACKOFF_PERIOD, Integer.class)).thenReturn(2000);

        when(s3Dao.createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION)).thenReturn(jobId);

        when(activeJobDescriptor.getStatus()).thenReturn(JobStatus.Active.toString());
        when(completedJobDescriptor.getStatus()).thenReturn(JobStatus.Failed.toString());
        when(describeJobResult.getJob()).thenReturn(activeJobDescriptor, completedJobDescriptor);
        when(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId)).thenReturn(describeJobResult);

        try
        {
            service.batchRestoreObjects(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains(describeJobResult.toString()));
        }

        verify(s3Dao).createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
        verify(s3Dao, times(2)).getBatchJobDescription(s3FileTransferRequestParamsDto, jobId);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void batchRestoreObjectsAwaitCancelledTest()
    {
        String jobId = UUID.randomUUID().toString();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        DescribeJobResult describeJobResult = mock(DescribeJobResult.class);
        JobDescriptor activeJobDescriptor = mock(JobDescriptor.class);
        JobDescriptor completedJobDescriptor = mock(JobDescriptor.class);

        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_MAX_ATTEMPTS, Integer.class)).thenReturn(5);
        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_BACKOFF_PERIOD, Integer.class)).thenReturn(2000);

        when(s3Dao.createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION)).thenReturn(jobId);

        when(activeJobDescriptor.getStatus()).thenReturn(JobStatus.Active.toString());
        when(completedJobDescriptor.getStatus()).thenReturn(JobStatus.Cancelled.toString());
        when(describeJobResult.getJob()).thenReturn(activeJobDescriptor, completedJobDescriptor);
        when(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId)).thenReturn(describeJobResult);

        try
        {
            service.batchRestoreObjects(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains(describeJobResult.toString()));
        }

        verify(s3Dao).createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
        verify(s3Dao, times(2)).getBatchJobDescription(s3FileTransferRequestParamsDto, jobId);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void batchRestoreObjectsAwaitExhaustedTest()
    {
        String jobId = UUID.randomUUID().toString();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        final int retries = 5;

        DescribeJobResult describeJobResult = mock(DescribeJobResult.class);
        JobDescriptor activeJobDescriptor = mock(JobDescriptor.class);

        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_MAX_ATTEMPTS, Integer.class)).thenReturn(retries);
        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_BACKOFF_PERIOD, Integer.class)).thenReturn(1000);

        when(s3Dao.createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION)).thenReturn(jobId);

        when(activeJobDescriptor.getStatus()).thenReturn(JobStatus.Active.toString());
        when(describeJobResult.getJob()).thenReturn(activeJobDescriptor);
        when(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId)).thenReturn(describeJobResult);

        try
        {
            service.batchRestoreObjects(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains(describeJobResult.toString()));
        }

        verify(s3Dao).createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
        verify(s3Dao, times(retries)).getBatchJobDescription(s3FileTransferRequestParamsDto, jobId);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void batchRestoreObjectsCreateBatchRestoreJobFailedTest()
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        String exceptionMessage = UUID.randomUUID().toString();

        when(s3Dao.createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION)).thenThrow(
            new IllegalStateException(exceptionMessage));

        try
        {
            service.batchRestoreObjects(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains(exceptionMessage));
        }

        verify(s3Dao).createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
        verifyNoMoreInteractions(s3Dao);
    }

    @Test
    public void batchRestoreObjectsGetBatchJobDescriptionFailedTest()
    {
        String jobId = UUID.randomUUID().toString();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        String errorMessage = UUID.randomUUID().toString();

        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_MAX_ATTEMPTS, Integer.class)).thenReturn(5);
        when(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_BACKOFF_PERIOD, Integer.class)).thenReturn(2000);

        when(s3Dao.createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION)).thenReturn(jobId);

        when(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId)).thenThrow(new BadRequestException(errorMessage));

        try
        {
            service.batchRestoreObjects(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains(errorMessage));
        }

        verify(s3Dao).createBatchRestoreJob(s3FileTransferRequestParamsDto, S3_RESTORE_OBJECT_EXPIRATION_IN_DAYS, ARCHIVE_RETRIEVAL_OPTION);
        verify(s3Dao).getBatchJobDescription(s3FileTransferRequestParamsDto, jobId);
        verifyNoMoreInteractions(s3Dao);
    }


}
