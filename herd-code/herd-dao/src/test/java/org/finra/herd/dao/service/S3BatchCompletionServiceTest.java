package org.finra.herd.dao.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;

import com.amazonaws.services.s3control.model.DescribeJobResult;
import com.amazonaws.services.s3control.model.JobDescriptor;
import com.amazonaws.services.s3control.model.JobStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.impl.S3BatchCompletionServiceImpl;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;

public class S3BatchCompletionServiceTest extends AbstractDaoTest
{
    @Mock
    private S3Dao s3Dao;

    @InjectMocks
    private S3BatchCompletionServiceImpl service;

    @Before
    public void setup() throws IOException
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testAwaitForBatchJobCompleteHappyPath()
    {
        String jobId = UUID.randomUUID().toString();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        DescribeJobResult describeJobResult = mock(DescribeJobResult.class);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);

        when(describeJobResult.getJob()).thenReturn(jobDescriptor);
        when(jobDescriptor.getStatus()).thenReturn(JobStatus.Complete.toString());
        when(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId)).thenReturn(describeJobResult);

        DescribeJobResult actual = service.awaitForBatchJobComplete(s3FileTransferRequestParamsDto, jobId);

        assertEquals(describeJobResult, actual);
        verify(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId));
    }

    @Test
    public void testAwaitForBatchJobCompleteFailed()
    {
        String jobId = UUID.randomUUID().toString();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        DescribeJobResult describeJobResult = mock(DescribeJobResult.class);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);

        when(describeJobResult.getJob()).thenReturn(jobDescriptor);
        when(jobDescriptor.getStatus()).thenReturn(JobStatus.Failed.toString());
        when(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId)).thenReturn(describeJobResult);

        DescribeJobResult actual = service.awaitForBatchJobComplete(s3FileTransferRequestParamsDto, jobId);

        assertEquals(describeJobResult, actual);
        verify(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId));
    }

    @Test
    public void testAwaitForBatchJobCompleteIncomplete()
    {
        String jobId = UUID.randomUUID().toString();
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();

        DescribeJobResult describeJobResult = mock(DescribeJobResult.class);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);

        when(describeJobResult.getJob()).thenReturn(jobDescriptor);
        when(jobDescriptor.getStatus()).thenReturn(JobStatus.Active.toString());
        when(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId)).thenReturn(describeJobResult);

        try
        {
            service.awaitForBatchJobComplete(s3FileTransferRequestParamsDto, jobId);
            fail();
        }
        catch(S3BatchJobIncompleteException e) {
            assertEquals(describeJobResult, e.jobDescriptor);
        }

        verify(s3Dao.getBatchJobDescription(s3FileTransferRequestParamsDto, jobId));
    }

}
