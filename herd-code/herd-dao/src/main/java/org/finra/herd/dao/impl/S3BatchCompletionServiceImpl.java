package org.finra.herd.dao.impl;

import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3control.model.DescribeJobResult;
import com.amazonaws.services.s3control.model.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.service.S3BatchCompletionService;
import org.finra.herd.dao.service.S3BatchJobIncompleteException;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;

@Component
public class S3BatchCompletionServiceImpl implements S3BatchCompletionService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(S3BatchCompletionServiceImpl.class);
    private static final List<JobStatus> finalStates = Arrays.asList(
        JobStatus.Complete,
        JobStatus.Failed,
        JobStatus.Cancelled,
        JobStatus.Suspended);

    @Autowired
    private S3Dao s3Dao;

    @Override
    public DescribeJobResult awaitForBatchJobComplete(S3FileTransferRequestParamsDto request, String batchJobId) throws S3BatchJobIncompleteException
    {
        DescribeJobResult result = s3Dao.getBatchJobDescription(request, batchJobId);
        JobStatus jobStatus = JobStatus.fromValue(result.getJob().getStatus());
        if (finalStates.contains(jobStatus)) return result;

        throw new S3BatchJobIncompleteException(result);
    }

    @Override
    public void recover(S3BatchJobIncompleteException e, S3FileTransferRequestParamsDto request, String batchJobId)
    {
        LOGGER.info("S3 batchJobID=\"{}\" was not complete within configured timeframe. Last description: {}.", batchJobId, e.toString());
    }
}
