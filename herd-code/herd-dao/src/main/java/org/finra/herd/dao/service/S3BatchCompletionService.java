package org.finra.herd.dao.service;

import com.amazonaws.services.s3control.model.DescribeJobResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;

@Service
public interface S3BatchCompletionService
{
    // TODO: Confure attempts and delay
    @Retryable(value = S3BatchJobIncompleteException.class, maxAttempts = 30, backoff = @Backoff(delay = 30*1000))
    DescribeJobResult awaitForBatchJobComplete(S3FileTransferRequestParamsDto request, String batchJobId) throws S3BatchJobIncompleteException;

    @Recover
    void recover(S3BatchJobIncompleteException e, S3FileTransferRequestParamsDto request, String batchJobId);
}
