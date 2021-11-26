package org.finra.herd.dao.service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.amazonaws.services.s3control.model.DescribeJobResult;
import com.amazonaws.services.s3control.model.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;

@Service
public class S3BatchWorkflowServiceImpl implements S3BatchWorkflowService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(S3BatchWorkflowServiceImpl.class);
    private static final List<JobStatus> finalStates = Arrays.asList(JobStatus.Complete, JobStatus.Failed, JobStatus.Cancelled, JobStatus.Suspended);

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Override
    public void batchRestoreObjects(final S3FileTransferRequestParamsDto params, int expirationInDays, String archiveRetrievalOption)
    {
        String jobId = s3Dao.createBatchRestoreJob(params, expirationInDays, archiveRetrievalOption);
        RetryTemplate template = new RetryTemplate();

        SimpleRetryPolicy policy = new SimpleRetryPolicy(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_MAX_ATTEMPTS, Integer.class),
            Collections.singletonMap(S3BatchJobIncompleteException.class, true));
        template.setRetryPolicy(policy);

        FixedBackOffPolicy backoffPolicy = new FixedBackOffPolicy();

        backoffPolicy.setBackOffPeriod(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_BACKOFF_PERIOD, Integer.class));
        template.setBackOffPolicy(backoffPolicy);

        template.setThrowLastExceptionOnExhausted(false);

        DescribeJobResult result = template.execute((RetryCallback<DescribeJobResult, S3BatchJobIncompleteException>) context -> {
            LOGGER.info("Attempt to get descriptor");
            DescribeJobResult retryResult = s3Dao.getBatchJobDescription(params, jobId);
            JobStatus jobStatus = JobStatus.fromValue(retryResult.getJob().getStatus());
            if (!finalStates.contains(jobStatus))
            {
                throw new S3BatchJobIncompleteException(retryResult);
            }

            return retryResult;
        }, context -> {
            if (context.getLastThrowable() instanceof S3BatchJobIncompleteException)
            {
                return ((S3BatchJobIncompleteException) context.getLastThrowable()).jobDescriptor;
            }
            else
            {
                throw new IllegalStateException(context.getLastThrowable());
            }
        });

        if (result == null || result.getJob() == null || result.getJob().getStatus() == null)
        {
            IllegalStateException e = new IllegalStateException("Unable to retrieve descriptor of the batch job");
            LOGGER.warn("Batch restore call failed", e);
            throw e;
        }

        JobStatus jobStatus = JobStatus.fromValue(result.getJob().getStatus());
        if (jobStatus != JobStatus.Complete)
        {
            IllegalStateException e = new IllegalStateException(String.format("S3 batch job was not complete. Detailed descriptor:  %s ", result));
            LOGGER.warn("Batch restore call failed", e);
            throw e;
        }
    }


}
