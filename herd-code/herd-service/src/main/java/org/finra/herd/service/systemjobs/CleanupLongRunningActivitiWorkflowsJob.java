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
package org.finra.herd.service.systemjobs;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.api.xml.JobDeleteRequest;
import org.finra.herd.model.api.xml.JobSummaries;
import org.finra.herd.model.api.xml.JobSummary;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.JobService;
import org.finra.herd.service.helper.ParameterHelper;

/**
 * This system job is a scheduled background process that deletes long running Activiti workflows as per specified threshold. The job selects long running
 * Activiti workflows that are older than specified threshold and deletes those running workflows by calling relative API.
 */
@Component(CleanupLongRunningActivitiWorkflowsJob.JOB_NAME)
@DisallowConcurrentExecution
public class CleanupLongRunningActivitiWorkflowsJob extends AbstractSystemJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CleanupLongRunningActivitiWorkflowsJob.class);

    public static final String JOB_NAME = "cleanupLongRunningActivitiWorkflows";

    @Autowired
    private JobService jobService;

    @Autowired
    private ParameterHelper parameterHelper;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException
    {
        // Log that the system job is started.
        LOGGER.info("Started system job. systemJobName=\"{}\"", JOB_NAME);

        // Get the parameter values.
        int maxActivitiWorkflowsToProcess =
            parameterHelper.getParameterValueAsInteger(parameters, ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS);

        int activitiJobRunningThresholdInDays =
            parameterHelper.getParameterValueAsInteger(parameters, ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS);

        // Log the parameter values.
        LOGGER.info("systemJobName=\"{}\" {}={} {}={}", JOB_NAME, ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS,
            maxActivitiWorkflowsToProcess, ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS, activitiJobRunningThresholdInDays);

        // Keep a count of the processed activiti workflows.
        int processedActivitiWorkflows = 0;

        // Create a list of workflows.
        List<JobSummary> jobSummaryList = new ArrayList<>();

        try
        {
            // Select the workflows to cleanup.
            JobSummaries jobSummaries = jobService.getRunningJobsByStartBeforeTime(DateTime.now().minusDays(activitiJobRunningThresholdInDays));
            jobSummaryList = jobSummaries.getJobSummaries();
        }
        catch (Exception exception)
        {
            // Log the exception.
            LOGGER.error("Failed to get a list of Activiti workflows. systemJobName=\"{}\"", JOB_NAME, exception);
        }

        // Log the number of activiti workflows selected for processing.
        LOGGER.info("Number of Activiti jobs that meet the criteria. systemJobName=\"{}\" activitiWorkflowCount={}", JOB_NAME, jobSummaryList.size());

        // Resize the list of Activiti workflows so that it is not greater than the maxActivitiWorkflowsToProcess.
        if (jobSummaryList.size() >= maxActivitiWorkflowsToProcess)
        {
            // Returns a view of the portion of this list between the specified fromIndex, inclusive, and toIndex, exclusive.
            jobSummaryList = jobSummaryList.subList(0, maxActivitiWorkflowsToProcess);
        }

        // Try to delete each of the selected Activiti workflows.
        for (JobSummary jobSummary : jobSummaryList)
        {
            try
            {
                LOGGER.info(
                    "Deleting Activiti workflow. systemJobName=\"{}\" jobId=\"{}\" jobName=\"{}\" jobNamespace=\"{}\" jobStatus=\"{}\" startTime=\"{}\"",
                    JOB_NAME, jobSummary.getId(), jobSummary.getJobName(), jobSummary.getNamespace(), jobSummary.getStatus(), jobSummary.getStartTime());
                jobService.deleteJob(jobSummary.getId(), new JobDeleteRequest(
                    "Activiti workflow running longer than " + activitiJobRunningThresholdInDays + " days. Deleted by " + JOB_NAME + "."));
                processedActivitiWorkflows += 1;
            }
            catch (Exception exception)
            {
                // Log the exception.
                LOGGER.error("Failed to delete an Activiti workflow. systemJobName=\"{}\" jobId=\"{}\" jobName=\"{}\" jobNamespace=\"{}\"",
                    JOB_NAME, jobSummary.getId(), jobSummary.getJobName(), jobSummary.getNamespace(), exception);
            }
        }

        // Log the number of cleanup activiti workflows.
        LOGGER.info("Cleanup activiti workflows. systemJobName=\"{}\" processedActivitiWorkflows={}", JOB_NAME, processedActivitiWorkflows);

        // Log that the system job is ended.
        LOGGER.info("Completed system job. systemJobName=\"{}\"", JOB_NAME);
    }

    @Override
    public void validateParameters(List<Parameter> parameters)
    {
        // This system job accepts two optional parameters as integer values.
        if (!CollectionUtils.isEmpty(parameters))
        {
            Assert.isTrue(parameters.size() < 3, String.format("Too many parameters are specified for \"%s\" system job.", JOB_NAME));
            Assert.isTrue(parameters.get(0).getName().equalsIgnoreCase(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS.getKey()) ||
                    parameters.get(0).getName().equalsIgnoreCase(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS.getKey()),
                String.format("Parameter \"%s\" is not supported by \"%s\" system job.", parameters.get(0).getName(), JOB_NAME));
            Assert.isTrue(parameterHelper.getParameterValueAsInteger(parameters.get(0)) > 0,
                String.format("Parameter \"%s\" must be greater than zero in the \"%s\" system job.", parameters.get(0).getName(), JOB_NAME));
            if (parameters.size() > 1)
            {
                Assert.isTrue(
                    parameters.get(1).getName().equalsIgnoreCase(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS.getKey()) ||
                        parameters.get(1).getName().equalsIgnoreCase(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS.getKey()),
                    String.format("Parameter \"%s\" is not supported by \"%s\" system job.", parameters.get(1).getName(), JOB_NAME));
                Assert.isTrue(parameterHelper.getParameterValueAsInteger(parameters.get(1)) > 0,
                    String.format("Parameter \"%s\" must be greater than zero in the \"%s\" system job.", parameters.get(1).getName(), JOB_NAME));
            }
        }
    }

    @Override
    public JobDataMap getJobDataMap()
    {
        return getJobDataMap(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS,
            ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS);
    }

    @Override
    public String getCronExpression()
    {
        return configurationHelper.getProperty(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_CRON_EXPRESSION);
    }
}
