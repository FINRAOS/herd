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
package org.finra.herd.service;

import org.joda.time.DateTime;

import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.JobDeleteRequest;
import org.finra.herd.model.api.xml.JobSignalRequest;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.JobSummaries;
import org.finra.herd.model.api.xml.JobUpdateRequest;

/**
 * The job service.
 */
public interface JobService
{
    /**
     * Creates and starts a new job asynchronously.
     *
     * @param jobCreateRequest the information needed to create the job
     *
     * @return the created job information
     * @throws Exception if any problems were encountered
     */
    public Job createAndStartJob(JobCreateRequest jobCreateRequest) throws Exception;

    /**
     * Deletes a currently running job and preserves the job state in history. The method calls org.activiti.engine.RuntimeService.deleteProcessInstance(String,
     * String) on the specified jobId.
     *
     * @param jobId the job id
     * @param jobDeleteRequest the delete request
     *
     * @return the job that has been deleted
     * @throws Exception if any problems were encountered
     */
    public Job deleteJob(String jobId, JobDeleteRequest jobDeleteRequest) throws Exception;

    /**
     * Gets the details of a previously submitted job.
     *
     * @param id the job id
     *
     * @return the job information
     * @throws Exception if any problems were encountered
     */
    public Job getJob(String id, boolean verbose) throws Exception;

    /**
     * <p>Gets a list of job executions based on the specified filter parameters.</p> <p>Jobs' namespace to which you do not have READ permissions to will be
     * omitted from the result.</p>
     *
     * @param namespace an optional namespace filter
     * @param jobName an optional job name filter
     * @param jobStatus an optional job status filter
     * @param startTime an optional job start time filter
     * @param endTime an optional job end time filter
     *
     * @return the list of job summaries
     * @throws Exception if any problems were encountered
     */
    public JobSummaries getJobs(String namespace, String jobName, JobStatusEnum jobStatus, DateTime startTime, DateTime endTime) throws Exception;

    /**
     * Signals the job with the receive task.
     *
     * @param jobSignalRequest the information needed to signal the job
     *
     * @return the created job information
     */
    public Job signalJob(JobSignalRequest jobSignalRequest) throws Exception;

    /**
     * Activates or suspends a job execution.
     *
     * @param jobId the job id
     * @param jobUpdateRequest the job update request
     *
     * @return the job
     * @throws Exception when any exception occurs
     */
    public Job updateJob(String jobId, JobUpdateRequest jobUpdateRequest) throws Exception;
}
