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

import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.JobDeleteRequest;
import org.finra.herd.model.api.xml.JobSignalRequest;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.JobSummaries;

/**
 * The job service.
 */
public interface JobService
{
    public Job createAndStartJob(JobCreateRequest jobCreateRequest, boolean isAsync) throws Exception;

    public Job getJob(String id, boolean verbose) throws Exception;

    public JobSummaries getJobs(String namespace, String jobName, JobStatusEnum jobStatus) throws Exception;

    public Job signalJob(JobSignalRequest jobSignalRequest) throws Exception;

    /**
     * Deletes a currently running job and preserves the job state in history.
     * 
     * @param jobId The job id
     * @param jobDeleteRequest The delete request
     * @return The job that has been deleted
     * @throws Exception when any exception occurs
     */
    public Job deleteJob(String jobId, JobDeleteRequest jobDeleteRequest) throws Exception;
}
