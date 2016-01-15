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
package org.finra.herd.rest;

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.JobDeleteRequest;
import org.finra.herd.model.api.xml.JobSignalRequest;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.JobSummaries;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.JobService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles job REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Job")
public class JobRestController extends HerdBaseController
{
    @Autowired
    private JobService jobService;

    /**
     * Creates and starts a new job asynchronously.
     *
     * @param request the information needed to create the job.
     *
     * @return the created job information.
     */
    @RequestMapping(value = "/jobs", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_JOBS_POST)
    public Job createJob(@RequestBody JobCreateRequest request) throws Exception
    {
        // Create and return a new job.
        return jobService.createAndStartJob(request, true);
    }

    /**
     * Gets a list of job executions based on the specified filter parameters. This currently only retrieves running jobs.
     *
     * @param namespace an optional namespace filter.
     * @param jobName an optional job name filter.
     * @param status an optional status filter.
     *
     * @return the list of job summaries.
     * @throws Exception if any problems were encountered.
     */
    @RequestMapping(value = "/jobs", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_JOBS_GET)
    public JobSummaries getJobs(@RequestParam(value = "namespace", required = false) String namespace,
        @RequestParam(value = "jobName", required = false) String jobName, @RequestParam(value = "status", required = false) JobStatusEnum status)
        throws Exception
    {
        return jobService.getJobs(namespace, jobName, status);
    }

    /**
     * Gets the details of a previously submitted job.
     *
     * @param id the job id.
     *
     * @return the job information.
     */
    @RequestMapping(value = "/jobs/ids/{id}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_JOBS_GET_BY_ID)
    public Job getJob(@PathVariable("id") String id, @RequestParam(value = "verbose", required = false, defaultValue = "false") Boolean verbose)
        throws Exception
    {
        return jobService.getJob(id, verbose);
    }

    /**
     * Signals the job with the receive task.
     *
     * @param request the information needed to signal the job.
     *
     * @return the created job information.
     */
    @RequestMapping(value = "/jobs/signal", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_JOBS_SIGNAL_POST)
    public Job signalJob(@RequestBody JobSignalRequest request) throws Exception
    {
        // Create and return a new job.
        return jobService.signalJob(request);
    }

    /**
     * Deletes a currently running job and preserves the job state in history.
     *
     * @param id The job id
     * @param jobDeleteRequest The delete request
     *
     * @return The job that has been deleted
     * @throws Exception when any exception occurs
     */
    @RequestMapping(value = "/jobs/ids/{id}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_JOBS_DELETE)
    public Job deleteJob(@PathVariable("id") String id, @RequestBody JobDeleteRequest jobDeleteRequest) throws Exception
    {
        return jobService.deleteJob(id, jobDeleteRequest);
    }
}
