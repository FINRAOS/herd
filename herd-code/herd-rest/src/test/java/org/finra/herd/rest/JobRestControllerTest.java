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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobActionEnum;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.JobDeleteRequest;
import org.finra.herd.model.api.xml.JobSignalRequest;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.JobSummaries;
import org.finra.herd.model.api.xml.JobSummary;
import org.finra.herd.model.api.xml.JobUpdateRequest;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.S3PropertiesLocation;
import org.finra.herd.service.JobService;

/**
 * This class tests various functionality within the Job REST controller.
 */
public class JobRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private JobRestController jobRestController;

    @Mock
    private JobService jobService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateJob() throws Exception
    {
        // Create a job create request.
        JobCreateRequest jobCreateRequest = new JobCreateRequest();
        jobCreateRequest.setNamespace(JOB_NAMESPACE);
        jobCreateRequest.setJobName(JOB_NAME);

        // Create a job.
        Job job = new Job();
        job.setId(JOB_ID);

        // Mock the external calls.
        when(jobService.createAndStartJob(jobCreateRequest)).thenReturn(job);

        // Call the method under test.
        Job result = jobRestController.createJob(jobCreateRequest);

        // Verify the external calls.
        verify(jobService).createAndStartJob(jobCreateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(job, result);
    }

    @Test
    public void testDeleteJob() throws Exception
    {
        // Create a job delete request.
        JobDeleteRequest jobDeleteRequest = new JobDeleteRequest(ACTIVITI_JOB_DELETE_REASON);

        // Create a job.
        Job job = new Job();
        job.setId(JOB_ID);

        // Mock the external calls.
        when(jobService.deleteJob(JOB_ID, jobDeleteRequest)).thenReturn(job);

        // Call the method under test.
        Job result = jobRestController.deleteJob(JOB_ID, jobDeleteRequest);

        // Verify the external calls.
        verify(jobService).deleteJob(JOB_ID, jobDeleteRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(job, result);
    }

    @Test
    public void testGetJob() throws Exception
    {
        // Create a job.
        Job job = new Job();
        job.setId(JOB_ID);

        // Mock the external calls.
        when(jobService.getJob(JOB_ID, VERBOSE)).thenReturn(job);

        // Call the method under test.
        Job result = jobRestController.getJob(JOB_ID, VERBOSE);

        // Verify the external calls.
        verify(jobService).getJob(JOB_ID, VERBOSE);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(job, result);
    }

    @Test
    public void testGetJobs() throws Exception
    {
        // Create a job summary.
        JobSummary jobSummary = new JobSummary();
        jobSummary.setId(JOB_ID);
        jobSummary.setNamespace(JOB_NAMESPACE);
        jobSummary.setJobName(JOB_NAME);

        // Create a job summaries object.
        JobSummaries jobSummaries = new JobSummaries(Arrays.asList(jobSummary));

        // Mock the external calls.
        when(jobService.getJobs(JOB_NAMESPACE, JOB_NAME, JobStatusEnum.RUNNING, START_TIME, END_TIME)).thenReturn(jobSummaries);

        // Call the method under test.
        JobSummaries result = jobRestController.getJobs(JOB_NAMESPACE, JOB_NAME, JobStatusEnum.RUNNING, START_TIME.toString(), END_TIME.toString());

        // Verify the external calls.
        verify(jobService).getJobs(JOB_NAMESPACE, JOB_NAME, JobStatusEnum.RUNNING, START_TIME, END_TIME);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(jobSummaries, result);
    }

    @Test
    public void testSignalJob() throws Exception
    {
        // Create a job signal request.
        JobSignalRequest jobSignalRequest =
            new JobSignalRequest(JOB_ID, JOB_RECEIVE_TASK_ID, Arrays.asList(new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1)),
                new S3PropertiesLocation(S3_BUCKET_NAME, S3_KEY));

        // Create a job.
        Job job = new Job();
        job.setId(JOB_ID);

        // Mock the external calls.
        when(jobService.signalJob(jobSignalRequest)).thenReturn(job);

        // Call the method under test.
        Job result = jobRestController.signalJob(jobSignalRequest);

        // Verify the external calls.
        verify(jobService).signalJob(jobSignalRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(job, result);
    }

    @Test
    public void testUpdateJob() throws Exception
    {
        // Create a job update request.
        JobUpdateRequest jobUpdateRequest = new JobUpdateRequest(JobActionEnum.RESUME);

        // Create a job.
        Job job = new Job();
        job.setId(JOB_ID);

        // Mock the external calls.
        when(jobService.updateJob(JOB_ID, jobUpdateRequest)).thenReturn(job);

        // Call the method under test.
        Job result = jobRestController.updateJob(JOB_ID, jobUpdateRequest);

        // Verify the external calls.
        verify(jobService).updateJob(JOB_ID, jobUpdateRequest);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(job, result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(jobService);
    }
}
