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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.ProcessInstance;
import org.activiti.engine.runtime.ProcessInstanceQuery;
import org.activiti.engine.task.Task;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobActionEnum;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.JobDefinitionCreateRequest;
import org.finra.herd.model.api.xml.JobDeleteRequest;
import org.finra.herd.model.api.xml.JobSignalRequest;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.JobSummaries;
import org.finra.herd.model.api.xml.JobSummary;
import org.finra.herd.model.api.xml.JobUpdateRequest;
import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.S3PropertiesLocation;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.SecurityUserWrapper;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * This class tests various functionality within the Job REST controller.
 */
public class JobServiceTest extends AbstractServiceTest
{
    private static Logger LOGGER = LoggerFactory.getLogger(JobServiceTest.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Test
    public void testCreateJob() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create a job definition create request using hard coded test values.
        JobDefinitionCreateRequest jobDefinitionCreateRequest = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();

        // Create the job definition.
        jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);

        // Create a job create request using hard coded test values.
        JobCreateRequest jobCreateRequest = jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);

        // Create the job.
        Job resultJob = jobService.createAndStartJob(jobCreateRequest);

        // Validate the results.
        assertNotNull(resultJob);
        assertNotNull(resultJob.getId());
        assertTrue(!resultJob.getId().isEmpty());
        assertEquals(TEST_ACTIVITI_NAMESPACE_CD, resultJob.getNamespace());
        assertEquals(TEST_ACTIVITI_JOB_NAME, resultJob.getJobName());
        assertEquals(jobDefinitionCreateRequest.getParameters().size() + jobCreateRequest.getParameters().size() + 1, resultJob.getParameters().size());
        List<String> expectedParameters = new ArrayList<>();
        expectedParameters.addAll(parametersToStringList(jobDefinitionCreateRequest.getParameters()));
        expectedParameters.addAll(parametersToStringList(jobCreateRequest.getParameters()));
        expectedParameters.addAll(parametersToStringList(
            Arrays.asList(new Parameter(HERD_WORKFLOW_ENVIRONMENT, configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT)))));
        List<String> resultParameters = parametersToStringList(resultJob.getParameters());
        assertTrue(expectedParameters.containsAll(resultParameters));
        assertTrue(resultParameters.containsAll(expectedParameters));
    }

    @Test
    public void testCreateJobNoParams() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create a job definition create request using hard coded test values.
        JobDefinitionCreateRequest jobDefinitionCreateRequest = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();
        jobDefinitionCreateRequest.setParameters(null);

        // Create the job definition.
        jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);

        // Create a job create request using hard coded test values.
        JobCreateRequest jobCreateRequest = jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);
        jobCreateRequest.setParameters(null);

        // Create the job.
        Job resultJob = jobService.createAndStartJob(jobCreateRequest);

        //expected default parameter
        List<Parameter> expectedParameters =
            Arrays.asList(new Parameter(HERD_WORKFLOW_ENVIRONMENT, configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT)));

        // Validate the results.
        assertNotNull(resultJob);
        assertNotNull(resultJob.getId());
        assertTrue(!resultJob.getId().isEmpty());
        assertEquals(TEST_ACTIVITI_NAMESPACE_CD, resultJob.getNamespace());
        assertEquals(TEST_ACTIVITI_JOB_NAME, resultJob.getJobName());
        assertTrue(resultJob.getParameters().size() == 1);
        assertTrue(expectedParameters.containsAll(resultJob.getParameters()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateJobNamespaceEmpty() throws Exception
    {
        // Try to create a job by passing an empty namespace code.
        jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(" ", TEST_ACTIVITI_JOB_NAME));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateJobJobNameEmpty() throws Exception
    {
        // Try to create a job by passing an empty job name.
        jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, " "));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateJobParameterNameEmpty() throws Exception
    {
        // Create a job create request using hard coded test values.
        JobCreateRequest jobCreateRequest = jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);

        // Add a parameter with an empty name.
        Parameter parameter = new Parameter(" ", ATTRIBUTE_VALUE_1);
        jobCreateRequest.getParameters().add(parameter);

        // Try to create a job.
        jobService.createAndStartJob(jobCreateRequest);
    }

    @Test
    public void testCreateJobInvalidParameters() throws Exception
    {
        // Try to create a job when namespace contains a forward slash character.
        try
        {
            jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(addSlash(TEST_ACTIVITI_NAMESPACE_CD), TEST_ACTIVITI_JOB_NAME));
            fail("Should throw an IllegalArgumentException when namespace contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a job when job name name contains a forward slash character.
        try
        {
            jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, addSlash(TEST_ACTIVITI_JOB_NAME)));
            fail("Should throw an IllegalArgumentException when job name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Job name can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateJobDuplicateParameterName() throws Exception
    {
        // Create a job create request using hard coded test values.
        JobCreateRequest jobCreateRequest = jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);

        // Add a duplicate parameter.
        Parameter parameter = new Parameter();
        parameter.setName(addWhitespace(jobCreateRequest.getParameters().get(0).getName().toUpperCase()));
        parameter.setValue(jobCreateRequest.getParameters().get(0).getValue());
        jobCreateRequest.getParameters().add(parameter);

        // Try to create a job.
        jobService.createAndStartJob(jobCreateRequest);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testCreateJobNamespaceNoExists() throws Exception
    {
        // Try to create a job using non-existing namespace code.
        jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest("I_DO_NOT_EXIST", TEST_ACTIVITI_JOB_NAME));
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testCreateJobJobNameNoExists() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Try to create a job using non-existing job definition name.
        jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, "I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetJob() throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);

        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String activitiXml = IOUtils.toString(resourceLoader.getResource(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH).getInputStream());
        // Job should be waiting at User task.

        // Running job with verbose
        Job jobGet = jobService.getJob(job.getId(), true);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertNotNull(jobGet.getActivitiJobXml());
        assertEquals(activitiXml, jobGet.getActivitiJobXml());
        assertTrue(jobGet.getCompletedWorkflowSteps().size() > 0);
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());
        assertEquals(job.getNamespace(), jobGet.getNamespace());
        assertEquals(job.getJobName(), jobGet.getJobName());

        // Running job with non verbose
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());
        assertEquals(job.getNamespace(), jobGet.getNamespace());
        assertEquals(job.getJobName(), jobGet.getJobName());

        // Query the pending task and complete it
        List<Task> tasks = activitiTaskService.createTaskQuery().processInstanceId(job.getId()).list();

        activitiTaskService.complete(tasks.get(0).getId());

        // Job should have been completed.

        // Completed job with verbose
        jobGet = jobService.getJob(job.getId(), true);
        assertEquals(JobStatusEnum.COMPLETED, jobGet.getStatus());
        assertNotNull(jobGet.getStartTime());
        assertNotNull(jobGet.getEndTime());
        assertNotNull(jobGet.getActivitiJobXml());
        assertEquals(activitiXml, jobGet.getActivitiJobXml());
        assertTrue(jobGet.getCompletedWorkflowSteps().size() > 0);
        assertNull(jobGet.getCurrentWorkflowStep());
        assertEquals(job.getNamespace(), jobGet.getNamespace());
        assertEquals(job.getJobName(), jobGet.getJobName());

        // Completed job with non verbose
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.COMPLETED, jobGet.getStatus());
        assertNotNull(jobGet.getStartTime());
        assertNotNull(jobGet.getEndTime());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertNull(jobGet.getCurrentWorkflowStep());
        assertEquals(job.getNamespace(), jobGet.getNamespace());
        assertEquals(job.getJobName(), jobGet.getJobName());
    }

    @Test
    public void testGetJobIntermediateTimer() throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_HERD_INTERMEDIATE_TIMER_WITH_CLASSPATH);

        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String activitiXml = IOUtils.toString(resourceLoader.getResource(ACTIVITI_XML_HERD_INTERMEDIATE_TIMER_WITH_CLASSPATH).getInputStream());
        // Job should be waiting at User task.

        // Get job status
        Job jobGet = jobService.getJob(job.getId(), true);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertNotNull(jobGet.getActivitiJobXml());
        assertEquals(activitiXml, jobGet.getActivitiJobXml());
        assertTrue(jobGet.getCompletedWorkflowSteps().size() > 0);
        // Current workflow step will be null
        assertNull(jobGet.getCurrentWorkflowStep());

        org.activiti.engine.runtime.Job timer = activitiManagementService.createJobQuery().processInstanceId(job.getId()).timers().singleResult();
        if (timer != null)
        {
            activitiManagementService.executeJob(timer.getId());
        }

        // Get the job status again. job should have completed now.
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.COMPLETED, jobGet.getStatus());
        assertNull(jobGet.getCurrentWorkflowStep());
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testGetJobNoJobFound() throws Exception
    {
        jobService.getJob("job_not_submitted", true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetJobNoJobId() throws Exception
    {
        jobService.getJob(null, false);
    }

    @Test
    public void testGetJobs() throws Exception
    {
        // Create and persist a job definition.
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);

        // Create and start three Activiti jobs.
        List<Job> jobs = Arrays
            .asList(jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME)),
                jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME)),
                jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME)));

        // Jobs should be waiting at relative User tasks.

        // Allow READ access for the current user to the job definition namespace.
        jobServiceTestHelper.setCurrentUserNamespaceAuthorizations(TEST_ACTIVITI_NAMESPACE_CD, Arrays.asList(NamespacePermissionEnum.READ));

        JobSummaries resultJobSummaries;
        Map<String, JobStatusEnum> expectedJobStatuses;

        DateTime startTime = new DateTime().minusHours(1);
        DateTime endTime = new DateTime().plusHours(1);

        // Get all jobs for the relative job definition and expected job status.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.RUNNING, startTime, NO_END_TIME);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(jobs.get(0).getId(), JobStatusEnum.RUNNING);
                put(jobs.get(1).getId(), JobStatusEnum.RUNNING);
                put(jobs.get(2).getId(), JobStatusEnum.RUNNING);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);

        // Query the pending tasks and complete them.
        for (Job job : jobs)
        {
            List<Task> tasks = activitiTaskService.createTaskQuery().processInstanceId(job.getId()).list();
            activitiTaskService.complete(tasks.get(0).getId());
        }

        // Jobs should have been completed.

        // Get all jobs for the relative job definition and expected job status.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.COMPLETED, startTime, endTime);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(jobs.get(0).getId(), JobStatusEnum.COMPLETED);
                put(jobs.get(1).getId(), JobStatusEnum.COMPLETED);
                put(jobs.get(2).getId(), JobStatusEnum.COMPLETED);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);
    }

    @Test
    public void testGetJobsTrimAndCaseInsensitivity() throws Exception
    {
        // Create and persist a job definition.
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);

        // Create and start an Activiti job.
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        // Job should be waiting at relative User tasks.

        // Allow READ access for the current user to the job definition namespace.
        jobServiceTestHelper.setCurrentUserNamespaceAuthorizations(TEST_ACTIVITI_NAMESPACE_CD, Arrays.asList(NamespacePermissionEnum.READ));

        // Perform the getJobs calls.
        JobSummaries resultJobSummaries;
        Map<String, JobStatusEnum> expectedJobStatuses;

        DateTime startTime = new DateTime().minusHours(1);
        DateTime endTime = new DateTime().plusHours(1);

        // Get all jobs using input parameters with leading and trailing empty spaces.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.RUNNING, startTime, NO_END_TIME);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(job.getId(), JobStatusEnum.RUNNING);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);

        // Query the pending task and complete it.
        List<Task> tasks = activitiTaskService.createTaskQuery().processInstanceId(job.getId()).list();
        activitiTaskService.complete(tasks.get(0).getId());

        // Jobs should have been completed.

        // Get all jobs using input parameters in uppercase.
        resultJobSummaries =
            jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD.toUpperCase(), TEST_ACTIVITI_JOB_NAME.toUpperCase(), JobStatusEnum.COMPLETED, startTime, endTime);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(job.getId(), JobStatusEnum.COMPLETED);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);

        // Get all jobs using input parameters in lowercase.
        resultJobSummaries =
            jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD.toLowerCase(), TEST_ACTIVITI_JOB_NAME.toLowerCase(), JobStatusEnum.COMPLETED, startTime, endTime);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(job.getId(), JobStatusEnum.COMPLETED);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);
    }

    @Test
    public void testGetJobsMissingOptionalParameters() throws Exception
    {
        // Create and persist a job definition.
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);

        // Create and start three Activiti jobs.
        List<Job> jobs = Arrays
            .asList(jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME)),
                jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME)),
                jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME)));

        // Jobs should be waiting at relative User tasks.

        // Allow READ access for the current user to the job definition namespace.
        jobServiceTestHelper.setCurrentUserNamespaceAuthorizations(TEST_ACTIVITI_NAMESPACE_CD, Arrays.asList(NamespacePermissionEnum.READ));

        JobSummaries resultJobSummaries;
        Map<String, JobStatusEnum> expectedJobStatuses;

        // Get all jobs without specifying any of the optional parameters.
        resultJobSummaries = jobService.getJobs(NO_NAMESPACE, NO_ACTIVITI_JOB_NAME, NO_ACTIVITI_JOB_STATUS, NO_START_TIME, NO_END_TIME);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(jobs.get(0).getId(), JobStatusEnum.RUNNING);
                put(jobs.get(1).getId(), JobStatusEnum.RUNNING);
                put(jobs.get(2).getId(), JobStatusEnum.RUNNING);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);

        // Query the pending tasks and complete them.
        for (Job job : jobs)
        {
            List<Task> tasks = activitiTaskService.createTaskQuery().processInstanceId(job.getId()).list();
            activitiTaskService.complete(tasks.get(0).getId());
        }

        // Jobs should have been completed.

        // Get all jobs without specifying any of the optional parameters.
        resultJobSummaries = jobService.getJobs(NO_NAMESPACE, NO_ACTIVITI_JOB_NAME, NO_ACTIVITI_JOB_STATUS, NO_START_TIME, NO_END_TIME);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(jobs.get(0).getId(), JobStatusEnum.COMPLETED);
                put(jobs.get(1).getId(), JobStatusEnum.COMPLETED);
                put(jobs.get(2).getId(), JobStatusEnum.COMPLETED);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);
    }

    @Test
    public void testGetJobsInvalidParameters() throws Exception
    {
        // Create and persist a job definition.
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);

        // Create and start three Activiti jobs.
        List<Job> jobs = Arrays
            .asList(jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME)),
                jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME)),
                jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME)));

        // Jobs should be waiting at relative User tasks.

        // Allow READ access for the current user to the job definition namespace.
        jobServiceTestHelper.setCurrentUserNamespaceAuthorizations(TEST_ACTIVITI_NAMESPACE_CD, Arrays.asList(NamespacePermissionEnum.READ));

        JobSummaries resultJobSummaries;
        Map<String, JobStatusEnum> expectedJobStatuses;

        // Get all jobs for the relative job definition and expected job status.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.RUNNING, NO_START_TIME, NO_END_TIME);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(jobs.get(0).getId(), JobStatusEnum.RUNNING);
                put(jobs.get(1).getId(), JobStatusEnum.RUNNING);
                put(jobs.get(2).getId(), JobStatusEnum.RUNNING);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);

        // Try to get jobs using invalid job definition namespace.
        resultJobSummaries = jobService.getJobs("I_DO_NOT_EXIST", TEST_ACTIVITI_JOB_NAME, JobStatusEnum.RUNNING, NO_START_TIME, NO_END_TIME);
        assertEquals(0, resultJobSummaries.getJobSummaries().size());

        // Try to get jobs using invalid job definition name.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, "I_DO_NOT_EXIST", JobStatusEnum.RUNNING, NO_START_TIME, NO_END_TIME);
        assertEquals(0, resultJobSummaries.getJobSummaries().size());

        // Try to get jobs using SUSPENDED job status.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.SUSPENDED, NO_START_TIME, NO_END_TIME);
        assertEquals(0, resultJobSummaries.getJobSummaries().size());

        // Try to get jobs using COMPLETED job status.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.COMPLETED, NO_START_TIME, NO_END_TIME);
        assertEquals(0, resultJobSummaries.getJobSummaries().size());

        // Try to get jobs using invalid start time (current time plus one hour).
        DateTime invalidStartTime = new DateTime().plusHours(1);
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.RUNNING, invalidStartTime, NO_END_TIME);
        assertEquals(0, resultJobSummaries.getJobSummaries().size());

        // Query the pending tasks and complete them.
        for (Job job : jobs)
        {
            List<Task> tasks = activitiTaskService.createTaskQuery().processInstanceId(job.getId()).list();
            activitiTaskService.complete(tasks.get(0).getId());
        }

        // Jobs should have been completed.

        // Get all jobs for the relative job definition and expected job status.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.COMPLETED, NO_START_TIME, NO_END_TIME);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(jobs.get(0).getId(), JobStatusEnum.COMPLETED);
                put(jobs.get(1).getId(), JobStatusEnum.COMPLETED);
                put(jobs.get(2).getId(), JobStatusEnum.COMPLETED);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);

        // Try to get jobs using invalid end time (current time minus one hour).
        DateTime invalidEndTime = new DateTime().minusHours(1);
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.COMPLETED, NO_START_TIME, invalidEndTime);
        assertEquals(0, resultJobSummaries.getJobSummaries().size());
    }

    @Test
    public void testGetJobsValidateJobStatusFilter() throws Exception
    {
        // Create and persist a job definition.
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);

        // Create and start two Activiti jobs.
        List<Job> jobs = Arrays
            .asList(jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME)),
                jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME)));

        // Jobs should be waiting at relative User tasks.

        // Allow READ and EXECUTE access for the current user to the job definition namespace.
        jobServiceTestHelper
            .setCurrentUserNamespaceAuthorizations(TEST_ACTIVITI_NAMESPACE_CD, Arrays.asList(NamespacePermissionEnum.READ, NamespacePermissionEnum.EXECUTE));

        JobSummaries resultJobSummaries;
        Map<String, JobStatusEnum> expectedJobStatuses;
        List<Task> tasks;

        // Get all jobs for the relative job definition and expected job status.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.RUNNING, NO_START_TIME, NO_END_TIME);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(jobs.get(0).getId(), JobStatusEnum.RUNNING);
                put(jobs.get(1).getId(), JobStatusEnum.RUNNING);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);

        // Suspend the first job.
        jobService.updateJob(jobs.get(0).getId(), new JobUpdateRequest(JobActionEnum.SUSPEND));

        // The second job is still RUNNING.

        // Get RUNNING jobs.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.RUNNING, NO_START_TIME, NO_END_TIME);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(jobs.get(1).getId(), JobStatusEnum.RUNNING);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);

        // Get SUSPENDED jobs.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.SUSPENDED, NO_START_TIME, NO_END_TIME);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(jobs.get(0).getId(), JobStatusEnum.SUSPENDED);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);

        // Resume the first job.
        jobService.updateJob(jobs.get(0).getId(), new JobUpdateRequest(JobActionEnum.RESUME));

        // Complete the first job.
        tasks = activitiTaskService.createTaskQuery().processInstanceId(jobs.get(0).getId()).list();
        activitiTaskService.complete(tasks.get(0).getId());

        // Get COMPLETED jobs.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.COMPLETED, NO_START_TIME, NO_END_TIME);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(jobs.get(0).getId(), JobStatusEnum.COMPLETED);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);

        // Complete the second job.
        tasks = activitiTaskService.createTaskQuery().processInstanceId(jobs.get(1).getId()).list();
        activitiTaskService.complete(tasks.get(0).getId());

        // All jobs now should have been completed.

        // Try to get RUNNING jobs.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.RUNNING, NO_START_TIME, NO_END_TIME);
        assertEquals(0, resultJobSummaries.getJobSummaries().size());

        // Try to get SUSPENDED jobs.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.SUSPENDED, NO_START_TIME, NO_END_TIME);
        assertEquals(0, resultJobSummaries.getJobSummaries().size());

        // Get COMPLETED jobs.
        resultJobSummaries = jobService.getJobs(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, JobStatusEnum.COMPLETED, NO_START_TIME, NO_END_TIME);

        // Validate the result job summaries.
        expectedJobStatuses = new HashMap<String, JobStatusEnum>()
        {{
                put(jobs.get(0).getId(), JobStatusEnum.COMPLETED);
                put(jobs.get(1).getId(), JobStatusEnum.COMPLETED);
            }};
        validateJobSummaries(expectedJobStatuses, resultJobSummaries);
    }

    @Test
    public void testGetRunningJobsByStartBeforeTime() throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);

        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String activitiXml = IOUtils.toString(resourceLoader.getResource(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH).getInputStream());
        // Job should be waiting at User task.

        // Get the start before time.
        DateTime startBeforeTime = DateTime.now();

        // Get the running jobs before start time.
        JobSummaries jobSummaries = jobService.getRunningJobsByStartBeforeTime(startBeforeTime);
        List<JobSummary> jobSummaryList = jobSummaries.getJobSummaries();
        assertEquals(jobSummaryList.size(), 1);
        JobSummary jobSummary = jobSummaryList.get(0);
        assertEquals(JobStatusEnum.RUNNING, jobSummary.getStatus());
        assertEquals(job.getId(), jobSummary.getId());
        assertEquals(job.getJobName(), jobSummary.getJobName());
        assertEquals(job.getNamespace(), jobSummary.getNamespace());

        // Query the pending task.
        List<Task> tasks = activitiTaskService.createTaskQuery().processInstanceId(job.getId()).list();

        // Suspend the task.
        activitiService.suspendProcessInstance(tasks.get(0).getProcessInstanceId());

        // There should be no running jobs before the start time.
        jobSummaries = jobService.getRunningJobsByStartBeforeTime(startBeforeTime);
        jobSummaryList = jobSummaries.getJobSummaries();
        assertEquals(jobSummaryList.size(), 0);

        // Resume the task.
        activitiService.resumeProcessInstance(tasks.get(0).getProcessInstanceId());

        // Complete the task.
        activitiTaskService.complete(tasks.get(0).getId());
        // Job should have been completed.

        // There should be no running jobs before the start time.
        jobSummaries = jobService.getRunningJobsByStartBeforeTime(startBeforeTime);
        jobSummaryList = jobSummaries.getJobSummaries();
        assertEquals(jobSummaryList.size(), 0);
    }

    @Test
    public void testSignalJob() throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);

        // Start the job.
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        // Job should be waiting at Receive task.
        Job jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertEquals("receivetask1", jobGet.getCurrentWorkflowStep().getId());

        // Signal job to continue.
        List<Parameter> signalParameters = new ArrayList<>();
        Parameter signalPameter1 = new Parameter("UT_SIGNAL_PARAM_1", "UT_SIGNAL_VALUE_1");
        signalParameters.add(signalPameter1);

        JobSignalRequest jobSignalRequest = new JobSignalRequest(job.getId(), "receivetask1", signalParameters, null);

        Job signalJob = jobService.signalJob(jobSignalRequest);

        assertEquals(JobStatusEnum.RUNNING, signalJob.getStatus());
        assertEquals("receivetask1", signalJob.getCurrentWorkflowStep().getId());
        assertTrue(signalJob.getParameters().contains(signalPameter1));

        // Job should have been completed.
        jobGet = jobService.getJob(job.getId(), true);
        assertEquals(JobStatusEnum.COMPLETED, jobGet.getStatus());
        assertTrue(jobGet.getParameters().contains(signalPameter1));
    }

    @Test
    public void testSignalJobNoParameters() throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);

        // Start the job.
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        // Job should be waiting at Receive task.
        Job jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertEquals("receivetask1", jobGet.getCurrentWorkflowStep().getId());

        // Signal job to continue.
        JobSignalRequest jobSignalRequest = new JobSignalRequest(job.getId(), "receivetask1", null, null);

        Job signalJob = jobService.signalJob(jobSignalRequest);

        assertEquals(JobStatusEnum.RUNNING, signalJob.getStatus());
        assertEquals("receivetask1", signalJob.getCurrentWorkflowStep().getId());

        // Job should have been completed.
        jobGet = jobService.getJob(job.getId(), true);
        assertEquals(JobStatusEnum.COMPLETED, jobGet.getStatus());
    }

    @Test
    public void testSignalJobWithCheckEmrClusterTask() throws Exception
    {
        // Create a list of parameters for the job.
        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter("clusterName", EMR_CLUSTER_NAME);
        parameters.add(parameter);

        // Run a job with Activiti XML that will start cluster, check status, wait on receive task and terminate.
        Job job = jobServiceTestHelper.createJobForCreateCluster(ACTIVITI_XML_CHECK_CLUSTER_AND_RECEIVE_TASK_WITH_CLASSPATH, parameters);
        assertNotNull(job);

        // Job should be waiting at receive task.
        Job getJobResponse = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.RUNNING, getJobResponse.getStatus());
        assertEquals("receiveTask", getJobResponse.getCurrentWorkflowStep().getId());

        // Validate that create and check cluster tasks were successful.
        assertTrue(getJobResponse.getParameters().contains(new Parameter("createClusterServiceTask_taskStatus", ActivitiRuntimeHelper.TASK_STATUS_SUCCESS)));
        assertTrue(getJobResponse.getParameters().contains(new Parameter("checkClusterServiceTask_taskStatus", ActivitiRuntimeHelper.TASK_STATUS_SUCCESS)));

        // Signal job to continue.
        Parameter signalParameter = new Parameter(PARAMETER_NAME, PARAMETER_VALUE);
        JobSignalRequest jobSignalRequest = new JobSignalRequest(job.getId(), "receiveTask", Collections.singletonList(signalParameter), null);
        Job signalJobResponse = jobService.signalJob(jobSignalRequest);

        // Validate the signal job response.
        assertEquals(JobStatusEnum.RUNNING, signalJobResponse.getStatus());
        assertEquals("receiveTask", signalJobResponse.getCurrentWorkflowStep().getId());
        assertTrue(signalJobResponse.getParameters().contains(signalParameter));

        // Validate the cluster status information.
        Map<String, Parameter> jobParameters = jobServiceTestHelper.toMap(signalJobResponse.getParameters());
        assertTrue(jobParameters.containsKey("checkClusterServiceTask_emrClusterStatus_creationTime"));
        assertTrue(jobParameters.containsKey("checkClusterServiceTask_emrClusterStatus_readyTime"));
        assertTrue(jobParameters.containsKey("checkClusterServiceTask_emrClusterStatus_endTime"));

        // Job should have been completed.
        getJobResponse = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.COMPLETED, getJobResponse.getStatus());
        assertTrue(getJobResponse.getParameters().contains(signalParameter));

        // Get the process variables.
        HistoricProcessInstance historicProcessInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> processVariables = historicProcessInstance.getProcessVariables();

        // Validate the cluster status information.
        assertTrue(processVariables.containsKey("checkClusterServiceTask_emrClusterStatus_creationTime"));
        assertNotNull(processVariables.get("checkClusterServiceTask_emrClusterStatus_creationTime"));
        assertTrue(processVariables.containsKey("checkClusterServiceTask_emrClusterStatus_readyTime"));
        assertNull(processVariables.get("checkClusterServiceTask_emrClusterStatus_readyTime"));
        assertTrue(processVariables.containsKey("checkClusterServiceTask_emrClusterStatus_endTime"));
        assertNull(processVariables.get("checkClusterServiceTask_emrClusterStatus_endTime"));
    }

    @Test
    public void testSignalJobNoExists() throws Exception
    {
        // Signal job with job id that does not exist.
        try
        {
            jobService.signalJob(new JobSignalRequest("job_does_not_exist", "receivetask1", null, null));
            fail("Should throw an ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException ex)
        {
            assertEquals(String.format("No job found for matching job id: \"%s\" and receive task id: \"%s\".", "job_does_not_exist", "receivetask1"),
                ex.getMessage());
        }

        // Signal job with receive task that does not exist.
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);

        // Start the job.
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        try
        {
            // Job should be waiting at Receive task.
            Job jobGet = jobService.getJob(job.getId(), false);
            assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
            assertEquals("receivetask1", jobGet.getCurrentWorkflowStep().getId());

            jobService.signalJob(new JobSignalRequest(job.getId(), "receivetask_does_not_exist", null, null));
            fail("Should throw an ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException ex)
        {
            assertEquals(String.format("No job found for matching job id: \"%s\" and receive task id: \"%s\".", job.getId(), "receivetask_does_not_exist"),
                ex.getMessage());
        }
    }

    /**
     * Signals job with S3 properties set. Parameters should be populated from the properties.
     *
     * @throws Exception
     */
    @Test
    public void testSignalJobWithS3Properties() throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);

        Parameter parameter = new Parameter("testName", "testValue");
        S3PropertiesLocation s3PropertiesLocation = getS3PropertiesLocation("s3BucketName", "s3ObjectKey", parameter);

        // Start the job.
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        JobSignalRequest jobSignalRequest = new JobSignalRequest(job.getId(), "receivetask1", null, null);
        jobSignalRequest.setS3PropertiesLocation(s3PropertiesLocation);

        Job signalJob = jobService.signalJob(jobSignalRequest);

        assertParameterEquals(parameter, signalJob.getParameters());
    }

    /**
     * Signals job with both S3 properties and request parameters set. If there are name clashes, the request parameter should take precedence.
     *
     * @throws Exception
     */
    @Test
    public void testSignalJobWithS3PropertiesPrecedenceRequestParamsOverridesS3() throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);

        Parameter s3Parameter = new Parameter("testName", "testValue");
        Parameter requestParameter = new Parameter("testName", "expectedValue");

        S3PropertiesLocation s3PropertiesLocation = getS3PropertiesLocation("s3BucketName", "s3ObjectKey", s3Parameter);

        // Start the job.
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        JobSignalRequest jobSignalRequest = new JobSignalRequest(job.getId(), "receivetask1", null, null);
        jobSignalRequest.setS3PropertiesLocation(s3PropertiesLocation);
        jobSignalRequest.setParameters(Arrays.asList(requestParameter));

        Job signalJob = jobService.signalJob(jobSignalRequest);

        assertParameterEquals(requestParameter, signalJob.getParameters());
    }

    @Test
    public void testUpdateJob() throws Exception
    {
        // Create a test job definition.
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);

        // Create and start the job.
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        // Job should be waiting at User task.

        // Get the running job with non verbose.
        Job jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());

        // Suspend the job.
        jobService.updateJob(job.getId(), new JobUpdateRequest(JobActionEnum.SUSPEND));

        // Validate that the job is suspended.
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.SUSPENDED, jobGet.getStatus());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());

        // Resume the job.
        jobService.updateJob(job.getId(), new JobUpdateRequest(JobActionEnum.RESUME));

        // Validate that the job is running.
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());

        // Query the pending task and complete it
        List<Task> tasks = activitiTaskService.createTaskQuery().processInstanceId(job.getId()).list();

        activitiTaskService.complete(tasks.get(0).getId());

        // Job should have been completed.

        // Get the completed job with non verbose.
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.COMPLETED, jobGet.getStatus());
        assertNotNull(jobGet.getStartTime());
        assertNotNull(jobGet.getEndTime());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertNull(jobGet.getCurrentWorkflowStep());
    }

    @Test
    public void testUpdateJobMissingRequiredParameters()
    {
        // Try to update a job when job id is not specified.
        try
        {
            jobService.updateJob(BLANK_TEXT, new JobUpdateRequest(JobActionEnum.RESUME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A job id must be specified.", e.getMessage());
        }

        // Try to update a job when job update request is not specified.
        try
        {
            jobService.updateJob(INTEGER_VALUE.toString(), null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A job update request must be specified.", e.getMessage());
        }

        // Try to update a job when job update action is not specified.
        try
        {
            jobService.updateJob(INTEGER_VALUE.toString(), new JobUpdateRequest(null));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A job update action must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateJobTrimParameters() throws Exception
    {
        // Create a test job definition.
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);

        // Create and start the job.
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        // Job should be waiting at User task.

        // Get the running job with non verbose.
        Job jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());

        // Suspend the job using input parameters with leading and trailing empty spaces.
        jobService.updateJob(addWhitespace(job.getId()), new JobUpdateRequest(JobActionEnum.SUSPEND));

        // Validate that the job is suspended.
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.SUSPENDED, jobGet.getStatus());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());

        // Resume the job using input parameters with leading and trailing empty spaces.
        jobService.updateJob(addWhitespace(job.getId()), new JobUpdateRequest(JobActionEnum.RESUME));

        // Validate that the job is running.
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());

        // Query the pending task and complete it
        List<Task> tasks = activitiTaskService.createTaskQuery().processInstanceId(job.getId()).list();

        activitiTaskService.complete(tasks.get(0).getId());

        // Job should have been completed.

        // Get the completed job with non verbose.
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.COMPLETED, jobGet.getStatus());
        assertNotNull(jobGet.getStartTime());
        assertNotNull(jobGet.getEndTime());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertNull(jobGet.getCurrentWorkflowStep());
    }

    @Test
    public void testUpdateJobInvalidParameters() throws Exception
    {
        // Create a test job definition.
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);

        // Create and start the job.
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        // Job should be waiting at User task.

        // Get the running job with non verbose.
        Job jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());

        // Try to update a job using invalid job id.
        try
        {
            jobService.updateJob("I_DO_NOT_EXIST", new JobUpdateRequest(JobActionEnum.SUSPEND));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals("Job with ID \"I_DO_NOT_EXIST\" does not exist or is already completed.", e.getMessage());
        }

        // Try to resume an already running job.
        try
        {
            jobService.updateJob(job.getId(), new JobUpdateRequest(JobActionEnum.RESUME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Job with ID \"%s\" is already in an active state.", job.getId()), e.getMessage());
        }

        // Suspend the job.
        jobService.updateJob(job.getId(), new JobUpdateRequest(JobActionEnum.SUSPEND));

        // Validate that the job is suspended.
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.SUSPENDED, jobGet.getStatus());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());

        // Try to suspend an already suspended job.
        try
        {
            jobService.updateJob(job.getId(), new JobUpdateRequest(JobActionEnum.SUSPEND));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Job with ID \"%s\" is already in a suspended state.", job.getId()), e.getMessage());
        }

        // Resume the job.
        jobService.updateJob(job.getId(), new JobUpdateRequest(JobActionEnum.RESUME));

        // Validate that the job is running.
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());

        // Query the pending task and complete it
        List<Task> tasks = activitiTaskService.createTaskQuery().processInstanceId(job.getId()).list();

        activitiTaskService.complete(tasks.get(0).getId());

        // Job should have been completed.

        // Get the completed job with non verbose.
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.COMPLETED, jobGet.getStatus());
        assertNotNull(jobGet.getStartTime());
        assertNotNull(jobGet.getEndTime());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertNull(jobGet.getCurrentWorkflowStep());

        // Try to update a completed job.
        try
        {
            jobService.updateJob(job.getId(), new JobUpdateRequest(JobActionEnum.SUSPEND));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Job with ID \"%s\" does not exist or is already completed.", job.getId()), e.getMessage());
        }
    }

    /**
     * Creates a job where the definition and request has S3 properties. Both parameters should be merged.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobWithS3Properties() throws Exception
    {
        Parameter jobDefinitionS3Parameter = new Parameter("name1", "value1");
        Parameter jobCreateRequestS3Parameter = new Parameter("name2", "value2");

        String s3BucketName = "s3BucketName";
        S3PropertiesLocation jobDefinitionS3PropertiesLocation = getS3PropertiesLocation(s3BucketName, "jobDefinitionObjectKey", jobDefinitionS3Parameter);
        S3PropertiesLocation jobCreateRequestS3PropertiesLocation = getS3PropertiesLocation(s3BucketName, "jobCreationObjectKey", jobCreateRequestS3Parameter);

        Job resultJob = createJobWithParameters(jobDefinitionS3PropertiesLocation, null, jobCreateRequestS3PropertiesLocation, null);

        List<Parameter> actualParameters = resultJob.getParameters();

        assertParameterEquals(jobDefinitionS3Parameter, actualParameters);
        assertParameterEquals(jobCreateRequestS3Parameter, actualParameters);
    }

    /**
     * A Java Properties file is invalid when there is an invalid unicode reference. The service should throw a friendly error message when such case happens.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobWithS3PropertiesWithInvalidUnicodeThrows() throws Exception
    {
        Parameter jobCreateRequestS3Parameter = new Parameter("name2", "value2\\uxxxx");

        String s3BucketName = "s3BucketName";
        S3PropertiesLocation jobCreateRequestS3PropertiesLocation = getS3PropertiesLocation(s3BucketName, "jobCreationObjectKey", jobCreateRequestS3Parameter);
        String bucketName = jobCreateRequestS3PropertiesLocation.getBucketName();
        String key = jobCreateRequestS3PropertiesLocation.getKey();

        try
        {
            createJobWithParameters(null, null, jobCreateRequestS3PropertiesLocation, null);
            Assert.fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "The properties file in S3 bucket '" + bucketName + "' and key '" + key + "' is invalid.",
                e.getMessage());
        }
    }

    /**
     * Creates a job where the definition and request has S3 properties and parameters. The job create request's parameter should take precedence if there are
     * name clashes.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobWithS3PropertiesPrecedenceJobRequestParamHighestPrecedence() throws Exception
    {
        Parameter jobDefinitionS3Parameter = new Parameter("testName", "testValue1");
        Parameter jobDefinitionRequestParameter = new Parameter("testName", "testValue2");
        Parameter jobCreateRequestS3Parameter = new Parameter("testName", "testValue3");
        Parameter jobCreateRequestParameter = new Parameter("testName", "expectedValue");

        String s3BucketName = "s3BucketName";
        S3PropertiesLocation jobDefinitionS3PropertiesLocation = getS3PropertiesLocation(s3BucketName, "jobDefinitionObjectKey", jobDefinitionS3Parameter);
        S3PropertiesLocation jobCreateRequestS3PropertiesLocation =
            getS3PropertiesLocation(s3BucketName, "jobCreateRequestObjectKey", jobCreateRequestS3Parameter);

        Job resultJob =
            createJobWithParameters(jobDefinitionS3PropertiesLocation, Arrays.asList(jobDefinitionRequestParameter), jobCreateRequestS3PropertiesLocation,
                Arrays.asList(jobCreateRequestParameter));

        List<Parameter> actualParameters = resultJob.getParameters();

        assertParameterEquals(jobCreateRequestParameter, actualParameters);
    }

    /**
     * Creates a job where the definition has S3 properties and parameters and request has S3 properties. The job create request's S3 properties should take
     * precedence if there are name clashes.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobWithS3PropertiesPrecedenceJobRequestS3OverridesDefinitionParams() throws Exception
    {
        Parameter jobDefinitionS3Parameter = new Parameter("testName", "testValue1");
        Parameter jobDefinitionRequestParameter = new Parameter("testName", "testValue2");
        Parameter jobCreateRequestS3Parameter = new Parameter("testName", "expectedValue");

        String s3BucketName = "s3BucketName";
        S3PropertiesLocation jobDefinitionS3PropertiesLocation = getS3PropertiesLocation(s3BucketName, "jobDefinitionObjectKey", jobDefinitionS3Parameter);
        S3PropertiesLocation jobCreateRequestS3PropertiesLocation =
            getS3PropertiesLocation(s3BucketName, "jobCreateRequestObjectKey", jobCreateRequestS3Parameter);

        Job resultJob =
            createJobWithParameters(jobDefinitionS3PropertiesLocation, Arrays.asList(jobDefinitionRequestParameter), jobCreateRequestS3PropertiesLocation,
                null);

        List<Parameter> actualParameters = resultJob.getParameters();

        assertParameterEquals(jobCreateRequestS3Parameter, actualParameters);
    }

    /**
     * Creates a job where the definition has S3 properties and parameters and request has no parameters. The job definition parameters should take precedence
     * if there are name clashes.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobWithS3PropertiesPrecedenceDefinitionParamsOverridesDefinitionS3() throws Exception
    {
        Parameter jobDefinitionS3Parameter = new Parameter("testName", "testValue1");
        Parameter jobDefinitionRequestParameter = new Parameter("testName", "expectedValue");

        String s3BucketName = "s3BucketName";
        S3PropertiesLocation jobDefinitionS3PropertiesLocation = getS3PropertiesLocation(s3BucketName, "jobDefinitionObjectKey", jobDefinitionS3Parameter);

        Job resultJob = createJobWithParameters(jobDefinitionS3PropertiesLocation, Arrays.asList(jobDefinitionRequestParameter), null, null);

        List<Parameter> actualParameters = resultJob.getParameters();

        assertParameterEquals(jobDefinitionRequestParameter, actualParameters);
    }

    /**
     * Creates a job where the definition's S3 object key does not exist. It should throw a not found exception.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobWithS3PropertiesDefinitionObjectKeyNotFoundThrows() throws Exception
    {
        Parameter jobDefinitionS3Parameter = new Parameter("name1", "value1");
        Parameter jobCreateRequestS3Parameter = new Parameter("name2", "value2");

        String s3BucketName = "s3BucketName";
        S3PropertiesLocation jobDefinitionS3PropertiesLocation = getS3PropertiesLocation(s3BucketName, "jobDefinitionObjectKey", jobDefinitionS3Parameter);
        S3PropertiesLocation jobCreateRequestS3PropertiesLocation = getS3PropertiesLocation(s3BucketName, "jobCreationObjectKey", jobCreateRequestS3Parameter);

        jobDefinitionS3PropertiesLocation.setKey("NOT_FOUND");

        try
        {
            createJobWithParameters(jobDefinitionS3PropertiesLocation, null, jobCreateRequestS3PropertiesLocation, null);
            Assert.fail("expected ObjectNotFoundException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "Specified S3 object key '" + jobDefinitionS3PropertiesLocation.getKey() + "' does not exist.",
                e.getMessage());
        }
    }

    /**
     * Creates a job where the request's S3 object key does not exist. It should throw a not found exception.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobWithS3PropertiesCreateRequestObjectKeyNotFoundThrows() throws Exception
    {
        Parameter jobDefinitionS3Parameter = new Parameter("name1", "value1");
        Parameter jobCreateRequestS3Parameter = new Parameter("name2", "value2");

        String s3BucketName = "s3BucketName";
        S3PropertiesLocation jobDefinitionS3PropertiesLocation = getS3PropertiesLocation(s3BucketName, "jobDefinitionObjectKey", jobDefinitionS3Parameter);
        S3PropertiesLocation jobCreateRequestS3PropertiesLocation = getS3PropertiesLocation(s3BucketName, "jobCreationObjectKey", jobCreateRequestS3Parameter);

        jobCreateRequestS3PropertiesLocation.setKey("NOT_FOUND");

        try
        {
            createJobWithParameters(jobDefinitionS3PropertiesLocation, null, jobCreateRequestS3PropertiesLocation, null);
            Assert.fail("expected ObjectNotFoundException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "Specified S3 object key '" + jobCreateRequestS3PropertiesLocation.getKey() + "' does not exist.",
                e.getMessage());
        }
    }

    /**
     * Creates a job where the request's S3 properties is given, but bucket name is not. It should throw a bad request exception.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobWithS3PropertiesValidationBucketNameRequired() throws Exception
    {
        Parameter jobCreateRequestS3Parameter = new Parameter("name2", "value2");

        String s3BucketName = "s3BucketName";
        S3PropertiesLocation jobCreateRequestS3PropertiesLocation = getS3PropertiesLocation(s3BucketName, "jobCreationObjectKey", jobCreateRequestS3Parameter);
        jobCreateRequestS3PropertiesLocation.setBucketName(null);

        try
        {
            createJobWithParameters(null, null, jobCreateRequestS3PropertiesLocation, null);
            Assert.fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "S3 properties location bucket name must be specified.", e.getMessage());
        }
    }

    /**
     * Creates a job where the request's S3 properties is given, but object key is not. It should throw a bad request exception.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobWithS3PropertiesValidationObjectKeyRequired() throws Exception
    {
        Parameter jobCreateRequestS3Parameter = new Parameter("name2", "value2");

        String s3BucketName = "s3BucketName";
        S3PropertiesLocation jobCreateRequestS3PropertiesLocation = getS3PropertiesLocation(s3BucketName, "jobCreationObjectKey", jobCreateRequestS3Parameter);
        jobCreateRequestS3PropertiesLocation.setKey(null);

        try
        {
            createJobWithParameters(null, null, jobCreateRequestS3PropertiesLocation, null);
            Assert.fail("expected IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "S3 properties location object key must be specified.", e.getMessage());
        }
    }

    /**
     * Tests an edge case where the job definition was persisted with some S3 properties location, but due to some datafix, the S3 properties location's object
     * key was removed, but not the bucket name. The service should still work, it would simply ignore the definition's S3 properties location.
     *
     * @throws Exception
     */
    @Test
    public void testCreateJobWithS3PropertiesJobDefinitionWrongDatafixSafety() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create a job definition create request using hard coded test values.
        JobDefinitionCreateRequest jobDefinitionCreateRequest = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();
        jobDefinitionCreateRequest.setS3PropertiesLocation(getS3PropertiesLocation("testBucketName", "testObjectKey", new Parameter("testName", "testValue")));
        jobDefinitionCreateRequest.setParameters(null);

        // Create the job definition.
        JobDefinition jobDefinition = jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);
        Long jobDefinitionId = jobDefinition.getId();
        JobDefinitionEntity jobDefinitionEntity = herdDao.findById(JobDefinitionEntity.class, jobDefinitionId);

        jobDefinitionEntity.setS3ObjectKey(null);

        // Create a job create request using hard coded test values.
        JobCreateRequest jobCreateRequest = jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);
        jobCreateRequest.setParameters(null);

        // Create the job.
        Job resultJob = jobService.createAndStartJob(jobCreateRequest);

        Assert.assertNotNull("resultJob parameters", resultJob.getParameters());
    }

    /**
     * Asserts that the deleteJob call will move the job to completion, and add a record in the history instance with the specified delete reason.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteJob() throws Exception
    {
        // Start a job that will wait in a receive task
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        // Create a job delete request
        JobDeleteRequest jobDeleteRequest = new JobDeleteRequest();
        jobDeleteRequest.setDeleteReason("test delete reason");
        Job deleteJobResponse = jobService.deleteJob(job.getId(), jobDeleteRequest);

        // Assert delete job response
        assertEquals(job.getId(), deleteJobResponse.getId());
        assertEquals(job.getJobName(), deleteJobResponse.getJobName());
        assertEquals(job.getNamespace(), deleteJobResponse.getNamespace());
        assertEquals(JobStatusEnum.COMPLETED, deleteJobResponse.getStatus());
        assertEquals(jobDeleteRequest.getDeleteReason(), deleteJobResponse.getDeleteReason());

        // Assert historic process instance
        HistoricProcessInstance historicProcessInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).singleResult();
        assertNotNull(historicProcessInstance);
        assertEquals(jobDeleteRequest.getDeleteReason(), historicProcessInstance.getDeleteReason());
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void testDeleteJobActiveJobWithMultipleSubProcesses() throws Exception
    {
        // Create and persist a test job definition.
        executeJdbcTestHelper
            .prepareHerdDatabaseForExecuteJdbcWithReceiveTaskTest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, ACTIVITI_XML_TEST_MULTIPLE_SUB_PROCESSES);

        try
        {
            // Get the job definition entity and ensure it exists.
            JobDefinitionEntity jobDefinitionEntity = jobDefinitionDao.getJobDefinitionByAltKey(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);
            assertNotNull(jobDefinitionEntity);

            // Get the process definition id.
            String processDefinitionId = jobDefinitionEntity.getActivitiId();

            // Build the parameters map.
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("counter", 0);

            // Start the job.
            ProcessInstance processInstance = activitiService.startProcessInstanceByProcessDefinitionId(processDefinitionId, parameters);
            assertNotNull(processInstance);

            // Get the process instance id for this job.
            String processInstanceId = processInstance.getProcessInstanceId();

            // Wait for all processes to become active - we expect to have the main process along with 800 sub-processes.
            waitUntilActiveProcessesThreshold(processDefinitionId, 801);

            // Get the job and validate that it is RUNNING.
            Job getJobResponse = jobService.getJob(processInstanceId, true);
            assertNotNull(getJobResponse);
            assertEquals(JobStatusEnum.RUNNING, getJobResponse.getStatus());

            // Delete the job and validate the response.
            Job deleteJobResponse = jobService.deleteJob(processInstanceId, new JobDeleteRequest(ACTIVITI_JOB_DELETE_REASON));
            assertEquals(JobStatusEnum.COMPLETED, deleteJobResponse.getStatus());
            assertEquals(ACTIVITI_JOB_DELETE_REASON, deleteJobResponse.getDeleteReason());

            // Validate the historic process instance.
            HistoricProcessInstance historicProcessInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(processInstanceId).singleResult();
            assertNotNull(historicProcessInstance);
            assertEquals(ACTIVITI_JOB_DELETE_REASON, historicProcessInstance.getDeleteReason());
        }
        finally
        {
            // Clean up the Herd database.
            executeJdbcTestHelper.cleanUpHerdDatabaseAfterExecuteJdbcWithReceiveTaskTest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);

            // Clean up the Activiti.
            deleteActivitiDeployments();
        }
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void testDeleteJobSuspendedJobWithMultipleSubProcesses() throws Exception
    {
        // Create and persist a test job definition.
        executeJdbcTestHelper
            .prepareHerdDatabaseForExecuteJdbcWithReceiveTaskTest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME, ACTIVITI_XML_TEST_MULTIPLE_SUB_PROCESSES);

        try
        {
            // Get the job definition entity and ensure it exists.
            JobDefinitionEntity jobDefinitionEntity = jobDefinitionDao.getJobDefinitionByAltKey(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);
            assertNotNull(jobDefinitionEntity);

            // Get the process definition id.
            String processDefinitionId = jobDefinitionEntity.getActivitiId();

            // Build the parameters map.
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("counter", 0);

            // Start the job.
            ProcessInstance processInstance = activitiService.startProcessInstanceByProcessDefinitionId(processDefinitionId, parameters);
            assertNotNull(processInstance);

            // Get the process instance id for this job.
            String processInstanceId = processInstance.getProcessInstanceId();

            // Wait for all processes to become active - we expect to have the main process along with 800 sub-processes.
            waitUntilActiveProcessesThreshold(processDefinitionId, 801);

            // Get the job and validate that it is RUNNING.
            Job getJobResponse = jobService.getJob(processInstanceId, true);
            assertNotNull(getJobResponse);
            assertEquals(JobStatusEnum.RUNNING, getJobResponse.getStatus());

            // Suspend the job.
            jobService.updateJob(processInstanceId, new JobUpdateRequest(JobActionEnum.SUSPEND));

            // Get the job again and validate that it is now SUSPENDED.
            getJobResponse = jobService.getJob(processInstanceId, true);
            assertNotNull(getJobResponse);
            assertEquals(JobStatusEnum.SUSPENDED, getJobResponse.getStatus());

            // Delete the job in suspended state and validate the response.
            Job deleteJobResponse = jobService.deleteJob(processInstanceId, new JobDeleteRequest(ACTIVITI_JOB_DELETE_REASON));
            assertEquals(JobStatusEnum.COMPLETED, deleteJobResponse.getStatus());
            assertEquals(ACTIVITI_JOB_DELETE_REASON, deleteJobResponse.getDeleteReason());

            // Validate the historic process instance.
            HistoricProcessInstance historicProcessInstance =
                activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(processInstanceId).singleResult();
            assertNotNull(historicProcessInstance);
            assertEquals(ACTIVITI_JOB_DELETE_REASON, historicProcessInstance.getDeleteReason());
        }
        finally
        {
            // Clean up the Herd database.
            executeJdbcTestHelper.cleanUpHerdDatabaseAfterExecuteJdbcWithReceiveTaskTest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);

            // Clean up the Activiti.
            deleteActivitiDeployments();
        }
    }

    /**
     * Blocks the current calling thread until number of active processes for the process definition id reaches the specified threshold. This method will
     * timeout with an assertion error if the waiting takes longer than 15,000 ms. This is a reasonable amount of time for the JUnits that use this method.
     *
     * @param processDefinitionId the process definition id
     * @param activeProcessesThreshold the threshold for the number of active processes
     */
    private void waitUntilActiveProcessesThreshold(String processDefinitionId, int activeProcessesThreshold) throws Exception
    {
        // Set the start time.
        long startTime = System.currentTimeMillis();

        // Create a process instance query for the active sub-processes.
        ProcessInstanceQuery processInstanceQuery = activitiRuntimeService.createProcessInstanceQuery().processDefinitionId(processDefinitionId).active();

        // Get the current count of the active processes.
        long activeProcessesCount = processInstanceQuery.count();

        // Run while there are less active processes than the specified threshold.
        while (activeProcessesCount < activeProcessesThreshold)
        {
            // Get the elapsed time.
            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - startTime;

            // If time spent waiting is longer than 15,000 ms
            if (elapsedTime > 15000)
            {
                // Dump the current runtime variables into the error log to make it easier to debug
                StringBuilder builder = new StringBuilder("Dumping workflow variables due to error:\n");
                builder.append("Process definition id: ").append(processDefinitionId).append('\n');
                builder.append("Active processes threshold: ").append(activeProcessesThreshold).append('\n');
                builder.append("Number of active processes: ").append(activeProcessesCount).append('\n');
                List<Execution> executions = activitiRuntimeService.createExecutionQuery().list();
                builder.append("Total number of executions: ").append(executions.size()).append('\n');
                for (Execution execution : executions)
                {
                    builder.append("Execution - ").append(execution).append(":\n");
                    builder.append("    execution.getId():").append(execution.getId()).append('\n');
                    builder.append("    execution.getActivityId():").append(execution.getActivityId()).append('\n');
                    builder.append("    execution.getParentId():").append(execution.getParentId()).append('\n');
                    builder.append("    execution.getProcessInstanceId():").append(execution.getProcessInstanceId()).append('\n');
                    builder.append("    execution.isEnded():").append(execution.isEnded()).append('\n');
                    builder.append("    execution.isSuspended():").append(execution.isSuspended()).append('\n');
                    Map<String, Object> executionVariables = activitiRuntimeService.getVariables(execution.getId());
                    for (Map.Entry<String, Object> variable : executionVariables.entrySet())
                    {
                        builder.append("    ").append(variable).append('\n');
                    }
                }
                LOGGER.error(builder.toString());

                // Fail assertion
                fail("The test did not finished in the specified timeout (15s). See error logs for variable dump.");
            }

            // Sleep for 100 ms.
            Thread.sleep(100);

            // Update the current count of the active processes.
            activeProcessesCount = processInstanceQuery.count();
        }
    }

    /**
     * Asserts that the deleteJob call will throw an error when delete reason is blank.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteJobAssertErrorDeleteReasonBlank() throws Exception
    {
        // Start a job that will wait in a receive task
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        // Create a job delete request
        JobDeleteRequest jobDeleteRequest = new JobDeleteRequest();
        jobDeleteRequest.setDeleteReason(BLANK_TEXT);
        try
        {
            jobService.deleteJob(job.getId(), jobDeleteRequest);
        }
        catch (Exception e)
        {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("deleteReason must be specified", e.getMessage());
        }
    }

    /**
     * Asserts that the deleteJob call will throw an error when specified job does not exist.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteJobAssertErrorJobDoesNotExist() throws Exception
    {
        // Create a job delete request
        JobDeleteRequest jobDeleteRequest = new JobDeleteRequest();
        jobDeleteRequest.setDeleteReason("test delete reason");
        try
        {
            jobService.deleteJob("DOES_NOT_EXIST", jobDeleteRequest);
        }
        catch (Exception e)
        {
            assertEquals(ObjectNotFoundException.class, e.getClass());
            assertEquals("Job with ID \"DOES_NOT_EXIST\" does not exist or is already completed.", e.getMessage());
        }
    }

    @Test
    public void testDeleteJobAssertAccessDeniedWhenUserHasNoPermissions() throws Exception
    {
        // Start a job that will wait in a receive task
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String username = "username";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(username);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(username, "password", false, false, false, false, Collections.emptyList(), applicationUser),
                null));

        try
        {
            jobService.deleteJob(job.getId(), new JobDeleteRequest("test delete reason"));
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[EXECUTE]\" permission(s) to the namespace \"%s\"", username, TEST_ACTIVITI_NAMESPACE_CD),
                e.getMessage());
        }
    }

    @Test
    public void testDeleteJobAssertNoErrorWhenUserHasPermissions() throws Exception
    {
        // Start a job that will wait in a receive task
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String username = "username";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(username);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        applicationUser.getNamespaceAuthorizations()
            .add(new NamespaceAuthorization(TEST_ACTIVITI_NAMESPACE_CD, Arrays.asList(NamespacePermissionEnum.EXECUTE)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(username, "password", false, false, false, false, Collections.emptyList(), applicationUser),
                null));

        try
        {
            jobService.deleteJob(job.getId(), new JobDeleteRequest("test delete reason"));
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    @Test
    public void testGetJobAssertAccessDeniedGivenJobCompletedAndUserDoesNotHavePermissions() throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(null);
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String username = "username";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(username);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(username, "password", false, false, false, false, Collections.emptyList(), applicationUser),
                null));

        try
        {
            jobService.getJob(job.getId(), false);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[READ]\" permission(s) to the namespace \"%s\"", username, TEST_ACTIVITI_NAMESPACE_CD),
                e.getMessage());
        }
    }

    @Test
    public void testGetJobAssertNoErrorGivenJobCompletedAndUserDoesHasPermissions() throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(null);
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String username = "username";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(username);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization(TEST_ACTIVITI_NAMESPACE_CD, Arrays.asList(NamespacePermissionEnum.READ)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(username, "password", false, false, false, false, Collections.emptyList(), applicationUser),
                null));

        try
        {
            jobService.getJob(job.getId(), false);
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    @Test
    public void testGetJobAssertAccessDeniedGivenJobRunningAndUserDoesNotHavePermissions() throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String username = "username";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(username);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(username, "password", false, false, false, false, Collections.emptyList(), applicationUser),
                null));

        try
        {
            jobService.getJob(job.getId(), false);
            fail();
        }
        catch (Exception e)
        {
            assertEquals(AccessDeniedException.class, e.getClass());
            assertEquals(String.format("User \"%s\" does not have \"[READ]\" permission(s) to the namespace \"%s\"", username, TEST_ACTIVITI_NAMESPACE_CD),
                e.getMessage());
        }
    }

    @Test
    public void testGetJobAssertNoErrorGivenJobRunningAndUserDoesHasPermissions() throws Exception
    {
        jobDefinitionServiceTestHelper.createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String username = "username";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(username);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        applicationUser.getNamespaceAuthorizations().add(new NamespaceAuthorization(TEST_ACTIVITI_NAMESPACE_CD, Arrays.asList(NamespacePermissionEnum.READ)));
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(username, "password", false, false, false, false, Collections.emptyList(), applicationUser),
                null));

        try
        {
            jobService.getJob(job.getId(), false);
        }
        catch (AccessDeniedException e)
        {
            fail();
        }
    }

    /**
     * Puts a Java properties into S3 where the key-value is the given parameter. Returns a {@link S3PropertiesLocation} with the S3 info.
     *
     * @param s3BucketName the S3 bucket name
     * @param s3ObjectKey the S3 object key
     * @param parameter the parameter
     *
     * @return the S3 properties location
     */
    private S3PropertiesLocation getS3PropertiesLocation(String s3BucketName, String s3ObjectKey, Parameter parameter)
    {
        putParameterIntoS3(s3BucketName, s3ObjectKey, parameter);

        S3PropertiesLocation jobDefinitionS3PropertiesLocation = new S3PropertiesLocation();
        jobDefinitionS3PropertiesLocation.setBucketName(s3BucketName);
        jobDefinitionS3PropertiesLocation.setKey(s3ObjectKey);
        return jobDefinitionS3PropertiesLocation;
    }

    /**
     * Creates a new job definition, and a job using the default values and given parameters configurations.
     *
     * @param jobDefinitionS3PropertiesLocation the S3 properties location for the job definition
     * @param jobDefinitionParameters the job definition parameters
     * @param jobCreateRequestS3PropertiesLocation the S3 properties location for the job create request
     * @param jobCreateRequestParameters the job create request parameters
     *
     * @return the job
     * @throws Exception
     */
    private Job createJobWithParameters(S3PropertiesLocation jobDefinitionS3PropertiesLocation, List<Parameter> jobDefinitionParameters,
        S3PropertiesLocation jobCreateRequestS3PropertiesLocation, List<Parameter> jobCreateRequestParameters) throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create a job definition create request using hard coded test values.
        JobDefinitionCreateRequest jobDefinitionCreateRequest = jobDefinitionServiceTestHelper.createJobDefinitionCreateRequest();
        jobDefinitionCreateRequest.setParameters(jobDefinitionParameters);
        jobDefinitionCreateRequest.setS3PropertiesLocation(jobDefinitionS3PropertiesLocation);

        // Create the job definition.
        jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);

        // Create a job create request using hard coded test values.
        JobCreateRequest jobCreateRequest = jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);
        jobCreateRequest.setParameters(jobCreateRequestParameters);
        jobCreateRequest.setS3PropertiesLocation(jobCreateRequestS3PropertiesLocation);

        // Create the job.
        return jobService.createAndStartJob(jobCreateRequest);
    }

    /**
     * Asserts that the given expected parameter exists and value matches from the given collection of parameters.
     *
     * @param expectedParameter the expected parameter
     * @param actualParameters the actual parameter
     */
    private void assertParameterEquals(Parameter expectedParameter, List<Parameter> actualParameters)
    {
        String name = expectedParameter.getName();
        Parameter actualParameter = getParameter(name, actualParameters);
        Assert.assertNotNull("parameter ['" + name + "'] not found", actualParameter);
        Assert.assertEquals("parameter ['" + name + "'] value", actualParameter.getValue(), expectedParameter.getValue());
    }

    /**
     * Puts a Java properties file content into S3. The Properties contains a single key-value defined by the given parameter object.
     *
     * @param s3BucketName the S3 bucket name
     * @param s3ObjectKey the S3 object key
     * @param parameter the parameter
     */
    private void putParameterIntoS3(String s3BucketName, String s3ObjectKey, Parameter parameter)
    {
        String content = parameter.getName() + "=" + parameter.getValue();
        byte[] bytes = content.getBytes();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length());

        PutObjectRequest putObjectRequest = new PutObjectRequest(s3BucketName, s3ObjectKey, inputStream, metadata);
        s3Operations.putObject(putObjectRequest, null);
    }

    /**
     * Gets a parameter by name from the give collection of parameters. Returns null if the parameter does not exist.
     *
     * @param name the name of the parameter
     * @param parameters the collection of the parameters
     *
     * @return the parameter with the specified name or null is not found
     */
    private Parameter getParameter(String name, Collection<Parameter> parameters)
    {
        for (Parameter parameter : parameters)
        {
            if (name.equals(parameter.getName()))
            {
                return parameter;
            }
        }
        return null;
    }

    /**
     * Validates the job summaries per specified map of expected job ids to their relative statuses.
     *
     * @param expectedJobStatuses the map of expected job ids to their relative statuses
     * @param actualJobSummaries the job summaries to be validated
     */
    private void validateJobSummaries(Map<String, JobStatusEnum> expectedJobStatuses, JobSummaries actualJobSummaries)
    {
        // Build the mapping of the actual job ids to job statuses.
        Map<String, JobStatusEnum> actualJobStatuses = new HashMap<>();
        for (JobSummary actualJobSummary : actualJobSummaries.getJobSummaries())
        {
            actualJobStatuses.put(actualJobSummary.getId(), actualJobSummary.getStatus());
        }

        // Compare the expected and actual mappings.
        assertEquals(expectedJobStatuses, actualJobStatuses);
    }
}
