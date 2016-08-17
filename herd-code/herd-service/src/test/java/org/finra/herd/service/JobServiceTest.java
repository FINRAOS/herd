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
import java.util.HashSet;
import java.util.List;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.task.Task;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.JobDefinitionCreateRequest;
import org.finra.herd.model.api.xml.JobDeleteRequest;
import org.finra.herd.model.api.xml.JobSignalRequest;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.api.xml.S3PropertiesLocation;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;
import org.finra.herd.model.jpa.JobDefinitionEntity;

/**
 * This class tests various functionality within the Job REST controller.
 */
public class JobServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateJob() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create a job definition create request using hard coded test values.
        JobDefinitionCreateRequest jobDefinitionCreateRequest = createJobDefinitionCreateRequest();

        // Create the job definition.
        jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);

        // Create a job create request using hard coded test values.
        JobCreateRequest jobCreateRequest = createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);

        // Create the job.
        Job resultJob = jobService.createAndStartJob(jobCreateRequest);

        // Validate the results.
        assertNotNull(resultJob);
        assertNotNull(resultJob.getId());
        assertTrue(!resultJob.getId().isEmpty());
        assertEquals(TEST_ACTIVITI_NAMESPACE_CD, resultJob.getNamespace());
        assertEquals(TEST_ACTIVITI_JOB_NAME, resultJob.getJobName());
        assertEquals(jobDefinitionCreateRequest.getParameters().size() + jobCreateRequest.getParameters().size(), resultJob.getParameters().size());
        List<String> expectedParameters = new ArrayList<>();
        expectedParameters.addAll(parametersToStringList(jobDefinitionCreateRequest.getParameters()));
        expectedParameters.addAll(parametersToStringList(jobCreateRequest.getParameters()));
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
        JobDefinitionCreateRequest jobDefinitionCreateRequest = createJobDefinitionCreateRequest();
        jobDefinitionCreateRequest.setParameters(null);

        // Create the job definition.
        jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);

        // Create a job create request using hard coded test values.
        JobCreateRequest jobCreateRequest = createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);
        jobCreateRequest.setParameters(null);

        // Create the job.
        Job resultJob = jobService.createAndStartJob(jobCreateRequest);

        // Validate the results.
        assertNotNull(resultJob);
        assertNotNull(resultJob.getId());
        assertTrue(!resultJob.getId().isEmpty());
        assertEquals(TEST_ACTIVITI_NAMESPACE_CD, resultJob.getNamespace());
        assertEquals(TEST_ACTIVITI_JOB_NAME, resultJob.getJobName());
        assertNull(resultJob.getParameters());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateJobNamespaceEmpty() throws Exception
    {
        // Try to create a job by passing an empty namespace code.
        jobService.createAndStartJob(createJobCreateRequest(" ", TEST_ACTIVITI_JOB_NAME));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateJobJobNameEmpty() throws Exception
    {
        // Try to create a job by passing an empty job name.
        jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, " "));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateJobParameterNameEmpty() throws Exception
    {
        // Create a job create request using hard coded test values.
        JobCreateRequest jobCreateRequest = createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);

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
            jobService.createAndStartJob(createJobCreateRequest(addSlash(TEST_ACTIVITI_NAMESPACE_CD), TEST_ACTIVITI_JOB_NAME));
            fail("Should throw an IllegalArgumentException when namespace contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a job when job name name contains a forward slash character.
        try
        {
            jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, addSlash(TEST_ACTIVITI_JOB_NAME)));
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
        JobCreateRequest jobCreateRequest = createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);

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
        jobService.createAndStartJob(createJobCreateRequest("I_DO_NOT_EXIST", TEST_ACTIVITI_JOB_NAME));
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testCreateJobJobNameNoExists() throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Try to create a job using non-existing job definition name.
        jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, "I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetJob() throws Exception
    {
        createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);

        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String activitiXml = IOUtils.toString(resourceLoader.getResource(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH).getInputStream());
        // Job should be waiting at User task.

        // Running job with verbose
        Job jobGet = jobService.getJob(job.getId(), true);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertNotNull(jobGet.getActivitiJobXml());
        assertEquals(activitiXml, jobGet.getActivitiJobXml());
        assertTrue(jobGet.getCompletedWorkflowSteps().size() > 0);
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());

        // Running job with non verbose
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.RUNNING, jobGet.getStatus());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertEquals("usertask1", jobGet.getCurrentWorkflowStep().getId());

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

        // Completed job with non verbose
        jobGet = jobService.getJob(job.getId(), false);
        assertEquals(JobStatusEnum.COMPLETED, jobGet.getStatus());
        assertNotNull(jobGet.getStartTime());
        assertNotNull(jobGet.getEndTime());
        assertNull(jobGet.getActivitiJobXml());
        assertTrue(CollectionUtils.isEmpty(jobGet.getCompletedWorkflowSteps()));
        assertNull(jobGet.getCurrentWorkflowStep());
    }

    @Test
    public void testGetJobIntermediateTimer() throws Exception
    {
        createJobDefinition(ACTIVITI_XML_HERD_INTERMEDIATE_TIMER_WITH_CLASSPATH);

        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

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
    public void testSignalJob() throws Exception
    {
        createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);

        // Start the job.
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

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
        createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);

        // Start the job.
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

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
        createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);

        // Start the job.
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

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
        createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);

        Parameter parameter = new Parameter("testName", "testValue");
        S3PropertiesLocation s3PropertiesLocation = getS3PropertiesLocation("s3BucketName", "s3ObjectKey", parameter);

        // Start the job.
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

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
        createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);

        Parameter s3Parameter = new Parameter("testName", "testValue");
        Parameter requestParameter = new Parameter("testName", "expectedValue");

        S3PropertiesLocation s3PropertiesLocation = getS3PropertiesLocation("s3BucketName", "s3ObjectKey", s3Parameter);

        // Start the job.
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        JobSignalRequest jobSignalRequest = new JobSignalRequest(job.getId(), "receivetask1", null, null);
        jobSignalRequest.setS3PropertiesLocation(s3PropertiesLocation);
        jobSignalRequest.setParameters(Arrays.asList(requestParameter));

        Job signalJob = jobService.signalJob(jobSignalRequest);

        assertParameterEquals(requestParameter, signalJob.getParameters());
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
        JobDefinitionCreateRequest jobDefinitionCreateRequest = createJobDefinitionCreateRequest();
        jobDefinitionCreateRequest.setS3PropertiesLocation(getS3PropertiesLocation("testBucketName", "testObjectKey", new Parameter("testName", "testValue")));
        jobDefinitionCreateRequest.setParameters(null);

        // Create the job definition.
        JobDefinition jobDefinition = jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);
        Integer jobDefinitionId = jobDefinition.getId();
        JobDefinitionEntity jobDefinitionEntity = herdDao.findById(JobDefinitionEntity.class, jobDefinitionId);

        jobDefinitionEntity.setS3ObjectKey(null);

        // Create a job create request using hard coded test values.
        JobCreateRequest jobCreateRequest = createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);
        jobCreateRequest.setParameters(null);

        // Create the job.
        Job resultJob = jobService.createAndStartJob(jobCreateRequest);

        Assert.assertNull("resultJob parameters", resultJob.getParameters());
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
        createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        // Create a job delete request
        JobDeleteRequest jobDeleteRequest = new JobDeleteRequest();
        jobDeleteRequest.setDeleteReason("test delete reason");
        Job deleteJobResponse = jobService.deleteJob(job.getId(), jobDeleteRequest);

        // Assert delete job response
        assertEquals(job.getId(), deleteJobResponse.getId());
        assertNull(deleteJobResponse.getNamespace());
        assertNull(deleteJobResponse.getJobName());
        assertEquals(JobStatusEnum.COMPLETED, deleteJobResponse.getStatus());
        assertEquals(jobDeleteRequest.getDeleteReason(), deleteJobResponse.getDeleteReason());

        // Assert historic process instance
        HistoricProcessInstance historicProcessInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).singleResult();
        assertNotNull(historicProcessInstance);
        assertEquals(jobDeleteRequest.getDeleteReason(), historicProcessInstance.getDeleteReason());
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
        createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

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
        createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String username = "username";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(username);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(username, "password", false, false, false, false, Collections.emptyList(), applicationUser),
                null));

        try
        {
            try
            {
                jobService.deleteJob(job.getId(), new JobDeleteRequest("test delete reason"));
                fail();
            }
            catch (Exception e)
            {
                assertEquals(AccessDeniedException.class, e.getClass());
                assertEquals(
                    String.format("User \"%s\" does not have \"[EXECUTE]\" permission(s) to the namespace \"%s\"", username, TEST_ACTIVITI_NAMESPACE_CD),
                    e.getMessage());
            }
        }
        finally
        {
            SecurityContextHolder.clearContext();
        }
    }

    @Test
    public void testDeleteJobAssertNoErrorWhenUserHasPermissions() throws Exception
    {
        // Start a job that will wait in a receive task
        createJobDefinition(ACTIVITI_XML_TEST_RECEIVE_TASK_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

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
            try
            {
                jobService.deleteJob(job.getId(), new JobDeleteRequest("test delete reason"));
            }
            catch (AccessDeniedException e)
            {
                fail();
            }
        }
        finally
        {
            SecurityContextHolder.clearContext();
        }
    }

    @Test
    public void testGetJobAssertAccessDeniedGivenJobCompletedAndUserDoesNotHavePermissions() throws Exception
    {
        createJobDefinition(null);
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String username = "username";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(username);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(username, "password", false, false, false, false, Collections.emptyList(), applicationUser),
                null));
        try
        {
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
        finally
        {
            SecurityContextHolder.clearContext();
        }
    }

    @Test
    public void testGetJobAssertNoErrorGivenJobCompletedAndUserDoesHasPermissions() throws Exception
    {
        createJobDefinition(null);
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

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
            try
            {
                jobService.getJob(job.getId(), false);
            }
            catch (AccessDeniedException e)
            {
                fail();
            }
        }
        finally
        {
            SecurityContextHolder.clearContext();
        }
    }

    @Test
    public void testGetJobAssertAccessDeniedGivenJobRunningAndUserDoesNotHavePermissions() throws Exception
    {
        createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

        String username = "username";
        ApplicationUser applicationUser = new ApplicationUser(getClass());
        applicationUser.setUserId(username);
        applicationUser.setNamespaceAuthorizations(new HashSet<>());
        SecurityContextHolder.getContext().setAuthentication(
            new TestingAuthenticationToken(new SecurityUserWrapper(username, "password", false, false, false, false, Collections.emptyList(), applicationUser),
                null));
        try
        {
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
        finally
        {
            SecurityContextHolder.clearContext();
        }
    }

    @Test
    public void testGetJobAssertNoErrorGivenJobRunningAndUserDoesHasPermissions() throws Exception
    {
        createJobDefinition(ACTIVITI_XML_TEST_USER_TASK_WITH_CLASSPATH);
        Job job = jobService.createAndStartJob(createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME));

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
            try
            {
                jobService.getJob(job.getId(), false);
            }
            catch (AccessDeniedException e)
            {
                fail();
            }
        }
        finally
        {
            SecurityContextHolder.clearContext();
        }
    }

    /**
     * Puts a Java properties into S3 where the key-value is the given parameter. Returns a {@link S3PropertiesLocation} with the S3 info.
     *
     * @param s3BucketName
     * @param s3ObjectKey
     * @param parameter
     *
     * @return
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
     * @param jobDefinitionS3PropertiesLocation
     * @param jobDefinitionParameters
     * @param jobCreateRequestS3PropertiesLocation
     * @param jobCreateRequestParameters
     *
     * @return
     * @throws Exception
     */
    private Job createJobWithParameters(S3PropertiesLocation jobDefinitionS3PropertiesLocation, List<Parameter> jobDefinitionParameters,
        S3PropertiesLocation jobCreateRequestS3PropertiesLocation, List<Parameter> jobCreateRequestParameters) throws Exception
    {
        // Create the namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create a job definition create request using hard coded test values.
        JobDefinitionCreateRequest jobDefinitionCreateRequest = createJobDefinitionCreateRequest();
        jobDefinitionCreateRequest.setParameters(jobDefinitionParameters);
        jobDefinitionCreateRequest.setS3PropertiesLocation(jobDefinitionS3PropertiesLocation);

        // Create the job definition.
        jobDefinitionService.createJobDefinition(jobDefinitionCreateRequest, false);

        // Create a job create request using hard coded test values.
        JobCreateRequest jobCreateRequest = createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);
        jobCreateRequest.setParameters(jobCreateRequestParameters);
        jobCreateRequest.setS3PropertiesLocation(jobCreateRequestS3PropertiesLocation);

        // Create the job.
        return jobService.createAndStartJob(jobCreateRequest);
    }

    /**
     * Asserts that the given expected parameter exists and value matches from the given collection of parameters.
     *
     * @param expectedParameter
     * @param actualParameters
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
     * @param s3BucketName
     * @param s3ObjectKey
     * @param parameter
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
     * @param name
     * @param parameters
     *
     * @return
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
}