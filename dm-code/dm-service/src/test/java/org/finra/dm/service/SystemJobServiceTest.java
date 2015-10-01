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
package org.finra.dm.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.model.api.xml.SystemJobRunRequest;
import org.finra.dm.model.api.xml.SystemJobRunResponse;
import org.finra.dm.service.systemjobs.FileUploadCleanupJob;
import org.finra.dm.service.systemjobs.JmsPublishingJob;

/**
 * This class tests various functionality within the Job REST controller.
 */
public class SystemJobServiceTest extends AbstractServiceTest
{
    @Test
    public void testRunSystemJobFileUploadCleanup() throws Exception
    {
        // Create the system job run request.
        SystemJobRunRequest systemJobRunRequest = createSystemJobRunRequest(FileUploadCleanupJob.JOB_NAME,
            Arrays.asList(new Parameter(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey(), String.valueOf(INTEGER_VALUE))));

        // Request to run the system job.
        SystemJobRunResponse resultSystemJobRunResponse = systemJobService.runSystemJob(systemJobRunRequest);

        // Validate the returned object.
        validateSystemJobRunResponse(FileUploadCleanupJob.JOB_NAME,
            Arrays.asList(new Parameter(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey(), String.valueOf(INTEGER_VALUE))),
            resultSystemJobRunResponse);
    }

    @Test
    public void testRunSystemJobJmsPublisher() throws Exception
    {
        // Create the system job run request.
        SystemJobRunRequest systemJobRunRequest = createSystemJobRunRequest(JmsPublishingJob.JOB_NAME, null);

        // Request to run the system job.
        SystemJobRunResponse resultSystemJobRunResponse = systemJobService.runSystemJob(systemJobRunRequest);

        // Validate the returned object.
        validateSystemJobRunResponse(JmsPublishingJob.JOB_NAME, null, resultSystemJobRunResponse);
    }

    @Test
    public void testRunSystemJobMissingRequiredParameters() throws Exception
    {
        // Try to run a system job when job name is not specified.
        try
        {
            systemJobService.runSystemJob(createSystemJobRunRequest(BLANK_TEXT, Arrays.asList(new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1))));
            fail("Should throw an IllegalArgumentException when job name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A job name must be specified.", e.getMessage());
        }

        // Try to run a system job when parameter name is not specified.
        try
        {
            systemJobService.runSystemJob(createSystemJobRunRequest(JOB_NAME, Arrays.asList(new Parameter(BLANK_TEXT, ATTRIBUTE_VALUE_1))));
            fail("Should throw an IllegalArgumentException when parameter name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A parameter name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testRunSystemJobMissingFileUploadCleanupOptionalParameters() throws Exception
    {
        // Create the system job run request without parameters.
        SystemJobRunRequest systemJobRunRequest = createSystemJobRunRequest(FileUploadCleanupJob.JOB_NAME, null);

        // Request to run the system job.
        SystemJobRunResponse resultSystemJobRunResponse = systemJobService.runSystemJob(systemJobRunRequest);

        // Validate the returned object.
        validateSystemJobRunResponse(FileUploadCleanupJob.JOB_NAME, null, resultSystemJobRunResponse);
    }

    @Test
    public void testRunSystemJobFileUploadCleanupTrimParameters() throws Exception
    {
        // Create a system job run request using input parameters with leading
        // and trailing empty spaces (except for  parameter values that do not get trimmed).
        SystemJobRunRequest systemJobRunRequest = createSystemJobRunRequest(addWhitespace(FileUploadCleanupJob.JOB_NAME),
            Arrays.asList(new Parameter(addWhitespace(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey()), String.valueOf(INTEGER_VALUE))));

        // Request to run the system job.
        SystemJobRunResponse resultSystemJobRunResponse = systemJobService.runSystemJob(systemJobRunRequest);

        // Validate the returned object.
        validateSystemJobRunResponse(FileUploadCleanupJob.JOB_NAME,
            Arrays.asList(new Parameter(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey(), String.valueOf(INTEGER_VALUE))),
            resultSystemJobRunResponse);
    }

    @Test
    public void testRunSystemJobFileUploadCleanupUpperCaseParameters() throws Exception
    {
        // Create a system job run request using upper case input parameters (except for case-sensitive job name).
        SystemJobRunRequest systemJobRunRequest = createSystemJobRunRequest(FileUploadCleanupJob.JOB_NAME,
            Arrays.asList(new Parameter(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey().toUpperCase(), String.valueOf(INTEGER_VALUE))));

        // Request to run the system job.
        SystemJobRunResponse resultSystemJobRunResponse = systemJobService.runSystemJob(systemJobRunRequest);

        // Validate the returned object.
        validateSystemJobRunResponse(FileUploadCleanupJob.JOB_NAME,
            Arrays.asList(new Parameter(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey().toUpperCase(), String.valueOf(INTEGER_VALUE))),
            resultSystemJobRunResponse);
    }

    @Test
    public void testRunSystemJobFileUploadCleanupLowerCaseParameters() throws Exception
    {
        // Create a system job run request using lower case input parameters (except for case-sensitive job name).
        SystemJobRunRequest systemJobRunRequest = createSystemJobRunRequest(FileUploadCleanupJob.JOB_NAME,
            Arrays.asList(new Parameter(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey().toLowerCase(), String.valueOf(INTEGER_VALUE))));

        // Request to run the system job.
        SystemJobRunResponse resultSystemJobRunResponse = systemJobService.runSystemJob(systemJobRunRequest);

        // Validate the returned object.
        validateSystemJobRunResponse(FileUploadCleanupJob.JOB_NAME,
            Arrays.asList(new Parameter(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey().toLowerCase(), String.valueOf(INTEGER_VALUE))),
            resultSystemJobRunResponse);
    }

    @Test
    public void testRunSystemJobJobNameCaseSensitivity() throws Exception
    {
        // Try to run a system job when specified system job name does not match due to case sensitivity.
        for (String systemJobName : Arrays.asList(FileUploadCleanupJob.JOB_NAME, JmsPublishingJob.JOB_NAME))
        {
            String testSystemJobName = systemJobName.toUpperCase();
            try
            {
                systemJobService.runSystemJob(createSystemJobRunRequest(testSystemJobName, null));
                fail("Should throw an ObjectNotFoundException when specified system job name does not exist.");
            }
            catch (ObjectNotFoundException ex)
            {
                assertEquals(String.format("System job with name \"%s\" doesn't exist.", testSystemJobName), ex.getMessage());
            }
        }
    }

    @Test
    public void testRunSystemJobFileUploadCleanupInvalidParameters() throws Exception
    {
        // Try to run a system job when too many parameters are specified.
        try
        {
            systemJobService.runSystemJob(createSystemJobRunRequest(FileUploadCleanupJob.JOB_NAME,
                Arrays.asList(new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1), new Parameter(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2))));
            fail("Should throw an IllegalArgumentException when too many parameters are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Too many parameters are specified for \"%s\" system job.", FileUploadCleanupJob.JOB_NAME), e.getMessage());
        }

        // Try to run a system job when invalid parameter name is specified.
        try
        {
            systemJobService.runSystemJob(
                createSystemJobRunRequest(FileUploadCleanupJob.JOB_NAME, Arrays.asList(new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1))));
            fail("Should throw an IllegalArgumentException when invalid parameter name is specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Parameter \"%s\" is not supported by \"%s\" system job.", ATTRIBUTE_NAME_1_MIXED_CASE, FileUploadCleanupJob.JOB_NAME),
                e.getMessage());
        }

        // Try to run a system job when invalid parameter value is specified.
        try
        {
            systemJobService.runSystemJob(createSystemJobRunRequest(FileUploadCleanupJob.JOB_NAME,
                Arrays.asList(new Parameter(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey(), "NOT_AN_INTEGER"))));
            fail("Should throw an IllegalArgumentException when invalid parameter value is specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Parameter \"%s\" specifies a non-integer value \"NOT_AN_INTEGER\".",
                ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey()), e.getMessage());
        }
    }

    @Test
    public void testRunSystemJobJmsPublishingInvalidParameters() throws Exception
    {
        // Try to run the system job with the specified parameters.
        try
        {
            systemJobService.runSystemJob(
                createSystemJobRunRequest(JmsPublishingJob.JOB_NAME, Arrays.asList(new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1))));
            fail("Should throw an IllegalArgumentException when parameters are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("\"%s\" system job does not except parameters.", JmsPublishingJob.JOB_NAME), e.getMessage());
        }
    }

    @Test
    public void testRunSystemJobDuplicateParameters() throws Exception
    {
        // Try to run a system job when duplicate parameters are specified.
        try
        {
            systemJobService.runSystemJob(createSystemJobRunRequest(JOB_NAME, Arrays
                .asList(new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1),
                    new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_2))));
            fail("Should throw an IllegalArgumentException when duplicate parameters are specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Duplicate parameter name found: %s", ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase()), e.getMessage());
        }
    }

    @Test
    public void testRunSystemJobSystemJobNoExists() throws Exception
    {
        String testSystemJobName = "I_DO_NOT_EXIST";

        // Try to run a system job when specified system job name does not exist.
        try
        {
            systemJobService.runSystemJob(createSystemJobRunRequest(testSystemJobName, null));
            fail("Should throw an ObjectNotFoundException when specified system job name does not exist.");
        }
        catch (ObjectNotFoundException ex)
        {
            assertEquals(String.format("System job with name \"%s\" doesn't exist.", testSystemJobName), ex.getMessage());
        }
    }
}
