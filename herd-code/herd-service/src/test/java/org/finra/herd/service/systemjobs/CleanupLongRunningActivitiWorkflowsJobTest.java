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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.quartz.JobDataMap;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.AbstractServiceTest;

/**
 * CleanupLongRunningActivitiWorkflowsJobTest
 */
public class CleanupLongRunningActivitiWorkflowsJobTest extends AbstractServiceTest
{
    @Autowired
    private CleanupLongRunningActivitiWorkflowsJob cleanupLongRunningActivitiWorkflowsJob;

    @Test
    public void testValidateParametersHappyPath()
    {
        // Test both parameters.
        {
            Parameter parameter1 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS.getKey(), "10");
            List<Parameter> parameters = new ArrayList<>();
            parameters.add(parameter1);
            cleanupLongRunningActivitiWorkflowsJob.validateParameters(parameters);

            Parameter parameter2 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS.getKey(), "100");
            parameters.add(parameter2);
            cleanupLongRunningActivitiWorkflowsJob.validateParameters(parameters);
        }

        // Reverse the order of parameters.
        {
            Parameter parameter1 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS.getKey(), "100");
            List<Parameter> parameters = new ArrayList<>();
            parameters.add(parameter1);
            cleanupLongRunningActivitiWorkflowsJob.validateParameters(parameters);

            Parameter parameter2 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS.getKey(), "10");
            parameters.add(parameter2);
            cleanupLongRunningActivitiWorkflowsJob.validateParameters(parameters);
        }
    }

    @Test
    public void testValidateParametersTooManyParameters()
    {
        Parameter parameter1 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS.getKey(), "10");
        Parameter parameter2 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS.getKey(), "100");
        Parameter parameter3 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS.getKey(), "100");

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(parameter1);
        parameters.add(parameter2);
        parameters.add(parameter3);

        try
        {
            cleanupLongRunningActivitiWorkflowsJob.validateParameters(parameters);
        }
        catch (Exception exception)
        {
            assertThat("Incorrect exception message.", exception.getMessage(),
                is(equalTo(String.format("Too many parameters are specified for \"%s\" system job.", CleanupLongRunningActivitiWorkflowsJob.JOB_NAME))));
        }
    }

    @Test
    public void testValidateParametersInvalidParameters()
    {
        // Invalid first parameter.
        {
            Parameter parameter1 = new Parameter(ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_UPDATED_ON_THRESHOLD_DAYS.getKey(), "1");

            List<Parameter> parameters = new ArrayList<>();
            parameters.add(parameter1);

            try
            {
                cleanupLongRunningActivitiWorkflowsJob.validateParameters(parameters);
            }
            catch (Exception exception)
            {
                assertThat("Incorrect exception message.", exception.getMessage(),
                    is(equalTo(String.format("Parameter \"%s\" is not supported by \"%s\" system job.", parameter1.getName(),
                        CleanupLongRunningActivitiWorkflowsJob.JOB_NAME))));
            }
        }

        // Invalid second parameter.
        {
            Parameter parameter1 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS.getKey(), "1");
            Parameter parameter2 = new Parameter(ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_UPDATED_ON_THRESHOLD_DAYS.getKey(), "2");

            List<Parameter> parameters = new ArrayList<>();
            parameters.add(parameter1);
            parameters.add(parameter2);

            try
            {
                cleanupLongRunningActivitiWorkflowsJob.validateParameters(parameters);
            }
            catch (Exception exception)
            {
                assertThat("Incorrect exception message.", exception.getMessage(),
                    is(equalTo(String.format("Parameter \"%s\" is not supported by \"%s\" system job.", parameter2.getName(),
                        CleanupLongRunningActivitiWorkflowsJob.JOB_NAME))));
            }
        }
    }

    @Test
    public void testValidateParametersInvalidParameterSize()
    {
        // Invalid threshold days first parameter.
        {
            Parameter parameter1 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS.getKey(), "0");

            List<Parameter> parameters = new ArrayList<>();
            parameters.add(parameter1);

            try
            {
                cleanupLongRunningActivitiWorkflowsJob.validateParameters(parameters);
            }
            catch (Exception exception)
            {
                assertThat("Incorrect exception message.", exception.getMessage(),
                    is(equalTo(String.format("Parameter \"%s\" must be greater than zero in the \"%s\" system job.", parameter1.getName(),
                        CleanupLongRunningActivitiWorkflowsJob.JOB_NAME))));
            }
        }

        // Invalid max workflows first parameter.
        {
            Parameter parameter1 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS.getKey(), "0");

            List<Parameter> parameters = new ArrayList<>();
            parameters.add(parameter1);

            try
            {
                cleanupLongRunningActivitiWorkflowsJob.validateParameters(parameters);
            }
            catch (Exception exception)
            {
                assertThat("Incorrect exception message.", exception.getMessage(),
                    is(equalTo(String.format("Parameter \"%s\" must be greater than zero in the \"%s\" system job.", parameter1.getName(),
                        CleanupLongRunningActivitiWorkflowsJob.JOB_NAME))));
            }
        }

        // Invalid threshold days second parameter.
        {
            Parameter parameter1 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS.getKey(), "1");
            Parameter parameter2 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS.getKey(), "0");

            List<Parameter> parameters = new ArrayList<>();
            parameters.add(parameter1);
            parameters.add(parameter2);

            try
            {
                cleanupLongRunningActivitiWorkflowsJob.validateParameters(parameters);
            }
            catch (Exception exception)
            {
                assertThat("Incorrect exception message.", exception.getMessage(),
                    is(equalTo(String.format("Parameter \"%s\" must be greater than zero in the \"%s\" system job.", parameter2.getName(),
                        CleanupLongRunningActivitiWorkflowsJob.JOB_NAME))));
            }
        }

        // Invalid max workflows second parameter.
        {
            Parameter parameter1 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS.getKey(), "1");
            Parameter parameter2 = new Parameter(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS.getKey(), "0");

            List<Parameter> parameters = new ArrayList<>();
            parameters.add(parameter1);
            parameters.add(parameter2);

            try
            {
                cleanupLongRunningActivitiWorkflowsJob.validateParameters(parameters);
            }
            catch (Exception exception)
            {
                assertThat("Incorrect exception message.", exception.getMessage(),
                    is(equalTo(String.format("Parameter \"%s\" must be greater than zero in the \"%s\" system job.", parameter2.getName(),
                        CleanupLongRunningActivitiWorkflowsJob.JOB_NAME))));
            }
        }
    }

    @Test
    public void testGetJobDataMap()
    {
        JobDataMap jobDataMap = cleanupLongRunningActivitiWorkflowsJob.getJobDataMap();

        Map<?, ?> parameters = (Map<?, ?>) jobDataMap.get(jobDataMap.keySet().iterator().next());

        String maxWorkFlows = (String) parameters.get(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS.getKey());
        assertThat("Incorrect number of max workflows.", maxWorkFlows,
            is(equalTo(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_MAX_WORKFLOWS.getDefaultValue())));

        String thresholdDays = (String) parameters.get(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS.getKey());
        assertThat("Incorrect number of job threshold days.", thresholdDays,
            is(equalTo(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_THRESHOLD_DAYS.getDefaultValue())));
    }

    @Test
    public void testGetCronExpression()
    {
        String cronExpression = cleanupLongRunningActivitiWorkflowsJob.getCronExpression();
        assertThat("Incorrect cron expression.", cronExpression,
            is(equalTo(ConfigurationValue.CLEANUP_LONG_RUNNING_ACTIVITI_WORKFLOWS_JOB_CRON_EXPRESSION.getDefaultValue())));
    }
}
