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
package org.finra.dm.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.api.xml.JobDefinition;
import org.finra.dm.model.api.xml.JobDefinitionCreateRequest;
import org.finra.dm.model.api.xml.JobDefinitionUpdateRequest;
import org.finra.dm.model.api.xml.Parameter;

/**
 * This class tests various functionality within the job definition REST controller.
 */
public class JobDefinitionRestControllerTest extends AbstractRestTest
{
    private static final String INVALID_NAME = "DM_Invalid_Name_" + UUID.randomUUID().toString().substring(0, 3);

    @Test
    public void testCreateJobDefinition() throws Exception
    {
        // Test the basic happy path of creating a job definition.
        createJobDefinition();
    }

    @Test
    public void testGetJobDefinition() throws Exception
    {
        // Create a new job definition.
        JobDefinition jobDefinition = createJobDefinition();

        // Retrieve the job definition.
        jobDefinition = jobDefinitionRestController.getJobDefinition(jobDefinition.getNamespace(), jobDefinition.getJobName());

        // Validate that the retrieved job definition matches what we created.
        validateJobDefinition(jobDefinition);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testGetJobDefinitionNoExist() throws Exception
    {
        // Retrieve a job definition that doesn't exist.
        jobDefinitionRestController.getJobDefinition(INVALID_NAME, INVALID_NAME);
    }

    @Test
    public void testUpdateJobDefinition() throws Exception
    {
        // Create job definition create request using hard coded test values.
        JobDefinitionCreateRequest createRequest = createJobDefinitionCreateRequest();

        // Set 2 distinct parameters.
        List<Parameter> parameters = new ArrayList<>();
        createRequest.setParameters(parameters);

        Parameter parameter = new Parameter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);
        parameters.add(parameter);
        parameter = new Parameter(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2);
        parameters.add(parameter);

        // Create the namespace entity.
        createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create the job definition in the database.
        jobDefinitionRestController.createJobDefinition(createRequest);

        // Create an update request with a varied set of data that is based on the same data used in the create request.
        JobDefinitionUpdateRequest updateRequest = createUpdateRequest(createRequest);

        // Update the job definition in the database.
        JobDefinition jobDefinition = jobDefinitionRestController.updateJobDefinition(createRequest.getNamespace(), createRequest.getJobName(), updateRequest);

        // Validate the updated job definition.
        assertNotNull(jobDefinition);
        assertEquals(createRequest.getNamespace(), jobDefinition.getNamespace());
        assertEquals(createRequest.getJobName(), jobDefinition.getJobName());
        assertEquals(updateRequest.getDescription(), jobDefinition.getDescription());
        assertEquals(updateRequest.getParameters(), jobDefinition.getParameters());
        assertTrue(jobDefinition.getActivitiJobXml().contains("Unit Test 2"));
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testUpdateJobDefinitionNamespaceNoExist() throws Exception
    {
        // Create an update request.
        JobDefinitionUpdateRequest updateRequest = createUpdateRequest(createJobDefinitionCreateRequest());

        // Update the process Id to match an invalid namespace and invalid job name to pass validation.
        updateRequest.setActivitiJobXml(
            updateRequest.getActivitiJobXml().replace(TEST_ACTIVITI_NAMESPACE_CD + "." + TEST_ACTIVITI_JOB_NAME, INVALID_NAME + "." + INVALID_NAME));

        // Try to update a job definition that has a namespace that doesn't exist.
        jobDefinitionRestController.updateJobDefinition(INVALID_NAME, INVALID_NAME, updateRequest);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testUpdateJobDefinitionJobNameNoExist() throws Exception
    {
        // Create an update request.
        JobDefinitionUpdateRequest updateRequest = createUpdateRequest(createJobDefinitionCreateRequest());

        // Update the process Id to match a valid namespace and invalid job name to pass validation.
        updateRequest.setActivitiJobXml(updateRequest.getActivitiJobXml()
            .replace(TEST_ACTIVITI_NAMESPACE_CD + "." + TEST_ACTIVITI_JOB_NAME, TEST_ACTIVITI_NAMESPACE_CD + "." + INVALID_NAME));

        // Create the namespace entity.
        createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Try to update a job definition that has a namespace that exists, but a job name that doesn't exist.
        jobDefinitionRestController.updateJobDefinition(TEST_ACTIVITI_NAMESPACE_CD, INVALID_NAME, updateRequest);
    }

    /**
     * Create an update request with a varied set of data that is based on the same data used in the create request.
     *
     * @param createRequest the create request.
     *
     * @return the update request.
     */
    private JobDefinitionUpdateRequest createUpdateRequest(JobDefinitionCreateRequest createRequest)
    {
        // Create an update request that modifies all data from the create request.
        JobDefinitionUpdateRequest updateRequest = new JobDefinitionUpdateRequest();
        updateRequest.setDescription(createRequest.getDescription() + "2");
        updateRequest.setActivitiJobXml(createRequest.getActivitiJobXml().replace("Unit Test", "Unit Test 2"));

        List<Parameter> parameters = new ArrayList<>();
        updateRequest.setParameters(parameters);

        // Delete the first parameter, update the second parameter, and add a new third parameter.
        Parameter parameter = new Parameter(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2 + "2");
        parameters.add(parameter);
        parameter = new Parameter(ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3);
        parameters.add(parameter);
        return updateRequest;
    }

    /**
     * Creates a new standard job definition.
     *
     * @return the created job definition.
     * @throws Exception if any problems were encountered.
     */
    private JobDefinition createJobDefinition() throws Exception
    {
        // Create the namespace entity.
        createNamespaceEntity(TEST_ACTIVITI_NAMESPACE_CD);

        // Create job definition create request using hard coded test values.
        JobDefinitionCreateRequest request = createJobDefinitionCreateRequest();

        // Create the job definition in the database.
        JobDefinition jobDefinition = jobDefinitionRestController.createJobDefinition(request);

        // Validate the created job definition.
        validateJobDefinition(jobDefinition);

        return jobDefinition;
    }

    /**
     * Validates a standard job definition.
     *
     * @param jobDefinition the job definition to validate.
     */
    private void validateJobDefinition(JobDefinition jobDefinition)
    {
        // Validate the basic job definition fields.
        assertNotNull(jobDefinition);
        assertEquals(TEST_ACTIVITI_NAMESPACE_CD, jobDefinition.getNamespace());
        assertEquals(TEST_ACTIVITI_JOB_NAME, jobDefinition.getJobName());
        assertEquals(JOB_DESCRIPTION, jobDefinition.getDescription());
        assertTrue(jobDefinition.getParameters().size() == 1);

        Parameter parameter = jobDefinition.getParameters().get(0);
        assertEquals(ATTRIBUTE_NAME_1_MIXED_CASE, parameter.getName());
        assertEquals(ATTRIBUTE_VALUE_1, parameter.getValue());
    }
}
