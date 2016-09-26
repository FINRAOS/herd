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
package org.finra.herd.service.activiti.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.activiti.bpmn.model.FieldExtension;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.impl.MockJdbcOperations;
import org.finra.herd.model.api.xml.JdbcExecutionRequest;
import org.finra.herd.model.api.xml.JdbcExecutionResponse;
import org.finra.herd.model.api.xml.JdbcStatement;
import org.finra.herd.model.api.xml.JdbcStatementStatus;
import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.JobStatusEnum;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.JdbcServiceTestHelper;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

public class ExecuteJdbcTest extends HerdActivitiServiceTaskTest
{
    private static final String JAVA_DELEGATE_CLASS_NAME = ExecuteJdbc.class.getCanonicalName();

    @Autowired
    private ExecuteJdbcTestHelper executeJdbcTestHelper;

    @Autowired
    private JdbcServiceTestHelper jdbcServiceTestHelper;

    @Test
    public void testExecuteJdbcSuccess() throws Exception
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        List<Parameter> parameters = new ArrayList<>();

        populateParameters(jdbcExecutionRequest, fieldExtensionList, parameters);

        JdbcExecutionResponse expectedJdbcExecutionResponse = new JdbcExecutionResponse();
        expectedJdbcExecutionResponse.setStatements(jdbcExecutionRequest.getStatements());
        expectedJdbcExecutionResponse.getStatements().get(0).setStatus(JdbcStatementStatus.SUCCESS);
        expectedJdbcExecutionResponse.getStatements().get(0).setResult("1");

        String expectedJdbcExecutionResponseJson = jsonHelper.objectToJson(expectedJdbcExecutionResponse);

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, expectedJdbcExecutionResponseJson);

        testActivitiServiceTaskSuccess(JAVA_DELEGATE_CLASS_NAME, fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testExecuteJdbcErrorValidation() throws Exception
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.setConnection(null);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        List<Parameter> parameters = new ArrayList<>();

        populateParameters(jdbcExecutionRequest, fieldExtensionList, parameters);

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, VARIABLE_VALUE_IS_NULL);
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "JDBC connection is required");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(JAVA_DELEGATE_CLASS_NAME, fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    @Test
    public void testExecuteJdbcErrorStatement() throws Exception
    {
        JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getStatements().get(0).setSql(MockJdbcOperations.CASE_2_SQL);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        List<Parameter> parameters = new ArrayList<>();

        populateParameters(jdbcExecutionRequest, fieldExtensionList, parameters);

        JdbcExecutionResponse expectedJdbcExecutionResponse = new JdbcExecutionResponse();
        expectedJdbcExecutionResponse.setStatements(jdbcExecutionRequest.getStatements());
        expectedJdbcExecutionResponse.getStatements().get(0).setStatus(JdbcStatementStatus.ERROR);
        expectedJdbcExecutionResponse.getStatements().get(0).setErrorMessage("java.sql.SQLException: test DataIntegrityViolationException cause");

        String expectedJdbcExecutionResponseString = jsonHelper.objectToJson(expectedJdbcExecutionResponse);

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, expectedJdbcExecutionResponseString);
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "There are failed executions. See JSON response for details.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(JAVA_DELEGATE_CLASS_NAME, fieldExtensionList, parameters, variableValuesToValidate);
        });
    }

    /**
     * Asserts that the task executes asynchronously when receiveTaskId is specified.
     * <p/>
     * This is a very special test case which involves multithreading and transactions, therefore we cannot use the standard test methods we have. The
     * transaction MUST BE DISABLED for this test to work correctly - since we have 2 threads which both access the database, if we run transactionally, the
     * threads cannot share information.
     * <p/>
     * TODO this test could be made generic once we have async support for other tasks.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void testExecuteJdbcWithReceiveTask() throws Exception
    {
        // Create and persist a test job definition.
        executeJdbcTestHelper.prepareHerdDatabaseForExecuteJdbcWithReceiveTaskTest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME,
            "classpath:org/finra/herd/service/testActivitiWorkflowExecuteJdbcTaskWithReceiveTask.bpmn20.xml");

        try
        {
            // Create a JDBC execution request.
            JdbcExecutionRequest jdbcExecutionRequest = jdbcServiceTestHelper.createDefaultUpdateJdbcExecutionRequest();

            // Create and initialize a list of parameters.
            List<Parameter> parameters = new ArrayList<>();
            parameters.add(new Parameter("contentType", "xml"));
            parameters.add(new Parameter("jdbcExecutionRequest", xmlHelper.objectToXml(jdbcExecutionRequest)));

            // Get a job create request.
            JobCreateRequest jobCreateRequest = jobServiceTestHelper.createJobCreateRequest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);
            jobCreateRequest.setParameters(parameters);

            // Start the job.
            Job jobStartResponse = jobService.createAndStartJob(jobCreateRequest);

            // Wait for the process to finish
            waitUntilAllProcessCompleted();

            // Validate that the job is completed.
            Job jobGetResponse = jobService.getJob(jobStartResponse.getId(), true);
            assertEquals(JobStatusEnum.COMPLETED, jobGetResponse.getStatus());

            // Validate the task status.
            assertTrue(jobGetResponse.getParameters().contains(new Parameter("service_taskStatus", "SUCCESS")));

            // Validate the JDBC execution response.
            JdbcExecutionResponse expectedJdbcExecutionResponse = new JdbcExecutionResponse();
            JdbcStatement originalJdbcStatement = jdbcExecutionRequest.getStatements().get(0);
            JdbcStatement expectedJdbcStatement = new JdbcStatement();
            expectedJdbcStatement.setType(originalJdbcStatement.getType());
            expectedJdbcStatement.setSql(originalJdbcStatement.getSql());
            expectedJdbcStatement.setStatus(JdbcStatementStatus.SUCCESS);
            expectedJdbcStatement.setResult("1");
            expectedJdbcExecutionResponse.setStatements(Arrays.asList(expectedJdbcStatement));
            Parameter expectedJdbcExecutionResponseParameter = new Parameter("service_jsonResponse", jsonHelper.objectToJson(expectedJdbcExecutionResponse));
            assertTrue(jobGetResponse.getParameters().contains(expectedJdbcExecutionResponseParameter));
        }
        finally
        {
            // Clean up the Herd database.
            executeJdbcTestHelper.cleanUpHerdDatabaseAfterExecuteJdbcWithReceiveTaskTest(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);

            // Clean up the Activiti.
            deleteActivitiDeployments();
        }
    }

    private void populateParameters(JdbcExecutionRequest jdbcExecutionRequest, List<FieldExtension> fieldExtensionList, List<Parameter> parameters)
    {
        try
        {
            String jdbcExecutionRequestString = xmlHelper.objectToXml(jdbcExecutionRequest);

            fieldExtensionList.add(buildFieldExtension("contentType", "${contentType}"));
            fieldExtensionList.add(buildFieldExtension("jdbcExecutionRequest", "${jdbcExecutionRequest}"));

            parameters.add(buildParameter("contentType", "xml"));
            parameters.add(buildParameter("jdbcExecutionRequest", jdbcExecutionRequestString));
        }
        catch (JAXBException e)
        {
            throw new IllegalArgumentException(e);
        }
    }
}
