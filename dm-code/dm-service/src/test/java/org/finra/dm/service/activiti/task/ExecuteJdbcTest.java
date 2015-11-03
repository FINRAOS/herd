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
package org.finra.dm.service.activiti.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.activiti.bpmn.model.FieldExtension;
import org.activiti.engine.HistoryService;
import org.activiti.engine.RepositoryService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.history.HistoricVariableInstance;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.ProcessInstance;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.dm.dao.impl.MockJdbcOperations;
import org.finra.dm.model.api.xml.JdbcExecutionRequest;
import org.finra.dm.model.api.xml.JdbcExecutionResponse;
import org.finra.dm.model.api.xml.JdbcStatement;
import org.finra.dm.model.api.xml.JdbcStatementStatus;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.activiti.ActivitiRuntimeHelper;

public class ExecuteJdbcTest extends DmActivitiServiceTaskTest
{
    private static final String JAVA_DELEGATE_CLASS_NAME = ExecuteJdbc.class.getCanonicalName();

    private static final Logger LOGGER = Logger.getLogger(ExecuteJdbcTest.class);

    @Autowired
    private RepositoryService repositoryService;

    @Autowired
    private RuntimeService runtimeService;

    @Autowired
    private HistoryService historyService;

    @Test
    public void testExecuteJdbcSuccess()
    {
        JdbcExecutionRequest jdbcExecutionRequest = createDefaultUpdateJdbcExecutionRequest();

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        List<Parameter> parameters = new ArrayList<>();

        populateParameters(jdbcExecutionRequest, fieldExtensionList, parameters);

        try
        {
            JdbcExecutionResponse expectedJdbcExecutionResponse = new JdbcExecutionResponse();
            expectedJdbcExecutionResponse.setStatements(jdbcExecutionRequest.getStatements());
            expectedJdbcExecutionResponse.getStatements().get(0).setStatus(JdbcStatementStatus.SUCCESS);
            expectedJdbcExecutionResponse.getStatements().get(0).setResult("1");

            String expectedJdbcExecutionResponseJson = jsonHelper.objectToJson(expectedJdbcExecutionResponse);

            Map<String, Object> variableValuesToValidate = new HashMap<>();
            variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, expectedJdbcExecutionResponseJson);

            testActivitiServiceTaskSuccess(JAVA_DELEGATE_CLASS_NAME, fieldExtensionList, parameters, variableValuesToValidate);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testExecuteJdbcErrorValidation()
    {
        JdbcExecutionRequest jdbcExecutionRequest = createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.setConnection(null);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        List<Parameter> parameters = new ArrayList<>();

        populateParameters(jdbcExecutionRequest, fieldExtensionList, parameters);

        try
        {
            Map<String, Object> variableValuesToValidate = new HashMap<>();
            variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, VARIABLE_VALUE_IS_NULL);
            variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "JDBC connection is required");

            testActivitiServiceTaskFailure(JAVA_DELEGATE_CLASS_NAME, fieldExtensionList, parameters, variableValuesToValidate);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testExecuteJdbcErrorStatement()
    {
        JdbcExecutionRequest jdbcExecutionRequest = createDefaultUpdateJdbcExecutionRequest();
        jdbcExecutionRequest.getStatements().get(0).setSql(MockJdbcOperations.CASE_2_SQL);

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        List<Parameter> parameters = new ArrayList<>();

        populateParameters(jdbcExecutionRequest, fieldExtensionList, parameters);

        try
        {
            JdbcExecutionResponse expectedJdbcExecutionResponse = new JdbcExecutionResponse();
            expectedJdbcExecutionResponse.setStatements(jdbcExecutionRequest.getStatements());
            expectedJdbcExecutionResponse.getStatements().get(0).setStatus(JdbcStatementStatus.ERROR);
            expectedJdbcExecutionResponse.getStatements().get(0).setErrorMessage("java.sql.SQLException: test DataIntegrityViolationException cause");

            String expectedJdbcExecutionResponseString = jsonHelper.objectToJson(expectedJdbcExecutionResponse);

            Map<String, Object> variableValuesToValidate = new HashMap<>();
            variableValuesToValidate.put(BaseJavaDelegate.VARIABLE_JSON_RESPONSE, expectedJdbcExecutionResponseString);
            variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "There are failed executions. See JSON response for details.");

            testActivitiServiceTaskFailure(JAVA_DELEGATE_CLASS_NAME, fieldExtensionList, parameters, variableValuesToValidate);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail();
        }
    }

    /**
     * Asserts that the task executes asynchronously when receiveTaskId is specified.
     * 
     * This is a very special test case which involves multithreading and transactions, therefore we cannot use the standard test methods we have.
     * The transaction MUST BE DISABLED for this test to work correctly - since we have 2 threads which both access the database, if we run transactionally, the
     * threads cannot share information.
     * 
     * TODO this test could be made generic once we have async support for other tasks.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void testExecuteJdbcWithReceiveTask() throws Exception
    {
        // Read workflow XML from classpath and deploy it.
        repositoryService.createDeployment().addClasspathResource("org/finra/dm/service/testActivitiWorkflowExecuteJdbcTaskWithReceiveTask.bpmn20.xml")
            .deploy();

        JdbcExecutionRequest jdbcExecutionRequest = createDefaultUpdateJdbcExecutionRequest();

        // Set workflow variables.
        Map<String, Object> variables = new HashMap<>();
        variables.put("contentType", "xml");
        variables.put("jdbcExecutionRequest", xmlHelper.objectToXml(jdbcExecutionRequest));

        // Execute workflow
        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("test", variables);

        // Wait for the process to finish
        waitUntilAllProcessCompleted();

        // Assert output
        Map<String, Object> outputVariables = getProcessInstanceHistoryVariables(processInstance);

        JdbcExecutionResponse expectedJdbcExecutionResponse = new JdbcExecutionResponse();
        JdbcStatement originalJdbcStatement = jdbcExecutionRequest.getStatements().get(0);
        JdbcStatement expectedJdbcStatement = new JdbcStatement();
        expectedJdbcStatement.setType(originalJdbcStatement.getType());
        expectedJdbcStatement.setSql(originalJdbcStatement.getSql());
        expectedJdbcStatement.setStatus(JdbcStatementStatus.SUCCESS);
        expectedJdbcStatement.setResult("1");
        expectedJdbcExecutionResponse.setStatements(Arrays.asList(expectedJdbcStatement));

        String actualJdbcExecutionResponseString = (String) outputVariables.get("service_jsonResponse");

        JdbcExecutionResponse actualJdbcExecutionResponse = jsonHelper.unmarshallJsonToObject(JdbcExecutionResponse.class, actualJdbcExecutionResponseString);

        Assert.assertEquals("service_jsonResponse", expectedJdbcExecutionResponse, actualJdbcExecutionResponse);
        Assert.assertEquals("service_taskStatus", "SUCCESS", outputVariables.get("service_taskStatus"));
    }

    /**
     * Retrieves the historic instance variables of the given process instance.
     * 
     * @param processInstance The process instance which owns the history
     * @return A map of name-value
     */
    private Map<String, Object> getProcessInstanceHistoryVariables(ProcessInstance processInstance)
    {
        Map<String, Object> outputVariables = new HashMap<>();
        List<HistoricVariableInstance> historicVariableInstances = historyService.createHistoricVariableInstanceQuery().processInstanceId(processInstance
            .getId()).list();
        for (HistoricVariableInstance historicVariableInstance : historicVariableInstances)
        {
            String name = historicVariableInstance.getVariableName();
            Object value = historicVariableInstance.getValue();

            outputVariables.put(name, value);
        }
        return outputVariables;
    }

    /**
     * Blocks the current calling thread until ALL processes are considered not active.
     * This method will timeout with an assertion error if the waiting takes longer than 5000ms.
     */
    private void waitUntilAllProcessCompleted()
    {
        long startTime = System.currentTimeMillis();

        // Run while there is at least 1 active instance
        while (runtimeService.createProcessInstanceQuery().active().count() > 0)
        {
            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - startTime;

            // If time spent waiting is longer than 5000 ms
            if (elapsedTime > 5000)
            {
                // Dump the current runtime variables into the error log to make it easier to debug
                StringBuilder builder = new StringBuilder("dumping workflow variables due to error:\n");
                List<Execution> executions = runtimeService.createExecutionQuery().list();
                for (Execution execution : executions)
                {
                    Map<String, Object> executionVariables = runtimeService.getVariables(execution.getId());
                    builder.append(execution).append('\n');
                    for (Map.Entry<String, Object> variable : executionVariables.entrySet())
                    {
                        builder.append(variable).append('\n');
                    }
                }
                LOGGER.error(builder);

                // Fail assertion
                Assert.fail("The test did not finished in the specified timeout (5s). See error logs for variable dump.");
            }
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
