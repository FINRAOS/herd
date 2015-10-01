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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.activiti.bpmn.model.FieldExtension;
import org.junit.Assert;
import org.junit.Test;

import org.finra.dm.dao.impl.MockJdbcOperations;
import org.finra.dm.model.api.xml.JdbcExecutionRequest;
import org.finra.dm.model.api.xml.JdbcExecutionResponse;
import org.finra.dm.model.api.xml.JdbcStatementStatus;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.activiti.ActivitiHelper;

public class ExecuteJdbcTest extends DmActivitiServiceTaskTest
{
    private static final String JAVA_DELEGATE_CLASS_NAME = ExecuteJdbc.class.getCanonicalName();

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
            variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, "JDBC connection is required");

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
            variableValuesToValidate.put(ActivitiHelper.VARIABLE_ERROR_MESSAGE, "There are failed executions. See JSON response for details.");

            testActivitiServiceTaskFailure(JAVA_DELEGATE_CLASS_NAME, fieldExtensionList, parameters, variableValuesToValidate);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail();
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
