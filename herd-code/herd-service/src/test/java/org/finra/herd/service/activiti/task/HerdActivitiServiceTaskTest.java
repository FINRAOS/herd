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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.FieldExtension;
import org.activiti.bpmn.model.ServiceTask;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.runtime.Execution;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.finra.herd.model.api.xml.Job;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Base class to tests the Activiti tasks for services.
 */
public abstract class HerdActivitiServiceTaskTest extends AbstractServiceTest
{
    private static Logger LOGGER = LoggerFactory.getLogger(HerdActivitiServiceTaskTest.class);

    private final String serviceTaskId = "testServiceTask";

    public final String VARIABLE_VALUE_NOT_NULL = "NOT_NULL";
    public final String VARIABLE_VALUE_IS_NULL = "IS_NULL";

    protected FieldExtension buildFieldExtension(String name, String expression)
    {
        FieldExtension exceptionField = new FieldExtension();
        exceptionField.setFieldName(name);
        exceptionField.setExpression(expression);
        return exceptionField;
    }

    protected Parameter buildParameter(String name, String value)
    {
        return new Parameter(name, value);
    }

    protected String buildActivitiXml(String implementation, List<FieldExtension> fieldExtensionList) throws Exception
    {
        BpmnModel bpmnModel = getBpmnModelForXmlResource(ACTIVITI_XML_TEST_SERVICE_TASK_WITH_CLASSPATH);

        ServiceTask serviceTask = (ServiceTask) bpmnModel.getProcesses().get(0).getFlowElement(serviceTaskId);

        serviceTask.setImplementation(implementation);
        serviceTask.getFieldExtensions().addAll(fieldExtensionList);

        return getActivitiXmlFromBpmnModel(bpmnModel);
    }

    protected Job testActivitiServiceTaskSuccess(String implementation, List<FieldExtension> fieldExtensionList, List<Parameter> parameters,
        Map<String, Object> variableValuesToValidate) throws Exception
    {
        String activitiXml = buildActivitiXml(implementation, fieldExtensionList);
        return createJobAndCheckTaskStatusSuccess(activitiXml, parameters, variableValuesToValidate);
    }

    private Job createJobAndCheckTaskStatusSuccess(String activitiXml, List<Parameter> parameters, Map<String, Object> variableValuesToValidate)
        throws Exception
    {
        Job job = createJobFromActivitiXml(activitiXml, parameters);
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_SUCCESS, serviceTaskStatus);

        if (variableValuesToValidate != null)
        {
            for (Map.Entry<String, Object> varEntry : variableValuesToValidate.entrySet())
            {
                Object wfVariableValue = variables.get(getServiceTaskVariableName(varEntry.getKey()));
                Object expectedVariableValue = varEntry.getValue();
                if (expectedVariableValue.equals(VARIABLE_VALUE_NOT_NULL))
                {
                    assertNotNull(wfVariableValue);
                }
                else if (expectedVariableValue.equals(VARIABLE_VALUE_IS_NULL))
                {
                    assertNull(wfVariableValue);
                }
                else
                {
                    assertEquals(expectedVariableValue, wfVariableValue);
                }
            }
        }

        return job;
    }

    protected Job testActivitiServiceTaskFailure(String implementation, List<FieldExtension> fieldExtensionList, List<Parameter> parameters,
        Map<String, Object> variableValuesToValidate) throws Exception
    {
        String activitiXml = buildActivitiXml(implementation, fieldExtensionList);
        return createJobAndCheckTaskStatusFailure(activitiXml, parameters, variableValuesToValidate);
    }

    private Job createJobAndCheckTaskStatusFailure(String activitiXml, List<Parameter> parameters, Map<String, Object> variableValuesToValidate)
        throws Exception
    {
        Job job = createJobFromActivitiXml(activitiXml, parameters);
        assertNotNull(job);

        HistoricProcessInstance hisInstance =
            activitiHistoryService.createHistoricProcessInstanceQuery().processInstanceId(job.getId()).includeProcessVariables().singleResult();
        Map<String, Object> variables = hisInstance.getProcessVariables();

        String serviceTaskStatus = (String) variables.get(getServiceTaskVariableName(ActivitiRuntimeHelper.VARIABLE_STATUS));
        assertEquals(ActivitiRuntimeHelper.TASK_STATUS_ERROR, serviceTaskStatus);

        if (variableValuesToValidate != null)
        {
            for (Map.Entry<String, Object> varEntry : variableValuesToValidate.entrySet())
            {
                Object wfVariableValue = variables.get(getServiceTaskVariableName(varEntry.getKey()));
                Object expectedVariableValue = varEntry.getValue();
                if (expectedVariableValue.equals(VARIABLE_VALUE_NOT_NULL))
                {
                    assertNotNull(wfVariableValue);
                }
                else if (expectedVariableValue.equals(VARIABLE_VALUE_IS_NULL))
                {
                    assertNull(wfVariableValue);
                }
                else
                {
                    assertEquals(expectedVariableValue, wfVariableValue);
                }
            }
        }

        return job;
    }

    protected String getServiceTaskVariableName(String variableName)
    {
        return serviceTaskId + ActivitiRuntimeHelper.TASK_VARIABLE_MARKER + variableName;
    }

    /**
     * Blocks the current calling thread until ALL processes are considered not active. This method will timeout with an assertion error if the waiting takes
     * longer than 10,000ms. This is a reasonable amount of time for the JUnits that use this method.
     */
    protected void waitUntilAllProcessCompleted()
    {
        long startTime = System.currentTimeMillis();

        // Run while there is at least 1 active instance
        while (activitiRuntimeService.createProcessInstanceQuery().active().count() > 0)
        {
            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - startTime;

            // If time spent waiting is longer than 15,000 ms
            if (elapsedTime > 15000)
            {
                // Dump the current runtime variables into the error log to make it easier to debug
                StringBuilder builder = new StringBuilder("dumping workflow variables due to error:\n");
                List<Execution> executions = activitiRuntimeService.createExecutionQuery().list();
                for (Execution execution : executions)
                {
                    Map<String, Object> executionVariables = activitiRuntimeService.getVariables(execution.getId());
                    builder.append(execution).append('\n');
                    for (Map.Entry<String, Object> variable : executionVariables.entrySet())
                    {
                        builder.append(variable).append('\n');
                    }
                }
                LOGGER.error(builder.toString());

                // Fail assertion
                Assert.fail("The test did not finished in the specified timeout (15s). See error logs for variable dump.");
            }
        }
    }
}
