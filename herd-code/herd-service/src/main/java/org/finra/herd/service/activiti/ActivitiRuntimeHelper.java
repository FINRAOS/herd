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
package org.finra.herd.service.activiti;

import org.activiti.engine.RuntimeService;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.ExecutionQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * A helper for activiti's {@link RuntimeService}.
 * <p/>
 * This class is distinguished from {@link ActivitiHelper} in that it has direct access to a RuntimeService. This class was separated from ActivitiHelper
 * implementation due to a circular dependency introduced if RuntimeService is autowired into ActivitiHelper. The circular dependency is as thus:
 * <p/>
 * RuntimeService -> ProcessEngine -> ProcessEngineConfiguration -> HerdCommandInvoker -> ActivitiHelper -> RuntimeService
 * <p/>
 * So instead, this class breaks the circularity:
 * <p/>
 * RuntimeService -> ProcessEngine -> ProcessEngineConfiguration -> HerdCommandInvoker -> ActivitiHelper ActivitiRuntimeHelper -> RuntimeService
 * <p/>
 * Due to historical reasons, some methods that were previously in ActivitiHelper have been moved into this class.
 */
@Component
public class ActivitiRuntimeHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ActivitiRuntimeHelper.class);

    public static final String VARIABLE_STATUS = "taskStatus";
    public static final String VARIABLE_ERROR_MESSAGE = "taskErrorMessage";
    public static final String TASK_STATUS_SUCCESS = "SUCCESS";
    public static final String TASK_STATUS_ERROR = "ERROR";
    public static final String TASK_VARIABLE_MARKER = "_";

    @Autowired
    private RuntimeService runtimeService;

    /**
     * Signals a task by its process instance ID and the ID of the task to signal.
     *
     * @param processInstanceId The process instance ID of the waiting task.
     * @param signalTaskId The ID of the task to signal.
     */
    public void signal(String processInstanceId, String signalTaskId)
    {
        ExecutionQuery signalTaskQuery = runtimeService.createExecutionQuery().processInstanceId(processInstanceId).activityId(signalTaskId);
        Execution execution = signalTaskQuery.singleResult();
        runtimeService.signal(execution.getId());
    }

    /**
     * Sets the workflow variable whose name references the task which produced the variable. This method will automatically generate and concatenate a name to
     * the variable name which can help reference the task which produces the variable.
     *
     * @param executionId The execution ID
     * @param activitiId The task's ID
     * @param variableName Name of the variable without the task reference
     * @param variableValue The value of the variable.
     */
    public void setTaskWorkflowVariable(String executionId, String activitiId, String variableName, Object variableValue)
    {
        // Get the variables associated with this execution. This is a workaround to what appears to be an Activiti bug in version 5.19.0.1 (which used
        // to work in 5.16.3). In the newer version, setting a variable like we are doing below doesn't actually set the variable properly in
        // VariableScopeImpl.createVariableInstance because an internal variableInstances hasn't yet been initialized. By calling the getVariables
        // method, that forces that map to get initialized and the subsequent setVariable method below will work.
        runtimeService.getVariables(executionId);

        runtimeService.setVariable(executionId, buildTaskWorkflowVariableName(activitiId, variableName), variableValue);
    }

    /**
     * Sets a workflow variable name based on the template.
     *
     * @param execution the workflow execution
     * @param variableName the variable name
     * @param variableValue the variable value
     */
    public void setTaskWorkflowVariable(DelegateExecution execution, String variableName, Object variableValue)
    {
        setTaskWorkflowVariable(execution.getId(), execution.getCurrentActivityId(), variableName, variableValue);
    }

    /**
     * Sets the task success status in a workflow variable.
     *
     * @param execution the workflow execution.
     */
    public void setTaskSuccessInWorkflow(DelegateExecution execution)
    {
        setTaskWorkflowVariable(execution, VARIABLE_STATUS, TASK_STATUS_SUCCESS);
    }

    /**
     * Sets the task error status and message in workflow variables.
     *
     * @param execution the workflow execution.
     * @param errorMessage the error message.
     * @param exception an optional exception that caused the error.
     */
    public void setTaskErrorInWorkflow(DelegateExecution execution, String errorMessage, Exception exception)
    {
        setTaskWorkflowVariable(execution, VARIABLE_STATUS, TASK_STATUS_ERROR);
        setTaskWorkflowVariable(execution, VARIABLE_ERROR_MESSAGE, errorMessage);
        if (exception != null)
        {
            LOGGER.warn("Workflow encountered an error. Logging stack trace in case it is needed to debug an issue. " +
                "activitiCurrentActivityId=\"{}\" activitiExecutionId=\"{}\"", execution.getCurrentActivityId(), execution.getId(), exception);
            // TODO: Remove this logging statement above and set a workflow variable with the stack trace instead.
            // setTaskWorkflowVariable(execution, "taskErrorStackTrace", ExceptionUtils.getStackTrace(exception));
        }
    }

    /**
     * Builds a workflow variable name based on the template.
     *
     * @param activitiId activitiId
     * @param variableName the variable name
     *
     * @return the workflow variable name
     */
    public String buildTaskWorkflowVariableName(String activitiId, String variableName)
    {
        return activitiId + TASK_VARIABLE_MARKER + variableName;
    }
}
