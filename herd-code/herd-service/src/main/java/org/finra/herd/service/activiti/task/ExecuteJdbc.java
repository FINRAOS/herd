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

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.HerdThreadHelper;
import org.finra.herd.model.api.xml.JdbcExecutionRequest;
import org.finra.herd.model.api.xml.JdbcExecutionResponse;
import org.finra.herd.model.api.xml.JdbcStatement;
import org.finra.herd.model.api.xml.JdbcStatementStatus;
import org.finra.herd.service.JdbcService;

/**
 * The Activiti delegate task for {@link JdbcService#executeJdbc(JdbcExecutionRequest)}.
 * <p/>
 * <pre>
 * <extensionElements>
 *    <activiti:fields name="contentType" stringValue="" />
 *    <activiti:fields name="jdbcExecutionRequest" stringValue="" />
 * </extensionElements>
 * </pre>
 */
@Component
public class ExecuteJdbc extends BaseJavaDelegate
{
    private Expression contentType;
    private Expression jdbcExecutionRequest;
    private Expression receiveTaskId;

    @Autowired
    private JdbcService jdbcService;

    @Autowired
    private HerdThreadHelper herdThreadHelper;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        // Construct request from parameters
        String contentTypeString = activitiHelper.getRequiredExpressionVariableAsString(contentType, execution, "ContentType");
        String requestString = activitiHelper.getRequiredExpressionVariableAsString(jdbcExecutionRequest, execution, "JdbcExecutionRequest").trim();
        String receiveTaskIdString = activitiHelper.getExpressionVariableAsString(receiveTaskId, execution);

        JdbcExecutionRequest jdbcExecutionRequestObject = getRequestObject(contentTypeString, requestString, JdbcExecutionRequest.class);

        if (receiveTaskIdString == null)
        {
            executeSync(execution, null, jdbcExecutionRequestObject);
        }
        else
        {
            executeAsync(execution, jdbcExecutionRequestObject, receiveTaskIdString);
        }
    }

    /**
     * Executes the task asynchronously, signaling the task with the given receiveTaskId when the asynchronous process is completed. The task's output - both
     * success and error - will be set whenever the signal occurs.
     * <p/>
     * TODO Find a way to move this method into BaseJavaDelegate since it really should be a generic operation TODO The expressions passed in a JavaDelegate
     * cannot be evaluated when running on a different thread. We must find a way around this if we want to move this.
     *
     * @param execution The current execution.
     * @param jdbcExecutionRequest The request.
     * @param receiveTaskId The ID of the task to signal when done.
     */
    private void executeAsync(final DelegateExecution execution, final JdbcExecutionRequest jdbcExecutionRequest, final String receiveTaskId)
    {
        final String processInstanceId = execution.getProcessInstanceId();
        final String currentActivitiId = execution.getCurrentActivityId();

        // Run the task asynchronously
        herdThreadHelper.executeAsync(new Runnable()
        {
            public void run()
            {
                try
                {
                    try
                    {
                        executeSync(execution, currentActivitiId, jdbcExecutionRequest);
                    }
                    catch (Exception e)
                    {
                        // Handle any exception thrown while executing the task
                        handleException(execution, e);
                    }
                    finally
                    {
                        // Signal the task whether success or failure so that the workflow may continue.
                        // Any exception messages should've been handled by the catch block
                        activitiRuntimeHelper.signal(processInstanceId, receiveTaskId);
                    }
                }
                catch (Exception e)
                {
                    throw new IllegalStateException("Error processing exception. See cause for details.", e);
                }
            }
        });
    }

    /**
     * Executes the task synchronously. overrideActivitiId is provided to customize the name of the variable to set during the execution of the task. This is
     * important when the task is running asynchronously because execution.getCurrentActivitiId() will report the ID the task that the workflow is currently in,
     * and not the ID of the task that was originally executed.
     *
     * @param execution The execution.
     * @param overrideActivitiId Optionally overrides the task id which to use to generate variable names.
     * @param jdbcExecutionRequest The request.
     *
     * @throws Exception When any exception occurs during the execution of the task.
     */
    private void executeSync(DelegateExecution execution, String overrideActivitiId, JdbcExecutionRequest jdbcExecutionRequest) throws Exception
    {
        String executionId = execution.getId();
        String activitiId = execution.getCurrentActivityId();
        if (overrideActivitiId != null)
        {
            activitiId = overrideActivitiId;
        }

        // Execute the request. May throw exception here in case of validation or system errors.
        // Thrown exceptions should be caught by parent implementation.
        JdbcExecutionResponse jdbcExecutionResponse = jdbcService.executeJdbc(jdbcExecutionRequest);

        // Set the response
        setJsonResponseAsWorkflowVariable(jdbcExecutionResponse, executionId, activitiId);

        /*
         * Special handling of error condition.
         * Any one of the requested SQL statements could've failed without throwing an exception.
         * If any of the statements are in ERROR, throw an exception.
         * This ensures that the workflow response variable is still available to the users.
         */
        for (JdbcStatement jdbcStatement : jdbcExecutionResponse.getStatements())
        {
            if (JdbcStatementStatus.ERROR.equals(jdbcStatement.getStatus()))
            {
                throw new IllegalArgumentException("There are failed executions. See JSON response for details.");
            }
        }
    }
}
