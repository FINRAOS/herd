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

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.finra.dm.model.api.xml.JdbcExecutionRequest;
import org.finra.dm.model.api.xml.JdbcExecutionResponse;
import org.finra.dm.model.api.xml.JdbcStatement;
import org.finra.dm.model.api.xml.JdbcStatementStatus;
import org.finra.dm.service.JdbcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The Activiti delegate task for {@link JdbcService#executeJdbc(JdbcExecutionRequest)}.
 * 
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

    @Autowired
    private JdbcService jdbcService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        // Construct request from parameters
        String contentTypeString = activitiHelper.getRequiredExpressionVariableAsString(contentType, execution, "ContentType");
        String requestString = activitiHelper.getRequiredExpressionVariableAsString(jdbcExecutionRequest, execution, "JdbcExecutionRequest").trim();
        JdbcExecutionRequest jdbcExecutionRequestObject = getRequestObject(contentTypeString, requestString, JdbcExecutionRequest.class);

        // Execute the request. May throw exception here in case of validation or system errors.
        // Thrown exceptions should be caught by parent implementation.
        JdbcExecutionResponse jdbcExecutionResponse = jdbcService.executeJdbc(jdbcExecutionRequestObject);

        // Set the response
        setJsonResponseAsWorkflowVariable(jdbcExecutionResponse, execution);

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
