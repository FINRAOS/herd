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
package org.finra.dm.service.activiti;

import java.io.Serializable;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.activiti.engine.impl.context.Context;
import org.activiti.engine.impl.interceptor.Command;
import org.activiti.engine.impl.interceptor.CommandContext;
import org.activiti.engine.impl.persistence.deploy.DeploymentManager;
import org.activiti.engine.impl.persistence.entity.ExecutionEntity;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.activiti.engine.runtime.ProcessInstance;
import org.apache.log4j.Logger;

import org.finra.dm.service.helper.DmErrorInformationExceptionHandler;

/**
 * This is an Activiti command that creates and starts a process instance for the process definition Id and uses a process instance holder to communicate the
 * created process instance back to the caller before it is started.
 */
@SuppressFBWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED",
    justification = "This custom Activiti command is invoked explicitly via a REST API so we aren't relying on it being serialized.")
public class CreateAndStartProcessInstanceCmd implements Command<ProcessInstance>, Serializable
{
    private static final Logger LOGGER = Logger.getLogger(CreateAndStartProcessInstanceCmd.class);

    private static final long serialVersionUID = 1L;
    protected String processDefinitionId;
    protected Map<String, Object> parameters;
    protected ProcessInstanceHolder processInstanceHolder;
    protected transient DmErrorInformationExceptionHandler exceptionHandler;

    /**
     * Create and start a create process instance command.
     *
     * @param processDefinitionId the process definition id
     * @param parameters the parameters that are passed to the job instance.
     * @param processInstanceHolder a process instance holder that will be initially populated with the process instance once it is created, but before it is
     * started.
     * @param exceptionHandler an exception handler that can determine what is a reportable error.
     */
    public CreateAndStartProcessInstanceCmd(String processDefinitionId, Map<String, Object> parameters, ProcessInstanceHolder processInstanceHolder,
        DmErrorInformationExceptionHandler exceptionHandler)
    {
        this.processDefinitionId = processDefinitionId;
        this.parameters = parameters;
        this.processInstanceHolder = processInstanceHolder;
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * Executes this command which will create a process instance.
     *
     * @param commandContext the command context.
     *
     * @return the created process instance.
     * @see org.activiti.engine.impl.cmd.StartProcessInstanceCmd#execute(org.activiti.engine.impl.interceptor.CommandContext)
     */
    public ProcessInstance execute(CommandContext commandContext)
    {
        // Get a handle to the Activiti deployment manager.
        DeploymentManager deploymentCache = Context.getProcessEngineConfiguration().getDeploymentManager();

        ProcessDefinitionEntity processDefinition;

        // Find the process definition entity based on the Id.
        try
        {
            processDefinition = deploymentCache.findDeployedProcessDefinitionById(processDefinitionId);
        }
        catch (Exception ex)
        {
            throw new IllegalArgumentException("No process definition found for Id: " + processDefinitionId, ex);
        }

        // Do not start process a process instance if the process definition is suspended.
        if (processDefinition.isSuspended())
        {
            throw new IllegalArgumentException(
                "Cannot start process instance for process definition Id: " + processDefinition.getId() + " because it is suspended.");
        }

        // Create the process instance.
        ExecutionEntity processInstance = processDefinition.createProcessInstance();

        // Set the variables on the process instance which will be available to the workflow when the instance is started.
        processInstance.setVariables(parameters);

        // Place the process instance in the holder so the caller can get access to it, even when this method hasn't yet completed.
        processInstanceHolder.setProcessInstance(processInstance);

        try
        {
            // Start the process instance which runs the workflow and could take a long time to complete depending on the workflow configuration.
            processInstance.start();
        }
        catch (Exception ex)
        {
            // Log something showing the exception. Note that we don't want to re-throw the exception because it will cause the transaction to roll back
            // which will cause the process instance to not be created. Since the user will have already received the process instance back
            // when it was set in the holder above, we want it to remain available in the database for debugging purposes and possibly for the user
            // to continue using.
            if (exceptionHandler.isReportableError(ex))
            {
                // In the case of an error that is reportable, we will log an error.
                LOGGER.error("Unexpected error occurred during asynchronous starting of process with process definition Id \"" + processDefinitionId +
                    "\" and process instance Id: \"" + processInstance.getProcessInstanceId() + "\".", ex);
            }
            else
            {
                // TODO: For now, log a warning since this is a workflow issue and not a system issue.
                // In the future, see if there is a way to set a workflow variable so workflow creators can access it and remove this warning logging.
                LOGGER.warn("Unexpected error occurred during asynchronous starting of process with process definition Id \"" + processDefinitionId +
                    "\" and process instance Id: \"" + processInstance.getProcessInstanceId() +
                    "\". This is a workflow related error and should be handled by the workflow creator.", ex);
            }
        }

        // Return the newly created and started process instance.
        return processInstance;
    }
}
