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

import org.activiti.engine.impl.cmd.ExecuteJobsCmd;
import org.activiti.engine.impl.context.Context;
import org.activiti.engine.impl.interceptor.Command;
import org.activiti.engine.impl.interceptor.CommandConfig;
import org.activiti.engine.impl.interceptor.CommandInvoker;
import org.activiti.engine.impl.persistence.entity.ExecutionEntity;
import org.activiti.engine.impl.persistence.entity.JobEntity;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.finra.herd.service.activiti.task.BaseJavaDelegate;
import org.finra.herd.service.helper.HerdErrorInformationExceptionHandler;

/**
 * A custom herd Activiti command invoker that extends the default standard Activiti command invoker so we can handle exceptions being thrown in a custom way.
 */
@Component
public class HerdCommandInvoker extends CommandInvoker
{
    private static final Logger LOGGER = Logger.getLogger(BaseJavaDelegate.class);

    @Autowired
    private ActivitiHelper activitiHelper;

    @Autowired
    @Qualifier("herdErrorInformationExceptionHandler") // This is to ensure we get the base class bean rather than any classes that extend it.
    private HerdErrorInformationExceptionHandler errorInformationExceptionHandler;

    @Override
    public <T> T execute(CommandConfig config, Command<T> command)
    {
        // NOTE: Attempting to set process instance variables here had resulted in a problem when asynchronous tasks were encountered.
        // In this case, the task somehow is getting marked as one which needs to be retried (i.e. a message entry in ACT_RU_JOB gets added) forever.
        // This essentially gets the workflow stuck on the async task. Beware of this situation when trying to address the "set variable" issue mentioned
        // below in the TODO comments.
        try
        {
            // Perform the normal execution.
            return super.execute(config, command);
        }
        catch (Exception e)
        {
            // We want to perform some custom steps (i.e. logging) in the case of a "execute jobs command" which occurs when an async task is complete.
            if (command instanceof ExecuteJobsCmd)
            {
                // Get a handle to the Activiti execution.
                ExecuteJobsCmd executeJobsCmd = (ExecuteJobsCmd) command;
                ExecutionEntity execution = getExecution(executeJobsCmd);

                // TODO: We want to set the error status and error message as workflow variables so workflows will have a way to detect and react to errors.
                if (errorInformationExceptionHandler.isReportableError(e))
                {
                    // In the case of an error that is reportable, we will log an error.
                    LOGGER.error(
                        activitiHelper.getProcessIdentifyingInformation(execution) + " Unexpected error occurred during task \"" + getClass().getSimpleName() +
                            "\".", e);
                }
                else
                {
                    // TODO: In the case where we don't want to report an error, we will log a warning for now since setting workflow variables above
                    // isn't working. We can remove this warning log message if we can find another way to notify the workflow (i.e. after the previous
                    // TODO has been addressed).
                    LOGGER.warn(activitiHelper.getProcessIdentifyingInformation(execution) +
                        " This is a workflow related error and should be handled by the workflow creator \"" + getClass().getSimpleName() + "\".", e);
                }
            }

            // Re-throw the exception.
            throw e;
        }
    }

    /**
     * Gets the execution entity from the execute jobs command.
     *
     * @param executeJobsCmd the execute jobs command.
     *
     * @return the execution entity.
     */
    private ExecutionEntity getExecution(ExecuteJobsCmd executeJobsCmd)
    {
        JobEntity job = Context.getCommandContext().getJobEntityManager().findJobById(executeJobsCmd.getJobId());
        return Context.getCommandContext().getExecutionEntityManager().findExecutionById(job.getExecutionId());
    }
}
