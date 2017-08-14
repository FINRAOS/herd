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

import java.lang.reflect.Field;

import org.activiti.engine.impl.cmd.ExecuteAsyncJobCmd;
import org.activiti.engine.impl.context.Context;
import org.activiti.engine.impl.interceptor.Command;
import org.activiti.engine.impl.interceptor.CommandConfig;
import org.activiti.engine.impl.interceptor.CommandInvoker;
import org.activiti.engine.impl.persistence.entity.JobEntity;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

/**
 * A custom herd Activiti command invoker that extends the default standard Activiti command invoker so we can handle exceptions being thrown in a custom way.
 */
@Component
public class HerdCommandInvoker extends CommandInvoker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HerdCommandInvoker.class);

    @Override
    public <T> T execute(CommandConfig config, Command<T> command)
    {
        LOGGER.debug("command=\"{}\"", command.getClass().getName());
        try
        {
            // Perform the normal execution.
            return super.execute(config, command);
        }
        catch (Exception e)
        {
            LOGGER.debug(String.format("HerdCommandInvoker caught an exception."), e);
            /*
             * Attempt to handle exception based on the command.
             * If we bubble the exception up here, the transaction will be rolled back and all variables which were not committed will not be persisted.
             * The problem with swallowing the exception, however, is that the exception message is not persisted automatically. To get around it, we must save
             * the exception message and stacktrace into a JobEntity which is associated with the current execution.
             */

            if (command instanceof ExecuteAsyncJobCmd)
            {
                /*
                 * ExecuteAsyncJobCmd is executed when a task is asynchronous.
                 * Save the exception information in the command's JobEntity
                 */
                ExecuteAsyncJobCmd executeAsyncJobCmd = (ExecuteAsyncJobCmd) command;
                JobEntity jobEntity = getJobEntity(executeAsyncJobCmd);
                jobEntity.setExceptionMessage(ExceptionUtils.getMessage(e));
                jobEntity.setExceptionStacktrace(ExceptionUtils.getStackTrace(e));
                return null;
            }
            else
            {
                /*
                 * We do not know how to handle any other commands, so just bubble it up and let Activiti's default mechanism kick in.
                 */
                throw e;
            }
        }
    }

    /**
     * Gets the JobEntity from the given ExecuteAsyncJobCmd.
     *
     * @param executeAsyncJobCmd The ExecuteAsyncJobCmd
     *
     * @return The JobEntity
     */
    private JobEntity getJobEntity(ExecuteAsyncJobCmd executeAsyncJobCmd)
    {
        /*
         * Unfortunately, ExecuteAsyncJobCmd does not provide an accessible method to get the JobEntity stored within it.
         * We use reflection to force the value out of the object.
         * Also, we cannot simply get the entity and update it. We must retrieve it through the entity manager so it registers in Activiti's persistent object
         * cache. This way when the transaction commits, Activiti is aware of any changes in the JobEntity and persists them correctly.
         */
        try
        {
            Field field = ExecuteAsyncJobCmd.class.getDeclaredField("job");
            ReflectionUtils.makeAccessible(field);
            String jobId = ((JobEntity) ReflectionUtils.getField(field, executeAsyncJobCmd)).getId();

            return Context.getCommandContext().getJobEntityManager().findJobById(jobId);
        }
        catch (NoSuchFieldException | SecurityException e)
        {
            /*
             * This exception should not happen.
             */
            throw new IllegalStateException(e);
        }
    }
}
