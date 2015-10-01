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

import java.util.Map;
import java.util.concurrent.Future;

import org.activiti.spring.SpringProcessEngineConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import org.finra.dm.service.helper.DmErrorInformationExceptionHandler;

/**
 * Implementation for the Activiti process instance creator.
 */
@Component
public class ActivitiProcessInstanceCreatorImpl implements ActivitiProcessInstanceCreator
{
    @Autowired
    private SpringProcessEngineConfiguration activitiConfiguration;

    @Autowired
    @Qualifier("dmErrorInformationExceptionHandler") // This is to ensure we get the base class bean rather than any classes that extend it.
    private DmErrorInformationExceptionHandler exceptionHandler;

    /**
     * Asynchronously creates and starts a process instance using a process instance holder to communicate the created process instance to the caller before the
     * process instance is started.
     *
     * @param processDefinitionId the process instance definition Id.
     * @param parameters the runtime parameters of the process instance.
     * @param processInstanceHolder the process instance holder.
     *
     * @return a future to know the asynchronous state of this method.
     * @throws Exception if any errors are encountered while creating the process instance. An exception won't be thrown if the process instance was created,
     * but not started successfully.
     */
    @Async
    @Override
    public Future<Void> createAndStartProcessInstanceAsync(String processDefinitionId, Map<String, Object> parameters,
        ProcessInstanceHolder processInstanceHolder) throws Exception
    {
        createAndStartProcessInstanceSync(processDefinitionId, parameters, processInstanceHolder);
        // Return an AsyncResult so callers will know the future is "done". They can call "isDone" to know when this method has completed and they
        // can call "get" to see if any exceptions were thrown.
        return new AsyncResult<>(null);
    }
    
    /**
     * Synchronously creates and starts a process instance using a process instance holder to communicate the created process instance to the caller.
     *
     * @param processDefinitionId the process instance definition Id.
     * @param parameters the runtime parameters of the process instance.
     * @param processInstanceHolder the process instance holder.
     *
     * @throws Exception if any errors are encountered while creating the process instance. An exception won't be thrown if the process instance was created,
     * but not started successfully.
     */
    @Override
    public void createAndStartProcessInstanceSync(String processDefinitionId, Map<String, Object> parameters,
        ProcessInstanceHolder processInstanceHolder) throws Exception
    {
        // Create and start the job with Activiti.
        activitiConfiguration.getCommandExecutor()
            .execute(new CreateAndStartProcessInstanceCmd(processDefinitionId, parameters, processInstanceHolder, exceptionHandler));
    }
}
