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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.LinkedBlockingQueue;

import org.activiti.engine.ProcessEngineConfiguration;
import org.activiti.engine.impl.asyncexecutor.AsyncExecutor;
import org.activiti.spring.SpringAsyncExecutor;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.AbstractServiceTest;

/**
 * Unit test that asserts Activiti process engine configuration is configured correctly.
 */
public class ActivitiProcessEngineConfigurationTest extends AbstractServiceTest
{
    @Autowired
    private ProcessEngineConfiguration processEngineConfiguration;

    @Autowired
    @Qualifier("getAsyncExecutor")
    private TaskExecutor genericTaskExecutor;

    /**
     * Ensure that the Activiti's thread pool is separate from the application's generic thread pool.
     */
    @Test
    public void testActivitiThreadPoolIsIsolatedFromGenericAsyncPool()
    {
        AsyncExecutor asyncExecutor = processEngineConfiguration.getAsyncExecutor();
        SpringAsyncExecutor springAsyncExecutor = (SpringAsyncExecutor) asyncExecutor;
        TaskExecutor taskExecutor = springAsyncExecutor.getTaskExecutor();

        assertTrue(genericTaskExecutor != taskExecutor);
    }

    /**
     * Ensure that the Activiti's thread pool uses the correct configuration value.
     * 
     * This assertion is limited in that the configuration values must be set before Spring application context is initialized, which we cannot control easily
     * in unit test.
     */
    @Test
    public void testActivitiThreadPoolUsesConfiguredValues()
    {
        AsyncExecutor asyncExecutor = processEngineConfiguration.getAsyncExecutor();
        SpringAsyncExecutor springAsyncExecutor = (SpringAsyncExecutor) asyncExecutor;
        TaskExecutor taskExecutor = springAsyncExecutor.getTaskExecutor();
        ThreadPoolTaskExecutor threadPoolTaskExecutor = (ThreadPoolTaskExecutor) taskExecutor;

        Integer corePoolSize = threadPoolTaskExecutor.getCorePoolSize();
        Integer maxPoolSize = threadPoolTaskExecutor.getMaxPoolSize();
        Integer keepAliveSeconds = threadPoolTaskExecutor.getKeepAliveSeconds();
        // No real easy way of getting the queue capacity from the already constructed thread pool
        Integer remainingCapacity = ((LinkedBlockingQueue<?>) threadPoolTaskExecutor.getThreadPoolExecutor().getQueue()).remainingCapacity();

        assertEquals(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_THREAD_POOL_CORE_POOL_SIZE, Integer.class), corePoolSize);
        assertEquals(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_THREAD_POOL_MAX_POOL_SIZE, Integer.class), maxPoolSize);
        assertEquals(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_THREAD_POOL_KEEP_ALIVE_SECS, Integer.class), keepAliveSeconds);
        assertEquals(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_THREAD_POOL_QUEUE_CAPACITY, Integer.class), remainingCapacity);
    }
}
