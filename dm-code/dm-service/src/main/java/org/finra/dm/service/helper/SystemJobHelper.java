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
package org.finra.dm.service.helper;

import static org.quartz.TriggerBuilder.newTrigger;

import java.util.List;

import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.stereotype.Component;

import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.systemjobs.AbstractSystemJob;

/**
 * A helper class for shared system job operations.
 */
@Component
public class SystemJobHelper
{
    @Autowired
    private SchedulerFactoryBean schedulerFactory;

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Starts a system job asynchronously.
     *
     * @param jobName the system job name (case-sensitive)
     * @param parameters the list of parameters
     *
     * @throws org.quartz.SchedulerException if fails to schedule the system job
     */
    public void runSystemJob(String jobName, List<Parameter> parameters) throws SchedulerException
    {
        // Validate the system job name.
        AbstractSystemJob systemJob;
        try
        {
            systemJob = (AbstractSystemJob) applicationContext.getBean(jobName);
        }
        catch (Exception e)
        {
            throw new ObjectNotFoundException(String.format("System job with name \"%s\" doesn't exist.", jobName), e);
        }

        // Validate parameters per relative system job.
        systemJob.validateParameters(parameters);

        // Prepare a trigger to run the system job only once.
        TriggerKey triggerKey = TriggerKey.triggerKey(jobName + AbstractSystemJob.RUN_ONCE_TRIGGER_SUFFIX);
        Trigger trigger = newTrigger().withIdentity(triggerKey).forJob(jobName).usingJobData(systemJob.getJobDataMap(parameters)).startNow().build();

        // Schedule the system job.
        schedulerFactory.getScheduler().scheduleJob(trigger);
    }
}
