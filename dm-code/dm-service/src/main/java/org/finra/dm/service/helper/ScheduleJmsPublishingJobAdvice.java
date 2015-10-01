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

import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.quartz.ObjectAlreadyExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.dm.service.systemjobs.JmsPublishingJob;

/**
 * Advice that schedules the JMS publishing system job.
 */
@Component
public class ScheduleJmsPublishingJobAdvice
{
    @Autowired
    private SystemJobHelper systemJobHelper;

    private static final Logger LOGGER = Logger.getLogger(ScheduleJmsPublishingJobAdvice.class);

    private static final ThreadLocal<Boolean> SCHEDULE_JMS_PUBLISHING_JOB_HOLDER = new ThreadLocal<Boolean>()
    {
        @Override
        protected Boolean initialValue()
        {
            return Boolean.FALSE;
        }
    };
    
    /**
     * Sets to schedule the JMS publishing job right away.
     */
    public static void setScheduleJmsPublishingJob()
    {
        SCHEDULE_JMS_PUBLISHING_JOB_HOLDER.set(Boolean.TRUE);
    }

    /**
     * Schedule the JMS publishing system job if requested in the ThreadLocal variable startJmsPublishingJobHolder.
     *
     * @param pjp the join point.
     *
     * @return the return value of the method at the join point.
     * @throws Throwable if any errors were encountered.
     */
    public Object scheduleJmsPublishingJob(ProceedingJoinPoint pjp) throws Throwable
    {
        // Proceed to the join point (i.e. call the method and let it return).
        Object returnValue = pjp.proceed();

        // Check if need to start JMS publishing job.
        if (SCHEDULE_JMS_PUBLISHING_JOB_HOLDER.get())
        {
            try
            {
                LOGGER.debug("Scheduling JMS publishing job to run.");
                systemJobHelper.runSystemJob(JmsPublishingJob.JOB_NAME, null);
            }
            catch (ObjectAlreadyExistsException  oaeex)
            {
                // Ignore the error when job is already running.
                LOGGER.debug("Failed to schedule the JMS publishing job.", oaeex);
            }
            catch (Exception ex)
            {
                LOGGER.error("Failed to schedule the JMS publishing job.", ex);
            }
        }

        return returnValue;
    }
}