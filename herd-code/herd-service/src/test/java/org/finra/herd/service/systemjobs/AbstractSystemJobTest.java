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
package org.finra.herd.service.systemjobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import org.finra.herd.service.AbstractServiceTest;

public class AbstractSystemJobTest extends AbstractServiceTest
{
    private static Logger LOGGER = LoggerFactory.getLogger(AbstractSystemJobTest.class);

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    public void testGetCronExpression() throws ParseException
    {
        // Build a list of system jobs to be scheduled.
        Map<String, AbstractSystemJob> systemJobs = applicationContext.getBeansOfType(AbstractSystemJob.class);

        // Validate that we located the expected number of system job.
        assertEquals(8, CollectionUtils.size(systemJobs));

        // Validate cron expression configured in the system for each system job.
        for (Map.Entry<String, AbstractSystemJob> entry : systemJobs.entrySet())
        {
            // Get the name and the system job implementation.
            String jobName = entry.getKey();
            AbstractSystemJob systemJob = entry.getValue();

            // Get the cron expression configured in the system for the job.
            String cronExpressionAsText = systemJob.getCronExpression();
            LOGGER.info(String.format("Testing cron expression \"%s\" specified for \"%s\" system job...", cronExpressionAsText, jobName));

            // Validate the cron expression.
            if (CronExpression.isValidExpression(cronExpressionAsText))
            {
                CronExpression cronExpression = new CronExpression(cronExpressionAsText);
                LOGGER.info(String.format("Next valid time for \"%s\" cron expression after now is \"%s\".", cronExpressionAsText,
                    cronExpression.getNextValidTimeAfter(new Date())));
            }
            else
            {
                fail(String.format("Cron expression \"%s\" specified for \"%s\" system job is not valid.", cronExpressionAsText, jobName));
            }
        }
    }
}
