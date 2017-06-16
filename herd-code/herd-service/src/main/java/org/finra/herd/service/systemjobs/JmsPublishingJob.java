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

import java.util.List;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.NotificationMessagePublishingService;

/**
 * The notification message publishing job.
 */
@Component(JmsPublishingJob.JOB_NAME)
@DisallowConcurrentExecution
public class JmsPublishingJob extends AbstractSystemJob
{
    public static final String JOB_NAME = "jmsPublishing";

    private static final Logger LOGGER = LoggerFactory.getLogger(JmsPublishingJob.class);

    @Autowired
    private NotificationMessagePublishingService notificationMessagePublishingService;

    @Override
    public String getCronExpression()
    {
        return configurationHelper.getProperty(ConfigurationValue.JMS_PUBLISHING_JOB_CRON_EXPRESSION);
    }

    @Override
    public JobDataMap getJobDataMap()
    {
        return getJobDataMapWithoutParameters();
    }

    @Override
    public void validateParameters(List<Parameter> parameters)
    {
        // This system job accepts no parameters.
        Assert.isTrue(CollectionUtils.isEmpty(parameters), String.format("\"%s\" system job does not except parameters.", JOB_NAME));
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException
    {
        // Log that the system job is started.
        LOGGER.info("Started system job. systemJobName=\"{}\"", JOB_NAME);

        // Publish all notification messages stored in the database queue.
        int count = 0;
        try
        {
            while (notificationMessagePublishingService.publishOldestNotificationMessageFromDatabaseQueue())
            {
                count++;
            }
        }
        catch (Exception e)
        {
            // Log the exception.
            LOGGER.error("Failed to publish a notification message. systemJobName=\"{}\"", JOB_NAME, e);
        }

        // Log the number of notification messages successfully published.
        LOGGER.info("Published {} notification messages. systemJobName=\"{}\"", Integer.toString(count), JOB_NAME);

        // Log that the system job is ended.
        LOGGER.info("Completed system job. systemJobName=\"{}\"", JOB_NAME);
    }
}
