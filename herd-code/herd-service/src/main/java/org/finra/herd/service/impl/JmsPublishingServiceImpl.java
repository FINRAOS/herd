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
package org.finra.herd.service.impl;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.SqsDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.jpa.JmsMessageEntity;
import org.finra.herd.service.JmsPublishingService;

/**
 * The JMS publishing service implementation.
 */
@Service
public class JmsPublishingServiceImpl implements JmsPublishingService
{
    private static final Logger LOGGER = Logger.getLogger(JmsPublishingServiceImpl.class);

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private SqsDao sqsDao;

    @Autowired
    private AwsHelper awsHelper;

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
    public boolean publishOldestJmsMessage()
    {
        boolean messageSent = false;

        // Retrieve the oldest JMS message, unless the queue is empty.
        JmsMessageEntity jmsMessageEntity = herdDao.getOldestJmsMessage();

        if (jmsMessageEntity != null)
        {
            try
            {
                // Send a text message to the specified AWS SQS queue.
                sqsDao.sendSqsTextMessage(awsHelper.getAwsParamsDto(), jmsMessageEntity.getJmsQueueName(), jmsMessageEntity.getMessageText());
                messageSent = true;
            }
            catch (Exception e)
            {
                LOGGER.error(String
                    .format("Failed to post message on \"%s\" SQS queue. Message: %s", jmsMessageEntity.getJmsQueueName(), jmsMessageEntity.getMessageText()));

                // Throw the exception up.
                throw new IllegalStateException(e.getMessage(), e);
            }

            // Delete this message from the queue.
            herdDao.delete(jmsMessageEntity);
        }

        return messageSent;
    }
}
