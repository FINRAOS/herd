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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.JmsMessageDao;
import org.finra.herd.dao.SqsDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.dto.JmsMessage;
import org.finra.herd.model.jpa.JmsMessageEntity;
import org.finra.herd.service.JmsPublishingService;
import org.finra.herd.service.helper.JmsMessageDaoHelper;

/**
 * The JMS publishing service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class JmsPublishingServiceImpl implements JmsPublishingService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JmsPublishingServiceImpl.class);

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private JmsMessageDao jmsMessageDao;

    @Autowired
    private JmsMessageDaoHelper jmsMessageDaoHelper;

    @Autowired
    private SqsDao sqsDao;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public JmsMessage addJmsMessageToDatabaseQueue(String jmsQueueName, String messageText)
    {
        return addJmsMessageToDatabaseQueueImpl(jmsQueueName, messageText);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean publishOldestJmsMessageFromDatabaseQueue()
    {
        return publishOldestJmsMessageFromDatabaseQueueImpl();
    }

    /**
     * Adds a JMS message to the database queue.
     *
     * @param jmsQueueName the JMS queue name
     * @param messageText the message text
     *
     * @return the JMS message added to the queue
     */
    protected JmsMessage addJmsMessageToDatabaseQueueImpl(String jmsQueueName, String messageText)
    {
        JmsMessageEntity jmsMessageEntity = jmsMessageDaoHelper.addJmsMessageToDatabaseQueue(jmsQueueName, messageText);
        return new JmsMessage(jmsMessageEntity.getJmsQueueName(), jmsMessageEntity.getMessageText());
    }

    /**
     * Publishes and removes from the database queue the oldest JMS message.
     *
     * @return true if a message was sent or false if no message was sent (i.e. no message needed to be sent).
     */
    protected boolean publishOldestJmsMessageFromDatabaseQueueImpl()
    {
        boolean messageSent = false;

        // Retrieve the oldest JMS message, unless the queue is empty.
        JmsMessageEntity jmsMessageEntity = jmsMessageDao.getOldestJmsMessage();

        if (jmsMessageEntity != null)
        {
            try
            {
                LOGGER.debug(String
                    .format("send SQS text message: id: %s, jmsQueueName: %s, messageText:%n%s", jmsMessageEntity.getId(), jmsMessageEntity.getJmsQueueName(),
                        jmsMessageEntity.getMessageText()));
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
            jmsMessageDao.delete(jmsMessageEntity);
        }

        return messageSent;
    }
}
