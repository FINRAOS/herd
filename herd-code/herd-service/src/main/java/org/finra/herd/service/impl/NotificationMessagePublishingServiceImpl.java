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

import java.io.IOException;
import java.util.List;

import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sqs.model.SendMessageResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.NotificationMessageDao;
import org.finra.herd.dao.SnsDao;
import org.finra.herd.dao.SqsDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.model.jpa.NotificationMessageEntity;
import org.finra.herd.service.NotificationMessagePublishingService;
import org.finra.herd.service.helper.MessageTypeDaoHelper;

/**
 * The notification publishing service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class NotificationMessagePublishingServiceImpl implements NotificationMessagePublishingService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationMessagePublishingServiceImpl.class);

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private MessageTypeDaoHelper messageTypeDaoHelper;

    @Autowired
    private NotificationMessageDao notificationMessageDao;

    @Autowired
    private SnsDao snsDao;

    @Autowired
    private SqsDao sqsDao;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void addNotificationMessageToDatabaseQueue(NotificationMessage notificationMessage)
    {
        addNotificationMessageToDatabaseQueueImpl(notificationMessage);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation executes non-transactionally, suspends the current transaction if one exists.
     */
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void publishNotificationMessage(NotificationMessage notificationMessage)
    {
        publishNotificationMessageImpl(notificationMessage);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean publishOldestNotificationMessageFromDatabaseQueue()
    {
        return publishOldestNotificationMessageFromDatabaseQueueImpl();
    }

    /**
     * Adds a notification message to the database queue.
     *
     * @param notificationMessage the notification message
     */
    protected void addNotificationMessageToDatabaseQueueImpl(NotificationMessage notificationMessage)
    {
        // Get a message type entity and ensure it exists.
        MessageTypeEntity messageTypeEntity = messageTypeDaoHelper.getMessageTypeEntity(notificationMessage.getMessageType());

        // Create and persist a notification message entity.
        NotificationMessageEntity notificationMessageEntity = new NotificationMessageEntity();
        notificationMessageEntity.setMessageType(messageTypeEntity);
        notificationMessageEntity.setMessageDestination(notificationMessage.getMessageDestination());
        notificationMessageEntity.setMessageText(notificationMessage.getMessageText());
        if (CollectionUtils.isNotEmpty(notificationMessage.getMessageHeaders()))
        {
            notificationMessageEntity.setMessageHeaders(jsonHelper.objectToJson(notificationMessage.getMessageHeaders()));
        }
        notificationMessageDao.saveAndRefresh(notificationMessageEntity);
    }

    /**
     * Publishes a notification message.
     *
     * @param notificationMessage the notification message
     */
    protected void publishNotificationMessageImpl(NotificationMessage notificationMessage)
    {
        try
        {
            // Send notification message to the specified destination.
            if (notificationMessage.getMessageType().equals(MessageTypeEntity.MessageEventTypes.SQS.name()))
            {
                // Publish the message using SQS.
                SendMessageResult sendMessageResult = sqsDao
                    .sendMessage(awsHelper.getAwsParamsDto(), notificationMessage.getMessageDestination(), notificationMessage.getMessageText(),
                        notificationMessage.getMessageHeaders());

                // Log response and the message information.
                LOGGER.info("Published {} notification message. messageDestination=\"{}\" messageText={} messageHeaders={} sendMessageResult={}",
                    notificationMessage.getMessageType(), notificationMessage.getMessageDestination(), notificationMessage.getMessageText(),
                    jsonHelper.objectToJson(notificationMessage.getMessageHeaders()), jsonHelper.objectToJson(sendMessageResult));
            }
            else if (notificationMessage.getMessageType().equals(MessageTypeEntity.MessageEventTypes.SNS.name()))
            {
                // Publish the message using SNS.
                PublishResult publishResult = snsDao
                    .publish(awsHelper.getAwsParamsDto(), notificationMessage.getMessageDestination(), notificationMessage.getMessageText(),
                        notificationMessage.getMessageHeaders());

                // Log response and the message information.
                LOGGER.info("Published {} notification message. messageDestination=\"{}\" messageText={} messageHeaders={} publishResult={}",
                    notificationMessage.getMessageType(), notificationMessage.getMessageDestination(), notificationMessage.getMessageText(),
                    jsonHelper.objectToJson(notificationMessage.getMessageHeaders()), jsonHelper.objectToJson(publishResult));
            }
            else
            {
                throw new IllegalStateException(String.format("Notification message type \"%s\" is not supported.", notificationMessage.getMessageType()));
            }
        }
        catch (RuntimeException e)
        {
            // Log an error message.
            LOGGER.error("Failed to publish {} notification message to \"{}\" destination. messageText={} messageHeaders={}",
                notificationMessage.getMessageType(), notificationMessage.getMessageDestination(), notificationMessage.getMessageText(),
                jsonHelper.objectToJson(notificationMessage.getMessageHeaders()));

            // Rethrow the exception.
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * Publishes and removes from the database queue the oldest notification message.
     *
     * @return true if notification message was successfully published and false otherwise
     */
    @SuppressWarnings("unchecked")
    protected boolean publishOldestNotificationMessageFromDatabaseQueueImpl()
    {
        // Initialize the result flag to false.
        boolean result = false;

        // Retrieve the oldest notification message from the database queue, unless the queue is empty.
        NotificationMessageEntity notificationMessageEntity = notificationMessageDao.getOldestNotificationMessage();

        // If message is retrieved, publish and remove it from the queue.
        if (notificationMessageEntity != null)
        {
            // Get the message headers from the entity.
            List<MessageHeader> messageHeaders = null;
            if (StringUtils.isNotBlank(notificationMessageEntity.getMessageHeaders()))
            {
                try
                {
                    messageHeaders = jsonHelper.unmarshallJsonToListOfObjects(MessageHeader.class, notificationMessageEntity.getMessageHeaders());
                }
                catch (IOException e)
                {
                    throw new IllegalStateException(String.format("Failed to unmarshall notification message headers. " +
                        "messageId=%d messageType=%s messageDestination=%s messageText=%s messageHeaders=%s", notificationMessageEntity.getId(),
                        notificationMessageEntity.getMessageType().getCode(), notificationMessageEntity.getMessageDestination(),
                        notificationMessageEntity.getMessageText(), notificationMessageEntity.getMessageHeaders()), e);
                }
            }

            // Publish notification message.
            publishNotificationMessageImpl(
                new NotificationMessage(notificationMessageEntity.getMessageType().getCode(), notificationMessageEntity.getMessageDestination(),
                    notificationMessageEntity.getMessageText(), messageHeaders));

            // Delete this message from the queue.
            notificationMessageDao.delete(notificationMessageEntity);

            // Set the result flag to true.
            result = true;
        }

        return result;
    }
}
