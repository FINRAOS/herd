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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.dao.impl.MockSqsOperationsImpl;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.model.jpa.NotificationMessageEntity;

/**
 * This class tests functionality within the notification message publishing service.
 */
public class NotificationMessagePublishingServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "notificationMessagePublishingServiceImpl")
    private NotificationMessagePublishingService notificationMessagePublishingServiceImpl;

    @Test
    public void testAddNotificationMessageToDatabaseQueue()
    {
        // Create a message type entity.
        messageTypeDaoTestHelper.createMessageTypeEntity(MESSAGE_TYPE);

        // Create a message header.
        List<MessageHeader> messageHeaders = Collections.singletonList(new MessageHeader(KEY, VALUE));

        // Create a notification message.
        NotificationMessage notificationMessage = new NotificationMessage(MESSAGE_TYPE, MESSAGE_DESTINATION, MESSAGE_TEXT, messageHeaders);

        // Add a notification message to the database queue.
        notificationMessagePublishingService.addNotificationMessageToDatabaseQueue(notificationMessage);

        // Retrieve the oldest notification message from the database queue.
        NotificationMessageEntity notificationMessageEntity = notificationMessageDao.getOldestNotificationMessage();

        // Validate the results.
        assertNotNull(notificationMessageEntity);
        assertEquals(MESSAGE_TYPE, notificationMessageEntity.getMessageType().getCode());
        assertEquals(MESSAGE_DESTINATION, notificationMessageEntity.getMessageDestination());
        assertEquals(MESSAGE_TEXT, notificationMessageEntity.getMessageText());
        assertEquals(jsonHelper.objectToJson(messageHeaders), notificationMessageEntity.getMessageHeaders());
    }

    @Test
    public void testAddNotificationMessageToDatabaseQueueNoMessageHeaders()
    {
        // Create a message type entity.
        messageTypeDaoTestHelper.createMessageTypeEntity(MESSAGE_TYPE);

        // Create a notification message without message headers.
        NotificationMessage notificationMessage = new NotificationMessage(MESSAGE_TYPE, MESSAGE_DESTINATION, MESSAGE_TEXT, NO_MESSAGE_HEADERS);

        // Add a notification message to the database queue.
        notificationMessagePublishingService.addNotificationMessageToDatabaseQueue(notificationMessage);

        // Retrieve the oldest notification message from the database queue.
        NotificationMessageEntity notificationMessageEntity = notificationMessageDao.getOldestNotificationMessage();

        // Validate the results.
        assertNotNull(notificationMessageEntity);
        assertEquals(MESSAGE_TYPE, notificationMessageEntity.getMessageType().getCode());
        assertEquals(MESSAGE_DESTINATION, notificationMessageEntity.getMessageDestination());
        assertEquals(MESSAGE_TEXT, notificationMessageEntity.getMessageText());
        assertNull(notificationMessageEntity.getMessageHeaders());
    }

    @Test
    public void testNotificationPublishingServiceMethodsNewTransactionPropagation()
    {
        // Validate that notification message database queue is empty.
        assertNull(notificationMessageDao.getOldestNotificationMessage());

        // Add a notification message to the database queue.
        notificationMessagePublishingServiceImpl.addNotificationMessageToDatabaseQueue(
            new NotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), MESSAGE_DESTINATION, MESSAGE_TEXT,
                Collections.singletonList(new MessageHeader(KEY, VALUE))));

        // Validate that the database queue is not empty now.
        assertNotNull(notificationMessageDao.getOldestNotificationMessage());

        // Publish the notification message from the database queue.
        assertTrue(notificationMessagePublishingServiceImpl.publishOldestNotificationMessageFromDatabaseQueue());

        // Publish notification message directly - not from the database queue.
        notificationMessagePublishingServiceImpl.publishNotificationMessage(
            new NotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), MESSAGE_DESTINATION, MESSAGE_TEXT,
                Collections.singletonList(new MessageHeader(KEY, VALUE))));
    }

    @Test
    public void testPublishNotificationMessage()
    {
        // Publish a notification message with SQS message type.
        notificationMessagePublishingService.publishNotificationMessage(
            new NotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), MESSAGE_DESTINATION, MESSAGE_TEXT,
                Collections.singletonList(new MessageHeader(KEY, VALUE))));

        // Publish a notification message with SNS message type.
        notificationMessagePublishingService.publishNotificationMessage(
            new NotificationMessage(MessageTypeEntity.MessageEventTypes.SNS.name(), MESSAGE_DESTINATION, MESSAGE_TEXT,
                Collections.singletonList(new MessageHeader(KEY, VALUE))));
    }

    @Test
    public void testPublishNotificationMessageInvalidMessageType()
    {
        // Try to publish notification message with an invalid message type.
        try
        {
            notificationMessagePublishingService.publishNotificationMessage(
                new NotificationMessage(I_DO_NOT_EXIST, MESSAGE_DESTINATION, MESSAGE_TEXT, Collections.singletonList(new MessageHeader(KEY, VALUE))));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Notification message type \"%s\" is not supported.", I_DO_NOT_EXIST), e.getMessage());
        }
    }

    @Test
    public void testPublishOldestNotificationMessageFromDatabaseQueue()
    {
        // Create a notification message and add it to the database queue.
        notificationMessageDaoTestHelper.createNotificationMessageEntity(MessageTypeEntity.MessageEventTypes.SQS.name(), AWS_SQS_QUEUE_NAME, MESSAGE_TEXT);

        // Publish the notification message from the database queue.
        assertTrue(notificationMessagePublishingService.publishOldestNotificationMessageFromDatabaseQueue());

        // Confirm that the database queue is empty now by trying to publish the next notification message from the queue.
        assertFalse(notificationMessagePublishingService.publishOldestNotificationMessageFromDatabaseQueue());
    }

    @Test
    public void testPublishOldestNotificationMessageFromDatabaseQueueAwsServiceException()
    {
        // Prepare database entries required for testing.
        NotificationMessageEntity notificationMessageEntity = notificationMessageDaoTestHelper
            .createNotificationMessageEntity(MessageTypeEntity.MessageEventTypes.SQS.name(), MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME, MESSAGE_TEXT);

        // Try to publish notification message.
        try
        {
            notificationMessagePublishingService.publishOldestNotificationMessageFromDatabaseQueue();
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("AWS SQS queue with \"%s\" name not found.", MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME), e.getMessage());
        }

        // Check that the test notification message is still the oldest message in the database queue.
        assertEquals(notificationMessageDao.getOldestNotificationMessage(), notificationMessageEntity);
    }

    @Test
    public void testPublishOldestNotificationMessageFromDatabaseQueueJsonParseException()
    {
        // Prepare database entries required for testing.
        NotificationMessageEntity notificationMessageEntity =
            notificationMessageDaoTestHelper.createNotificationMessageEntity(MESSAGE_TYPE, MESSAGE_DESTINATION, MESSAGE_TEXT);
        notificationMessageEntity.setMessageHeaders(INVALID_VALUE);
        notificationMessageDao.saveAndRefresh(notificationMessageEntity);

        // Try to publish notification message.
        try
        {
            notificationMessagePublishingService.publishOldestNotificationMessageFromDatabaseQueue();
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String
                .format("Failed to unmarshall notification message headers. messageId=%d messageType=%s messageDestination=%s messageText=%s messageHeaders=%s",
                    notificationMessageEntity.getId(), MESSAGE_TYPE, MESSAGE_DESTINATION, MESSAGE_TEXT, INVALID_VALUE), e.getMessage());
        }

        // Check that the test notification message is still the oldest message in the database queue.
        assertEquals(notificationMessageDao.getOldestNotificationMessage(), notificationMessageEntity);
    }
}
