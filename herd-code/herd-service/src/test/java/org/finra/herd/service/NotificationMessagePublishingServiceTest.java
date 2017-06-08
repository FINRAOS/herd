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

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.dao.impl.MockSqsOperationsImpl;
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
    public void NotificationPublishingServiceMethodsNewTransactionPropagation()
    {
        // Validate that notification message database queue is empty.
        assertNull(notificationMessageDao.getOldestNotificationMessage());

        // Add a notification message to the database queue.
        notificationMessagePublishingServiceImpl
            .addNotificationMessageToDatabaseQueue(new NotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), MESSAGE_DESTINATION, MESSAGE_TEXT));

        // Validate that the database queue is not empty now.
        assertNotNull(notificationMessageDao.getOldestNotificationMessage());

        // Publish the notification message from the database queue.
        assertTrue(notificationMessagePublishingServiceImpl.publishOldestNotificationMessageFromDatabaseQueue());
    }

    @Test
    public void testPublishOldestNotificationMessageFromDatabaseQueue()
    {
        // Create a notification message and add it to the database queue.
        notificationMessageDaoTestHelper.createNotificationMessageEntity(MessageTypeEntity.MessageEventTypes.SQS.name(), AWS_SQS_QUEUE_NAME, MESSAGE_TEXT);

        // Validate the results by ensuring there is only 1 message that got published (i.e. true for the first publish call and false for the second one since
        // only one messaged is queued).
        assertTrue(notificationMessagePublishingService.publishOldestNotificationMessageFromDatabaseQueue());
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
}
