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
import org.finra.herd.model.jpa.JmsMessageEntity;

/**
 * This class tests functionality within the JmsPublishingService.
 */
public class JmsPublishingServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "jmsPublishingServiceImpl")
    private JmsPublishingService jmsPublishingServiceImpl;

    @Test
    public void testPublishOldestJmsMessageFromDatabaseQueue() throws Exception
    {
        // Create only 1 message to be sent in the database.
        jmsMessageDaoTestHelper.createJmsMessageEntity(JMS_QUEUE_NAME, MESSAGE_TEXT);

        // Validate the results by ensuring there is only 1 message that got published (i.e. true for the first message and false for the second one since
        // only 1 exists).
        assertTrue(jmsPublishingService.publishOldestJmsMessageFromDatabaseQueue());
        assertFalse(jmsPublishingService.publishOldestJmsMessageFromDatabaseQueue());
    }

    @Test
    public void testPublishOldestJmsMessageFromDatabaseQueueAwsServiceException() throws Exception
    {
        // Prepare database entries required for testing.
        jmsMessageDaoTestHelper.createJmsMessageEntity(MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME, MESSAGE_TEXT);

        // Try to publish a JMS message which should fail since the database message has an invalid queue name.
        try
        {
            jmsPublishingService.publishOldestJmsMessageFromDatabaseQueue();
            fail("Should throw a RuntimeException when AWS SQS queue does not exist.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("AWS SQS queue with \"%s\" name not found.", MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME), e.getMessage());
        }

        // Check that the test JMS message is still the oldest message in the database queue.
        JmsMessageEntity jmsMessageEntity = jmsMessageDao.getOldestJmsMessage();
        assertNotNull(jmsMessageEntity);
        assertEquals(MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME, jmsMessageEntity.getJmsQueueName());
        assertEquals(MESSAGE_TEXT, jmsMessageEntity.getMessageText());
    }

    /**
     * This method is to get coverage for the JMS publishing service methods that have explicit transaction propagation annotation.
     */
    @Test
    public void JmsPublishingServiceMethodsNewTransactionPropagation()
    {
        // Validate that the JMS message database queue is empty.
        assertNull(jmsMessageDao.getOldestJmsMessage());

        // Add a JMS message to the database queue.
        jmsPublishingServiceImpl.addJmsMessageToDatabaseQueue(SQS_QUEUE_NAME, MESSAGE_TEXT);

        // Validate that the database queue is not empty now.
        assertNotNull(jmsMessageDao.getOldestJmsMessage());

        // Publish the JMS message from the database queue.
        assertTrue(jmsPublishingServiceImpl.publishOldestJmsMessageFromDatabaseQueue());
    }
}
