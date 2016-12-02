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
package org.finra.herd.service.advice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.dao.impl.MockSqsOperationsImpl;
import org.finra.herd.model.annotation.PublishJmsMessages;
import org.finra.herd.model.dto.JmsMessage;
import org.finra.herd.model.jpa.JmsMessageEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.helper.JmsMessageInMemoryQueue;

public class PublishJmsMessagesAdviceTest extends AbstractServiceTest
{
    @Autowired
    private JmsMessageInMemoryQueue jmsMessageInMemoryQueue;

    @Autowired
    private PublishJmsMessagesAdvice publishJmsMessagesAdvice;

    @Test
    public void testPublishJmsMessages() throws Throwable
    {
        // Create a JMS message.
        JmsMessage jmsMessage = new JmsMessage(SQS_QUEUE_NAME, MESSAGE_TEXT);

        // Clean up "in-memory" JMS message queue.
        jmsMessageInMemoryQueue.clear();

        // Validate that "in-memory" JMS message queue is empty.
        assertTrue(jmsMessageInMemoryQueue.isEmpty());

        // Add JMS message to the "in-memory" queue for publishing.
        jmsMessageInMemoryQueue.add(jmsMessage);

        // Validate that "in-memory" JMS message queue now has one element.
        assertEquals(1, jmsMessageInMemoryQueue.size());

        // Validate that the JMS message database queue is empty.
        assertNull(jmsMessageDao.getOldestJmsMessage());

        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint = getMockedProceedingJoinPoint("testPublishJmsMessages");

        // Publish JMS messages stored in the queue.
        publishJmsMessagesAdvice.publishJmsMessages(joinPoint);

        // Validate that "in-memory" JMS message queue is now empty.
        assertTrue(jmsMessageInMemoryQueue.isEmpty());

        // Validate that the JMS message database queue is still empty.
        assertNull(jmsMessageDao.getOldestJmsMessage());
    }

    @Test
    public void testPublishJmsMessagesDebugEnabled() throws Throwable
    {
        LogLevel originalLevel = getLogLevel(PublishJmsMessagesAdvice.class);

        try
        {
            // Set the logger level to debug.
            setLogLevel(PublishJmsMessagesAdvice.class, LogLevel.DEBUG);

            // Execute the happy path unit test with DEBUG logging level enabled.
            testPublishJmsMessages();
        }
        finally
        {
            // Restore logger level to the original value.
            setLogLevel(PublishJmsMessagesAdvice.class, originalLevel);
        }
    }

    @Test
    public void testPublishJmsMessagesAwsServiceException() throws Throwable
    {
        // Create a JMS message with a mocked SQS queue name that causes an AWS service exception when trying to post a SQS message.
        JmsMessage jmsMessage = new JmsMessage(MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME, MESSAGE_TEXT);

        // Clean up "in-memory" JMS message queue.
        jmsMessageInMemoryQueue.clear();

        // Validate that "in-memory" JMS message queue is empty.
        assertTrue(jmsMessageInMemoryQueue.isEmpty());

        // Add JMS message to the "in-memory" queue for publishing.
        jmsMessageInMemoryQueue.add(jmsMessage);

        // Validate that "in-memory" JMS message queue now has one element.
        assertEquals(1, jmsMessageInMemoryQueue.size());

        // Validate that the JMS message database queue is empty.
        assertNull(jmsMessageDao.getOldestJmsMessage());

        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint = getMockedProceedingJoinPoint("testPublishJmsMessagesAwsServiceException");

        // Try to publish JMS message stored in the "in-memory" queue when specified SQS queue is not valid.
        publishJmsMessagesAdvice.publishJmsMessages(joinPoint);

        // Validate that "in-memory" JMS message queue is now empty.
        assertTrue(jmsMessageInMemoryQueue.isEmpty());

        // Check that the test JMS message is now queued in the database queue.
        JmsMessageEntity jmsMessageEntity = jmsMessageDao.getOldestJmsMessage();
        assertNotNull(jmsMessageEntity);
        assertEquals(MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME, jmsMessageEntity.getJmsQueueName());
        assertEquals(MESSAGE_TEXT, jmsMessageEntity.getMessageText());
    }

    /**
     * Creates and returns a mocked join point of the method call.
     *
     * @param methodName the name of the method
     *
     * @return the mocked ProceedingJoinPoint
     */
    private ProceedingJoinPoint getMockedProceedingJoinPoint(String methodName) throws Exception
    {
        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        Method method = PublishJmsMessagesAdviceTest.class.getDeclaredMethod("mockMethod");
        when(joinPoint.getTarget()).thenReturn(new PublishJmsMessagesAdviceTest());
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(methodSignature.getName()).thenReturn(methodName);

        return joinPoint;
    }

    /**
     * Do not invoke this method. This method is a test input for reflection related tests.
     */
    @PublishJmsMessages
    private void mockMethod()
    {
    }
}
