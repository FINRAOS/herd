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
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.model.jpa.NotificationMessageEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.helper.NotificationMessageInMemoryQueue;

public class PublishNotificationMessagesAdviceTest extends AbstractServiceTest
{
    @Autowired
    private NotificationMessageInMemoryQueue notificationMessageInMemoryQueue;

    @Autowired
    private PublishNotificationMessagesAdvice publishNotificationMessagesAdvice;

    @Test
    public void testPublishNotificationMessages() throws Throwable
    {
        // Create a notification message.
        NotificationMessage notificationMessage = new NotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), AWS_SQS_QUEUE_NAME, MESSAGE_TEXT);

        // Clean up "in-memory" JMS message queue.
        notificationMessageInMemoryQueue.clear();

        // Validate that "in-memory" JMS message queue is empty.
        assertTrue(notificationMessageInMemoryQueue.isEmpty());

        // Add JMS message to the "in-memory" queue for publishing.
        notificationMessageInMemoryQueue.add(notificationMessage);

        // Validate that "in-memory" JMS message queue now has one element.
        assertEquals(1, notificationMessageInMemoryQueue.size());

        // Validate that the JMS message database queue is empty.
        assertNull(notificationMessageDao.getOldestNotificationMessage());

        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint = getMockedProceedingJoinPoint("testPublishNotificationMessages");

        // Publish JMS messages stored in the queue.
        publishNotificationMessagesAdvice.publishNotificationMessages(joinPoint);

        // Validate that "in-memory" JMS message queue is now empty.
        assertTrue(notificationMessageInMemoryQueue.isEmpty());

        // Validate that the JMS message database queue is still empty.
        assertNull(notificationMessageDao.getOldestNotificationMessage());
    }

    @Test
    public void testPublishNotificationMessagesAwsServiceException() throws Throwable
    {
        // Create a JMS message with a mocked SQS queue name that causes an AWS service exception when trying to post a SQS message.
        NotificationMessage notificationMessage =
            new NotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME, MESSAGE_TEXT);

        // Clean up "in-memory" JMS message queue.
        notificationMessageInMemoryQueue.clear();

        // Validate that "in-memory" JMS message queue is empty.
        assertTrue(notificationMessageInMemoryQueue.isEmpty());

        // Add JMS message to the "in-memory" queue for publishing.
        notificationMessageInMemoryQueue.add(notificationMessage);

        // Validate that "in-memory" JMS message queue now has one element.
        assertEquals(1, notificationMessageInMemoryQueue.size());

        // Validate that the JMS message database queue is empty.
        assertNull(notificationMessageDao.getOldestNotificationMessage());

        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint = getMockedProceedingJoinPoint("testPublishNotificationMessagesAwsServiceException");

        // Try to publish JMS message stored in the "in-memory" queue when specified SQS queue is not valid.
        publishNotificationMessagesAdvice.publishNotificationMessages(joinPoint);

        // Validate that "in-memory" JMS message queue is now empty.
        assertTrue(notificationMessageInMemoryQueue.isEmpty());

        // Check that the test JMS message is now queued in the database queue.
        NotificationMessageEntity notificationMessageEntity = notificationMessageDao.getOldestNotificationMessage();
        assertNotNull(notificationMessageEntity);
        assertEquals(MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME, notificationMessageEntity.getMessageDestination());
        assertEquals(MESSAGE_TEXT, notificationMessageEntity.getMessageText());
    }

    @Test
    public void testPublishNotificationMessagesDebugEnabled() throws Throwable
    {
        LogLevel originalLevel = getLogLevel(PublishNotificationMessagesAdvice.class);

        try
        {
            // Set the logger level to debug.
            setLogLevel(PublishNotificationMessagesAdvice.class, LogLevel.DEBUG);

            // Execute the happy path unit test with DEBUG logging level enabled.
            testPublishNotificationMessages();
        }
        finally
        {
            // Restore logger level to the original value.
            setLogLevel(PublishNotificationMessagesAdvice.class, originalLevel);
        }
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
        Method method = PublishNotificationMessagesAdviceTest.class.getDeclaredMethod("mockMethod");
        when(joinPoint.getTarget()).thenReturn(new PublishNotificationMessagesAdviceTest());
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(methodSignature.getName()).thenReturn(methodName);

        return joinPoint;
    }

    /**
     * Do not invoke this method. This method is a test input for reflection related tests.
     */
    @PublishNotificationMessages
    private void mockMethod()
    {
    }
}
