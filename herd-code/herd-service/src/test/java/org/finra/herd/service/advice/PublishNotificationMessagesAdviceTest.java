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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;

import com.amazonaws.AmazonServiceException;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.MessageTypeEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.helper.NotificationMessageInMemoryQueue;
import org.finra.herd.service.impl.NotificationMessagePublishingServiceImpl;

public class PublishNotificationMessagesAdviceTest extends AbstractServiceTest
{
    @Mock
    private NotificationMessageInMemoryQueue notificationMessageInMemoryQueue;

    @Mock
    private NotificationMessagePublishingServiceImpl notificationMessagePublishingService;

    @InjectMocks
    private PublishNotificationMessagesAdvice publishNotificationMessagesAdvice;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testPublishNotificationMessages() throws Throwable
    {
        // Create a notification message.
        NotificationMessage notificationMessage = new NotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), AWS_SQS_QUEUE_NAME, MESSAGE_TEXT,
            Collections.singletonList(new MessageHeader(KEY, VALUE)));

        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint = getMockedProceedingJoinPoint("testPublishNotificationMessages");

        // Mock the external calls.
        doCallRealMethod().when(notificationMessageInMemoryQueue).clear();
        doCallRealMethod().when(notificationMessageInMemoryQueue).add(notificationMessage);
        when(notificationMessageInMemoryQueue.isEmpty()).thenCallRealMethod();
        doCallRealMethod().when(notificationMessageInMemoryQueue).remove();

        // Clear the queue.
        notificationMessageInMemoryQueue.clear();

        // Add the notification message to the queue.
        notificationMessageInMemoryQueue.add(notificationMessage);

        // Validate that the queue is not empty now.
        assertFalse(notificationMessageInMemoryQueue.isEmpty());

        // Call the method under test.
        publishNotificationMessagesAdvice.publishNotificationMessages(joinPoint);

        // Verify the external calls.
        verify(notificationMessageInMemoryQueue, times(2)).clear();
        verify(notificationMessageInMemoryQueue).add(notificationMessage);
        verify(notificationMessageInMemoryQueue, times(3)).isEmpty();
        verify(notificationMessageInMemoryQueue).remove();
        verify(notificationMessagePublishingService).publishNotificationMessage(notificationMessage);
        verify(notificationMessageInMemoryQueue).size();
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertTrue(notificationMessageInMemoryQueue.isEmpty());
    }

    @Test
    public void testPublishNotificationMessagesAmazonServiceException() throws Throwable
    {
        // Create a notification message.
        NotificationMessage notificationMessage = new NotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), AWS_SQS_QUEUE_NAME, MESSAGE_TEXT,
            Collections.singletonList(new MessageHeader(KEY, VALUE)));

        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint = getMockedProceedingJoinPoint("testPublishNotificationMessages");

        // Mock the external calls.
        doCallRealMethod().when(notificationMessageInMemoryQueue).clear();
        doCallRealMethod().when(notificationMessageInMemoryQueue).add(notificationMessage);
        when(notificationMessageInMemoryQueue.isEmpty()).thenCallRealMethod();
        doCallRealMethod().when(notificationMessageInMemoryQueue).remove();
        doThrow(new AmazonServiceException(ERROR_MESSAGE)).when(notificationMessagePublishingService).publishNotificationMessage(notificationMessage);

        // Clear the queue.
        notificationMessageInMemoryQueue.clear();

        // Add the notification message to the queue.
        notificationMessageInMemoryQueue.add(notificationMessage);

        // Validate that the queue is not empty now.
        assertFalse(notificationMessageInMemoryQueue.isEmpty());

        // Call the method under test.
        publishNotificationMessagesAdvice.publishNotificationMessages(joinPoint);

        // Verify the external calls.
        verify(notificationMessageInMemoryQueue, times(2)).clear();
        verify(notificationMessageInMemoryQueue).add(notificationMessage);
        verify(notificationMessageInMemoryQueue, times(3)).isEmpty();
        verify(notificationMessageInMemoryQueue).remove();
        verify(notificationMessagePublishingService).publishNotificationMessage(notificationMessage);
        verify(notificationMessagePublishingService).addNotificationMessageToDatabaseQueue(notificationMessage);
        verify(notificationMessageInMemoryQueue).size();
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertTrue(notificationMessageInMemoryQueue.isEmpty());
    }

    @Test
    public void testPublishNotificationMessagesDatabaseException() throws Throwable
    {
        // Create a notification message.
        NotificationMessage notificationMessage = new NotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), AWS_SQS_QUEUE_NAME, MESSAGE_TEXT,
            Collections.singletonList(new MessageHeader(KEY, VALUE)));

        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint = getMockedProceedingJoinPoint("testPublishNotificationMessages");

        // Mock the external calls.
        doCallRealMethod().when(notificationMessageInMemoryQueue).clear();
        doCallRealMethod().when(notificationMessageInMemoryQueue).add(notificationMessage);
        when(notificationMessageInMemoryQueue.isEmpty()).thenCallRealMethod();
        doCallRealMethod().when(notificationMessageInMemoryQueue).remove();
        doThrow(new AmazonServiceException(ERROR_MESSAGE)).when(notificationMessagePublishingService).publishNotificationMessage(notificationMessage);
        doThrow(new RuntimeException(ERROR_MESSAGE)).when(notificationMessagePublishingService).addNotificationMessageToDatabaseQueue(notificationMessage);

        // Clear the queue.
        notificationMessageInMemoryQueue.clear();

        // Add the notification message to the queue.
        notificationMessageInMemoryQueue.add(notificationMessage);

        // Validate that the queue is not empty now.
        assertFalse(notificationMessageInMemoryQueue.isEmpty());

        // Call the method under test.
        publishNotificationMessagesAdvice.publishNotificationMessages(joinPoint);

        // Verify the external calls.
        verify(notificationMessageInMemoryQueue, times(2)).clear();
        verify(notificationMessageInMemoryQueue).add(notificationMessage);
        verify(notificationMessageInMemoryQueue, times(3)).isEmpty();
        verify(notificationMessageInMemoryQueue).remove();
        verify(notificationMessagePublishingService).publishNotificationMessage(notificationMessage);
        verify(notificationMessagePublishingService).addNotificationMessageToDatabaseQueue(notificationMessage);
        verify(notificationMessageInMemoryQueue).size();
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertTrue(notificationMessageInMemoryQueue.isEmpty());
    }

    @Test
    public void testPublishNotificationMessagesDebugEnabled() throws Throwable
    {
        // Save the current log level.
        LogLevel originalLogLevel = getLogLevel(PublishNotificationMessagesAdvice.class);

        try
        {
            // Set the log level to debug.
            setLogLevel(PublishNotificationMessagesAdvice.class, LogLevel.DEBUG);

            // Create a notification message.
            NotificationMessage notificationMessage = new NotificationMessage(MessageTypeEntity.MessageEventTypes.SQS.name(), AWS_SQS_QUEUE_NAME, MESSAGE_TEXT,
                Collections.singletonList(new MessageHeader(KEY, VALUE)));

            // Mock a join point of the method call.
            ProceedingJoinPoint joinPoint = getMockedProceedingJoinPoint("testPublishNotificationMessages");

            // Mock the external calls.
            doCallRealMethod().when(notificationMessageInMemoryQueue).clear();
            doCallRealMethod().when(notificationMessageInMemoryQueue).add(notificationMessage);
            when(notificationMessageInMemoryQueue.size()).thenCallRealMethod();
            when(notificationMessageInMemoryQueue.isEmpty()).thenCallRealMethod();
            doCallRealMethod().when(notificationMessageInMemoryQueue).remove();

            // Clear the queue.
            notificationMessageInMemoryQueue.clear();

            // Add the notification message to the queue.
            notificationMessageInMemoryQueue.add(notificationMessage);

            // Validate that the queue is not empty now.
            assertFalse(notificationMessageInMemoryQueue.isEmpty());

            // Call the method under test.
            publishNotificationMessagesAdvice.publishNotificationMessages(joinPoint);

            // Verify the external calls.
            verify(notificationMessageInMemoryQueue, times(2)).clear();
            verify(notificationMessageInMemoryQueue).add(notificationMessage);
            verify(notificationMessageInMemoryQueue).size();
            verify(notificationMessageInMemoryQueue, times(3)).isEmpty();
            verify(notificationMessageInMemoryQueue).remove();
            verify(notificationMessagePublishingService).publishNotificationMessage(notificationMessage);
            verifyNoMoreInteractionsHelper();

            // Validate the results.
            assertTrue(notificationMessageInMemoryQueue.isEmpty());
        }
        finally
        {
            // Restore log level to the original value.
            setLogLevel(PublishNotificationMessagesAdvice.class, originalLogLevel);
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

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(notificationMessageInMemoryQueue, notificationMessagePublishingService);
    }
}
