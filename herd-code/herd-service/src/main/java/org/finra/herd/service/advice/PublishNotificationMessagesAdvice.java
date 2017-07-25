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

import java.lang.reflect.Method;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.service.NotificationMessagePublishingService;
import org.finra.herd.service.helper.NotificationMessageInMemoryQueue;

/**
 * Advice that publishes notification messages stored in the "in-memory" notification message queue.
 */
@Component
@Aspect
public class PublishNotificationMessagesAdvice extends AbstractServiceAdvice
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PublishNotificationMessagesAdvice.class);

    @Autowired
    private NotificationMessageInMemoryQueue notificationMessageInMemoryQueue;

    @Autowired
    private NotificationMessagePublishingService notificationMessagePublishingService;

    /**
     * Publishes all notification messages stored in the "in-memory" notification message queue.
     *
     * @param joinPoint the join point
     *
     * @throws Throwable if any errors were encountered
     */
    @AfterReturning("serviceMethods()")
    public void publishNotificationMessages(ProceedingJoinPoint joinPoint) throws Throwable
    {
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();

        boolean publishNotificationMessages = method.isAnnotationPresent(PublishNotificationMessages.class);

        try
        {
            if (publishNotificationMessages)
            {
                if (LOGGER.isDebugEnabled())
                {
                    // Get the target class being called.
                    Class<?> targetClass = joinPoint.getTarget().getClass();

                    LOGGER.debug("Method is initiating notification message publishing. javaMethod=\"{}.{}\" notificationMessageInMemoryQueueSize={}",
                        targetClass.getName(), methodSignature.getName(), notificationMessageInMemoryQueue.size());
                }

                // Publish all notification messages stored in the "in-memory" notification message queue.
                while (!notificationMessageInMemoryQueue.isEmpty())
                {
                    // Get notification message from the "in-memory" queue.
                    NotificationMessage notificationMessage = notificationMessageInMemoryQueue.remove();

                    // Publish the message.
                    try
                    {
                        notificationMessagePublishingService.publishNotificationMessage(notificationMessage);
                    }
                    catch (Exception sqsException)
                    {
                        // On error, add this notification message to the database queue.
                        try
                        {
                            notificationMessagePublishingService.addNotificationMessageToDatabaseQueue(notificationMessage);
                        }
                        catch (Exception dbException)
                        {
                            // Log the error.
                            LOGGER
                                .error("Failed to add notification message to the database queue. messageType=\"{}\" messageDestination=\"{}\" messageText={}",
                                    notificationMessage.getMessageType(), notificationMessage.getMessageDestination(), notificationMessage.getMessageText(),
                                    dbException);
                        }
                    }
                }
            }
        }
        finally
        {
            // Removes all of the elements from the queue, since the thread might be reused from the thread pool.
            if (publishNotificationMessages)
            {
                notificationMessageInMemoryQueue.clear();
            }
        }
    }
}
