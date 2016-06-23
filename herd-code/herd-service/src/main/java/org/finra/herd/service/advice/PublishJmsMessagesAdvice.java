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
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.SqsDao;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.annotation.PublishJmsMessages;
import org.finra.herd.model.dto.JmsMessage;
import org.finra.herd.service.JmsPublishingService;
import org.finra.herd.service.helper.JmsMessageInMemoryQueue;

/**
 * Advice that publishes JMS messages stored in the "in-memory" JMS message queue.
 */
@Component
@Aspect
public class PublishJmsMessagesAdvice extends AbstractServiceAdvice
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PublishJmsMessagesAdvice.class);

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private JmsMessageInMemoryQueue jmsMessageInMemoryQueue;

    @Autowired
    private JmsPublishingService jmsPublishingService;

    @Autowired
    private SqsDao sqsDao;

    /**
     * Publishes all JMS messages stored in the "in-memory" JMS message queue.
     *
     * @param joinPoint the join point
     *
     * @return the return value of the method at the join point
     * @throws Throwable if any errors were encountered
     */
    @Around("serviceMethods()")
    public Object publishJmsMessages(ProceedingJoinPoint joinPoint) throws Throwable
    {
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();

        boolean publishJmsMessages = method.isAnnotationPresent(PublishJmsMessages.class);

        // Proceed to the join point (i.e. call the method and let it return).
        try
        {
            Object returnValue = joinPoint.proceed();

            if (publishJmsMessages)
            {
                if (LOGGER.isDebugEnabled())
                {
                    // Get the target class being called.
                    Class<?> targetClass = joinPoint.getTarget().getClass();

                    LOGGER.debug("Method is initiating JMS message publishing. javaMethod=\"{}.{}\" jmsMessageInMemoryQueueSize={}", targetClass.getName(),
                        methodSignature.getName(), jmsMessageInMemoryQueue.size());
                }

                // Publish all JMS messages stored in the "in-memory" JMS message queue.
                while (!jmsMessageInMemoryQueue.isEmpty())
                {
                    JmsMessage jmsMessage = jmsMessageInMemoryQueue.remove();

                    try
                    {
                        // Send a text message to the specified AWS SQS queue.
                        sqsDao.sendSqsTextMessage(awsHelper.getAwsParamsDto(), jmsMessage.getJmsQueueName(), jmsMessage.getMessageText());
                        LOGGER
                            .info("Published JMS message. jmsQueueName=\"{}\" jmsMessagePayload={}", jmsMessage.getJmsQueueName(), jmsMessage.getMessageText());
                    }
                    catch (Exception sqsException)
                    {
                        LOGGER.warn("Failed to publish message to the JMS queue. jmsQueueName=\"{}\" jmsMessagePayload={}", jmsMessage.getJmsQueueName(),
                            jmsMessage.getMessageText(), sqsException);

                        try
                        {
                            jmsPublishingService.addJmsMessageToDatabaseQueue(jmsMessage.getJmsQueueName(), jmsMessage.getMessageText());
                        }
                        catch (Exception dbException)
                        {
                            LOGGER.error("Failed to add JMS message to the database queue. jmsQueueName=\"{}\" jmsMessagePayload={}",
                                jmsMessage.getJmsQueueName(), jmsMessage.getMessageText(), dbException);
                        }
                    }
                }
            }

            return returnValue;
        }
        finally
        {
            // Removes all of the elements from the queue, since the thread might be reused from the thread pool.
            if (publishJmsMessages)
            {
                jmsMessageInMemoryQueue.clear();
            }
        }
    }
}
