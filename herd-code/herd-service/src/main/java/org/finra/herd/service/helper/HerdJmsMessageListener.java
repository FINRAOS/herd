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
package org.finra.herd.service.helper;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import com.amazonaws.services.s3.event.S3EventNotification;
import org.apache.commons.lang3.CharEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.listener.MessageListenerContainer;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import org.finra.herd.core.ApplicationContextHolder;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.MessageNotificationEventService;
import org.finra.herd.service.UploadDownloadService;
import org.finra.herd.service.impl.UploadDownloadServiceImpl.CompleteUploadSingleMessageResult;

/*
 * herd JMS message listener.
 */
@Component
public class HerdJmsMessageListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HerdJmsMessageListener.class);

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private MessageNotificationEventService sqsNotificationEventService;

    @Autowired
    private UploadDownloadService uploadDownloadService;

    @Autowired
    private ConfigurationHelper configurationHelper;


    /**
     * Periodically check the configuration and apply the action to the herd JMS message listener service, if needed.
     */
    @Scheduled(fixedDelay = 60000)
    public void controlHerdJmsMessageListener()
    {
        try
        {
            // Get the configuration setting.
            Boolean jmsMessageListenerEnabled = Boolean.valueOf(configurationHelper.getProperty(ConfigurationValue.JMS_LISTENER_ENABLED));

            // Get the registry bean.
            JmsListenerEndpointRegistry registry = ApplicationContextHolder.getApplicationContext()
                .getBean("org.springframework.jms.config.internalJmsListenerEndpointRegistry", JmsListenerEndpointRegistry.class);

            // Get the herd JMS message listener container.
            MessageListenerContainer jmsMessageListenerContainer = registry.getListenerContainer(HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING);

            // Get the current JMS message listener status and the configuration value.
            LOGGER.debug("controlHerdJmsMessageListener(): {}={} jmsMessageListenerContainer.isRunning()={}", ConfigurationValue.JMS_LISTENER_ENABLED.getKey(),
                jmsMessageListenerEnabled, jmsMessageListenerContainer.isRunning());

            // Apply the relative action if needed.
            if (!jmsMessageListenerEnabled && jmsMessageListenerContainer.isRunning())
            {
                LOGGER.info("controlHerdJmsMessageListener(): Stopping the herd JMS message listener ...");
                jmsMessageListenerContainer.stop();
                LOGGER.info("controlHerdJmsMessageListener(): Done");
            }
            else if (jmsMessageListenerEnabled && !jmsMessageListenerContainer.isRunning())
            {
                LOGGER.info("controlHerdJmsMessageListener(): Starting the herd JMS message listener ...");
                jmsMessageListenerContainer.start();
                LOGGER.info("controlHerdJmsMessageListener(): Done");
            }
        }
        catch (Exception e)
        {
            LOGGER.error("controlHerdJmsMessageListener(): Failed to control the herd Jms message listener service.", e);
        }
    }

    /**
     * Processes a JMS message.
     *
     * @param payload the message payload.
     * @param allHeaders the JMS headers.
     */
    @JmsListener(id = HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING,
        containerFactory = "jmsListenerContainerFactory", destination = HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING)
    public void processMessage(String payload, @Headers Map<Object, Object> allHeaders)
    {
        LOGGER.info("JMS message received from the queue. jmsQueueName=\"{}\" jmsMessageHeaders=\"{}\" jmsMessagePayload={}",
            HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING, allHeaders, payload);

        // Process the message as S3 notification.
        boolean messageProcessed = processS3Notification(payload);

        // If message is not processed as S3 notification, then process it as ESB system monitor message.
        if (!messageProcessed)
        {
            messageProcessed = processEsbSystemMonitorMessage(payload);
        }

        if (!messageProcessed)
        {
            // The message was not processed, log the error.
            LOGGER.error("Failed to process message from the JMS queue. jmsQueueName=\"{}\" jmsMessagePayload={}",
                HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING, payload);
        }
    }

    /**
     * Process the message as system monitor.
     *
     * @param payload the JMS message payload.
     *
     * @return boolean whether message was processed.
     */
    private boolean processEsbSystemMonitorMessage(String payload)
    {
        boolean messageProcessed = false;

        try
        {
            sqsNotificationEventService.processSystemMonitorNotificationEvent(payload);
            messageProcessed = true;
        }
        catch (Exception e)
        {
            // The logging is set to DEBUG level, since the method is expected to fail when message is not of the expected type.
            LOGGER.debug("Failed to process message from the JMS queue for a system monitor request. jmsQueueName=\"{}\" jmsMessagePayload={}",
                HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING, payload, e);
        }

        return messageProcessed;
    }

    /**
     * Process the message as S3 notification.
     *
     * @param payload the JMS message payload.
     *
     * @return boolean whether message was processed.
     */
    private boolean processS3Notification(String payload)
    {
        boolean messageProcessed = false;

        try
        {
            // Process messages coming from S3 bucket.
            S3EventNotification s3EventNotification = S3EventNotification.parseJson(payload);
            String objectKey = URLDecoder.decode(s3EventNotification.getRecords().get(0).getS3().getObject().getKey(), CharEncoding.UTF_8);

            // Perform the complete upload single file.
            CompleteUploadSingleMessageResult completeUploadSingleMessageResult = uploadDownloadService.performCompleteUploadSingleMessage(objectKey);

            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("completeUploadSingleMessageResult={}", jsonHelper.objectToJson(completeUploadSingleMessageResult));
            }

            messageProcessed = true;
        }
        catch (RuntimeException | UnsupportedEncodingException e)
        {
            // The logging is set to DEBUG level, since the method is expected to fail when message is not of the expected type.
            LOGGER.debug("Failed to process message from the JMS queue for an S3 notification. jmsQueueName=\"{}\" jmsMessagePayload={}",
                HerdJmsDestinationResolver.SQS_DESTINATION_HERD_INCOMING, payload, e);
        }

        return messageProcessed;
    }
}
