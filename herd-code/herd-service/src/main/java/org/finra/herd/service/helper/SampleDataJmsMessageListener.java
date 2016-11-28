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

import java.io.IOException;
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
import org.springframework.util.Assert;

import org.finra.herd.core.ApplicationContextHolder;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.BusinessObjectDefinitionService;

/**
 * sample data jms message listener
 */
@Component
public class SampleDataJmsMessageListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleDataJmsMessageListener.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private BusinessObjectDefinitionService businessObjectDefinitionService;

    /**
     * Periodically check the configuration and apply the action to the storage policy processor JMS message listener service, if needed.
     */
    @Scheduled(fixedDelay = 60000)
    public void controlSampleDataJmsMessageListener()
    {
        try
        {
            // Get the configuration setting.
            Boolean jmsMessageListenerEnabled =
                Boolean.valueOf(configurationHelper.getProperty(ConfigurationValue.SAMPLE_DATA_JMS_LISTENER_ENABLED));

            // Get the registry bean.
            JmsListenerEndpointRegistry registry = ApplicationContextHolder.getApplicationContext()
                .getBean("org.springframework.jms.config.internalJmsListenerEndpointRegistry", JmsListenerEndpointRegistry.class);

            // Get the sample data JMS message listener container.
            MessageListenerContainer jmsMessageListenerContainer =
                registry.getListenerContainer(HerdJmsDestinationResolver.SQS_DESTINATION_SAMPLE_DATA_QUEUE);

            // Get the current JMS message listener status and the configuration value.
            LOGGER.debug("controlStoragePolicyProcessorJmsMessageListener(): {}={} jmsMessageListenerContainer.isRunning()={}",
                ConfigurationValue.SAMPLE_DATA_JMS_LISTENER_ENABLED.getKey(), jmsMessageListenerEnabled, jmsMessageListenerContainer.isRunning());

            // Apply the relative action if needed.
            if (!jmsMessageListenerEnabled && jmsMessageListenerContainer.isRunning())
            {
                LOGGER.info("controlSampleDataJmsMessageListener(): Stopping the sample data JMS message listener ...");
                jmsMessageListenerContainer.stop();
                LOGGER.info("controlSampleDataJmsMessageListener(): Done");
            }
            else if (jmsMessageListenerEnabled && !jmsMessageListenerContainer.isRunning())
            {
                LOGGER.info("controlSampleDataJmsMessageListener(): Starting the sample data JMS message listener ...");
                jmsMessageListenerContainer.start();
                LOGGER.info("controlSampleDataJmsMessageListener(): Done");
            }
        }
        catch (Exception e)
        {
            LOGGER.error("controlSampleDataJmsMessageListener(): Failed to control the sample data Jms message listener service.", e);
        }
    }

    /**
     * Processes a JMS message.
     *
     * @param payload the message payload
     * @param allHeaders the JMS headers
     */
    @JmsListener(id = HerdJmsDestinationResolver.SQS_DESTINATION_SAMPLE_DATA_QUEUE,
        containerFactory = "jmsListenerContainerFactory",
        destination = HerdJmsDestinationResolver.SQS_DESTINATION_SAMPLE_DATA_QUEUE)
    public void processMessage(String payload, @Headers Map<Object, Object> allHeaders)
    {
        LOGGER.info("Message received from the JMS queue. jmsQueueName=\"{}\" jmsMessageHeaders=\"{}\" jmsMessagePayload={}",
            HerdJmsDestinationResolver.SQS_DESTINATION_SAMPLE_DATA_QUEUE, allHeaders, payload);

        try
        {
            // Process messages coming from S3 bucket.
            S3EventNotification s3EventNotification = S3EventNotification.parseJson(payload);
            String objectKey = URLDecoder.decode(s3EventNotification.getRecords().get(0).getS3().getObject().getKey(), CharEncoding.UTF_8);
            //convert the S3 string back to normal format
            objectKey = converts3KeyFormat(objectKey);
            long fileSize = s3EventNotification.getRecords().get(0).getS3().getObject().getSizeAsLong();
            //parse the objectKey, it should be in the format of namespace/businessObjectDefinitionName/fileName
            String[] objectKeyArrays = objectKey.split("/");
            Assert.isTrue(objectKeyArrays.length == 3, String.format("S3 notification message %s is not in expected format", objectKey));
            
            String namespace = objectKeyArrays[0];
            String businessObjectDefinitionName = objectKeyArrays[1];
            String fileName = objectKeyArrays[2];
            BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(namespace, businessObjectDefinitionName);            
            businessObjectDefinitionService.updatedBusinessObjectDefinitionEntitySampleFile(businessObjectDefinitionKey, fileName, fileSize);
        }
        catch (RuntimeException | IOException e)
        {
            LOGGER.error("Failed to process message from the JMS queue. jmsQueueName=\"{}\" jmsMessagePayload={}",
                HerdJmsDestinationResolver.SQS_DESTINATION_SAMPLE_DATA_QUEUE, payload, e);
        }
    }
    
    /**
     * Converts the specified string from the S3 key format. This implies converting dashes to underscores.
     *
     * @param string string in S3 format
     *
     * @return the regular string
     */
    private String converts3KeyFormat(String string)
    {
        return string.replace('-', '_');
    }
}
