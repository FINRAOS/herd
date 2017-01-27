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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.support.destination.DestinationResolver;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.dto.ConfigurationValue;

@Component
public class HerdJmsDestinationResolver implements DestinationResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HerdJmsDestinationResolver.class);

    // The queue names that are used to annotate the JMS listener {@link HerdJmsMessageListener}.
    public static final String SQS_DESTINATION_HERD_INCOMING = "herd_incoming_queue";

    public static final String SQS_DESTINATION_STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE = "storage_policy_selector_job_sqs_queue";

    public static final String SQS_DESTINATION_SAMPLE_DATA_QUEUE = "sample_data_queue";

    public static final String SQS_DESTINATION_SEARCH_INDEX_UPDATE_QUEUE = "search_index_update_queue";
    
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Override
    public Destination resolveDestinationName(Session session, String destinationName, boolean pubSubDomain) throws JMSException
    {
        String sqsQueueName;

        switch (destinationName)
        {
            case SQS_DESTINATION_HERD_INCOMING:
                // Get the incoming SQS queue name.
                sqsQueueName = getSqsQueueName(ConfigurationValue.HERD_NOTIFICATION_SQS_INCOMING_QUEUE_NAME);
                break;
            case SQS_DESTINATION_STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE:
                // Get the storage policy selector job SQS queue name.
                sqsQueueName = getSqsQueueName(ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME);
                break;
            case SQS_DESTINATION_SAMPLE_DATA_QUEUE:
                // Get the storage policy selector job SQS queue name.
                sqsQueueName = getSqsQueueName(ConfigurationValue.SAMPLE_DATA_SQS_QUEUE_NAME);
                break;
            case SQS_DESTINATION_SEARCH_INDEX_UPDATE_QUEUE:
                // Get the storage policy selector job SQS queue name.
                sqsQueueName = getSqsQueueName(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME);
                break;
            default:
                LOGGER.warn("Failed to resolve the destination name: \"%s\".", destinationName);
                sqsQueueName = "";
                break;
        }

        Destination destination;

        try
        {
            destination = session.createQueue(sqsQueueName);
        }
        catch (Exception ex)
        {
            throw new IllegalStateException(String.format("Failed to resolve the SQS queue: \"%s\".", sqsQueueName), ex);
        }

        return destination;
    }

    /**
     * Returns the SQS queue name configured in the system per specified configuration value. Throws {@link IllegalStateException} if SQS queue name is
     * undefined.
     *
     * @return the SQS queue name
     */
    private String getSqsQueueName(ConfigurationValue configurationValue)
    {
        String sqsQueueName = configurationHelper.getProperty(configurationValue);

        if (StringUtils.isBlank(sqsQueueName))
        {
            throw new IllegalStateException(
                String.format("SQS queue name not found. Ensure the \"%s\" configuration entry is configured.", configurationValue.getKey()));
        }

        return sqsQueueName;
    }
}
