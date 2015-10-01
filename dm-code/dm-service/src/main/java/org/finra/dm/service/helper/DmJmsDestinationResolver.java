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
package org.finra.dm.service.helper;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.support.destination.DestinationResolver;
import org.springframework.stereotype.Component;

import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.model.dto.ConfigurationValue;

@Component
public class DmJmsDestinationResolver implements DestinationResolver
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    // The queue name that is used to annotate the JMS listener {@link DmJmsMessageListener}. 
    public static final String SQS_DESTINATION_DM_INCOMING = "dm_incoming_queue";

    @Override
    public Destination resolveDestinationName(Session session, String destinationName, boolean pubSubDomain) throws JMSException
    {
        String sqsQueueName = "";
        if (destinationName.equals(SQS_DESTINATION_DM_INCOMING))
        {
            sqsQueueName = getIncomingSqsQueueName();
        }
        
        Destination destination = null;
        try
        {
            destination = session.createQueue(sqsQueueName);
        }
        catch(Exception ex)
        {
            throw new IllegalStateException(String.format("Failed to resolve the SQS queue: \"%s\".", sqsQueueName), ex);
        }
        return destination;
    }
    
    /**
     * Returns the incoming SQS queue name. Throws {@link IllegalStateException} if SQS queue name is undefined.
     *
     * @return the incoming  queue name
     */
    private String getIncomingSqsQueueName()
    {
        String sqsQueueName = configurationHelper.getProperty(ConfigurationValue.DM_NOTIFICATION_SQS_INCOMING_QUEUE_NAME);

        if (StringUtils.isBlank(sqsQueueName))
        {
            throw new IllegalStateException(String.format("SQS queue name not found. Ensure the \"%s\" configuration entry is configured.",
                ConfigurationValue.DM_NOTIFICATION_SQS_INCOMING_QUEUE_NAME.getKey()));
        }

        return sqsQueueName;
    }
}
