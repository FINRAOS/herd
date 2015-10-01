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
package org.finra.dm.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.jpa.JmsMessageEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.service.SqsNotificationEventService;
import org.finra.dm.service.helper.DmDaoHelper;
import org.finra.dm.service.helper.SqsMessageBuilder;

/**
 * The SQS notification event service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class SqsNotificationEventServiceImpl implements SqsNotificationEventService
{
    private static final Logger LOGGER = Logger.getLogger(SqsNotificationEventServiceImpl.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private DmDaoHelper dmDaoHelper;

    @Autowired
    private SqsMessageBuilder sqsMessageBuilder;

    @Override
    public JmsMessageEntity processBusinessObjectDataStatusChangeNotificationEvent(BusinessObjectDataKey businessObjectDataKey,
        String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus)
    {
        return processMessage(
            sqsMessageBuilder.buildBusinessObjectDataStatusChangeMessage(businessObjectDataKey, newBusinessObjectDataStatus, oldBusinessObjectDataStatus),
            "business object data status change");
    }

    @Override
    public JmsMessageEntity processSystemMonitorNotificationEvent(String systemMonitorRequestPayload)
    {
        return processMessage(sqsMessageBuilder.buildSystemMonitorResponse(systemMonitorRequestPayload), "system monitor response");
    }

    /**
     * Processes a message by adding it to the database "queue" table to ultimately be placed on the real queue by a separate job.
     *
     * @param messageText the message text to place on the queue.
     * @param messageName the message name. This is a description of the message that is being processed.
     *
     * @return the JMS message entity that got saved.
     */
    private JmsMessageEntity processMessage(String messageText, String messageName)
    {
        JmsMessageEntity jmsMessageEntity = null;

        // Only process messages if the service is enabled.
        if (isDmSqsNotificationEnabled())
        {
            // Add the message to the database queue if a message was configured. Otherwise, log a warning.
            if (messageText == null)
            {
                LOGGER.warn("Not sending \"" + messageName + "\" message because it is not configured.");
            }
            else
            {
                jmsMessageEntity = dmDaoHelper.addJmsMessageToDatabaseQueue(getSqsQueueName(), messageText);
            }
        }

        return jmsMessageEntity;
    }

    /**
     * Returns the SQS queue name. Throws {@link IllegalStateException} if SQS queue name is undefined.
     *
     * @return the sqs queue name
     */
    private String getSqsQueueName()
    {
        String sqsQueueName = configurationHelper.getProperty(ConfigurationValue.DM_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME);

        if (StringUtils.isBlank(sqsQueueName))
        {
            throw new IllegalStateException(String.format("SQS queue name not found. Ensure the \"%s\" configuration entry is configured.",
                ConfigurationValue.DM_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME.getKey()));
        }

        return sqsQueueName;
    }

    /**
     * Checks if DM SQS notification is enabled.
     *
     * @return true if DM SQS notification is enabled, false otherwise
     */
    private boolean isDmSqsNotificationEnabled()
    {
        return Boolean.valueOf(configurationHelper.getProperty(ConfigurationValue.DM_NOTIFICATION_SQS_ENABLED));
    }
}
