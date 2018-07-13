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
package org.finra.herd.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.MessageNotificationEventService;
import org.finra.herd.service.helper.NotificationMessageBuilder;
import org.finra.herd.service.helper.NotificationMessageInMemoryQueue;

/**
 * The message notification event service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class MessageNotificationEventServiceImpl implements MessageNotificationEventService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageNotificationEventServiceImpl.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private NotificationMessageBuilder notificationMessageBuilder;

    @Autowired
    private NotificationMessageInMemoryQueue notificationMessageInMemoryQueue;

    @Override
    public List<NotificationMessage> processBusinessObjectDataStatusChangeNotificationEvent(BusinessObjectDataKey businessObjectDataKey,
        String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus)
    {
        return processNotificationMessages(notificationMessageBuilder
            .buildBusinessObjectDataStatusChangeMessages(businessObjectDataKey, newBusinessObjectDataStatus, oldBusinessObjectDataStatus));
    }

    @Override
    public List<NotificationMessage> processBusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion, String lastUpdatedByUserId,
        XMLGregorianCalendar lastUpdatedOn, NamespaceEntity namespaceEntity)
    {
        return processNotificationMessages(notificationMessageBuilder
            .buildBusinessObjectDefinitionDescriptionSuggestionChangeMessages(businessObjectDefinitionDescriptionSuggestion, lastUpdatedByUserId, lastUpdatedOn,
                namespaceEntity));
    }

    @Override
    public List<NotificationMessage> processBusinessObjectFormatVersionChangeNotificationEvent(BusinessObjectFormatKey businessObjectFormatKey,
        String oldBusinessObjectFormatVersion)
    {
        return processNotificationMessages(
            notificationMessageBuilder.buildBusinessObjectFormatVersionChangeMessages(businessObjectFormatKey, oldBusinessObjectFormatVersion));
    }

    @Override
    public List<NotificationMessage> processStorageUnitStatusChangeNotificationEvent(BusinessObjectDataKey businessObjectDataKey, String storageName,
        String newStorageUnitStatus, String oldStorageUnitStatus)
    {
        return processNotificationMessages(
            notificationMessageBuilder.buildStorageUnitStatusChangeMessages(businessObjectDataKey, storageName, newStorageUnitStatus, oldStorageUnitStatus));
    }

    @PublishNotificationMessages
    @Override
    public List<NotificationMessage> processSystemMonitorNotificationEvent(String systemMonitorRequestPayload)
    {
        // Build a system monitor response message.
        NotificationMessage notificationMessage = notificationMessageBuilder.buildSystemMonitorResponse(systemMonitorRequestPayload);

        // If message is null, send an empty list of notification messages to be processed.
        return processNotificationMessages(notificationMessage == null ? new ArrayList<>() : Collections.singletonList(notificationMessage));
    }

    /**
     * Processes a message by adding it to the "in-memory" queue for publishing by the advice.
     *
     * @param notificationMessages the list of notification messages, may be empty
     *
     * @return the list of notification messages that got queued for publishing
     */
    private List<NotificationMessage> processNotificationMessages(final List<NotificationMessage> notificationMessages)
    {
        // Create an empty result list.
        List<NotificationMessage> result = new ArrayList<>();

        // Check if message notification is enabled.
        boolean herdSqsNotificationEnabled = configurationHelper.getBooleanProperty(ConfigurationValue.HERD_NOTIFICATION_SQS_ENABLED);

        // Only process messages if the service is enabled.
        if (herdSqsNotificationEnabled)
        {
            // Process the list of notification messages.
            for (NotificationMessage notificationMessage : notificationMessages)
            {
                // Add the message to the "in-memory" queue if a message was configured. Otherwise, log a warning.
                if (StringUtils.isNotBlank(notificationMessage.getMessageText()))
                {
                    notificationMessageInMemoryQueue.add(notificationMessage);
                    result.add(notificationMessage);
                }
                else
                {
                    LOGGER.warn("Not sending notification message because it is not configured. messageType={} messageDestination={}",
                        notificationMessage.getMessageType(), notificationMessage.getMessageDestination());
                }
            }
        }

        return result;
    }
}
