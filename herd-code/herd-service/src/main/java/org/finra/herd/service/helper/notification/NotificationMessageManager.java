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
package org.finra.herd.service.helper.notification;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.dto.BusinessObjectDataPublishedAttributesChangeNotificationEvent;
import org.finra.herd.model.dto.BusinessObjectDataStatusChangeNotificationEvent;
import org.finra.herd.model.dto.BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent;
import org.finra.herd.model.dto.BusinessObjectFormatVersionChangeNotificationEvent;
import org.finra.herd.model.dto.NotificationEvent;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.dto.StorageUnitStatusChangeNotificationEvent;
import org.finra.herd.model.dto.UserNamespaceAuthorizationChangeNotificationEvent;

/**
 * The manager class that knows how to build a list of notification messages for a given event.
 */
@Component
public class NotificationMessageManager
{
    @Autowired
    private BusinessObjectDataPublishedAttributesChangeMessageBuilder businessObjectDataPublishedAttributesChangeMessageBuilder;

    @Autowired
    private BusinessObjectDataStatusChangeMessageBuilder businessObjectDataStatusChangeMessageBuilder;

    @Autowired
    private BusinessObjectDefinitionDescriptionSuggestionChangeMessageBuilder businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder;

    @Autowired
    private BusinessObjectFormatVersionChangeMessageBuilder businessObjectFormatVersionChangeMessageBuilder;

    @Autowired
    private StorageUnitStatusChangeMessageBuilder storageUnitStatusChangeMessageBuilder;

    @Autowired
    private UserNamespaceAuthorizationChangeMessageBuilder userNamespaceAuthorizationChangeMessageBuilder;

    private Map<Class<?>, NotificationMessageBuilder> eventTypeNotificationMessageBuilderMap = new HashMap<>();

    /**
     * Build the event type to message builder map. This method should only be invoked when all the builder spring beans are fully initialized.
     */
    @PostConstruct
    public void populateEventTypeNotificationMessageBuilderMap()
    {
        // Build the event type to notification message builders
        eventTypeNotificationMessageBuilderMap
            .put(BusinessObjectDataPublishedAttributesChangeNotificationEvent.class, businessObjectDataPublishedAttributesChangeMessageBuilder);
        eventTypeNotificationMessageBuilderMap.put(BusinessObjectDataStatusChangeNotificationEvent.class, businessObjectDataStatusChangeMessageBuilder);
        eventTypeNotificationMessageBuilderMap
            .put(BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent.class, businessObjectDefinitionDescriptionSuggestionChangeMessageBuilder);
        eventTypeNotificationMessageBuilderMap.put(BusinessObjectFormatVersionChangeNotificationEvent.class, businessObjectFormatVersionChangeMessageBuilder);
        eventTypeNotificationMessageBuilderMap.put(StorageUnitStatusChangeNotificationEvent.class, storageUnitStatusChangeMessageBuilder);
        eventTypeNotificationMessageBuilderMap.put(UserNamespaceAuthorizationChangeNotificationEvent.class, userNamespaceAuthorizationChangeMessageBuilder);
    }

    /**
     * Get the event type to notification message builder map
     *
     * @return the message builder map
     */
    public Map<Class<?>, NotificationMessageBuilder> getEventTypeNotificationMessageBuilderMap()
    {
        return eventTypeNotificationMessageBuilderMap;
    }

    /**
     * Builds a list of notification messages for the event.
     *
     * @param notificationEvent the notification event
     *
     * @return list of notification message for the event
     */
    public List<NotificationMessage> buildNotificationMessages(NotificationEvent notificationEvent)
    {
        Assert.notNull(notificationEvent, "Parameter \"notificationEvent\" must not be null");

        return eventTypeNotificationMessageBuilderMap.get(notificationEvent.getClass()).buildNotificationMessages(notificationEvent);
    }
}
