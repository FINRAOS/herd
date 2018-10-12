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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.dao.UserNamespaceAuthorizationDao;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.dto.BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent;
import org.finra.herd.model.dto.NotificationEvent;
import org.finra.herd.model.jpa.NamespaceEntity;

/**
 * The builder that knows how to build Business Object Definition Description suggestion Change notification messages
 */
@Component
public class BusinessObjectDefinitionDescriptionSuggestionChangeMessageBuilder extends AbstractNotificationMessageBuilder implements NotificationMessageBuilder
{
    @Autowired
    private UserNamespaceAuthorizationDao userNamespaceAuthorizationDao;

    @Autowired
    private NamespaceDao namespaceDao;

    @Override
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification =
        "The NotificationEvent is cast to a BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent which is always the case since" +
            " we manage the event type to a builder in a map defined in NotificationMessageManager")
    public Map<String, Object> getNotificationMessageVelocityContextMap(NotificationEvent notificationEvent)
    {
        BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent event =
            (BusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent) notificationEvent;

        // Create a list of users (User IDs) that need to be notified about this event.
        // Initialize the list with the user who created this business object definition description suggestion.
        List<String> notificationList = new ArrayList<>();
        notificationList.add(event.getBusinessObjectDefinitionDescriptionSuggestion().getCreatedByUserId());

        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(event.getNamespace());
        Assert.notNull(namespaceEntity, String.format("namespaceEntity must not be null for namespace code \"%s\"", event.getNamespace()));

        // Add to the notification list all users that have WRITE or WRITE_DESCRIPTIVE_CONTENT permission on the namespace.
        notificationList.addAll(userNamespaceAuthorizationDao.getUserIdsWithWriteOrWriteDescriptiveContentPermissionsByNamespace(namespaceEntity));

        // Create JSON and XML escaped copies of the business object definition description suggestion.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestionWithJson =
            escapeJsonBusinessObjectDefinitionDescriptionSuggestion(event.getBusinessObjectDefinitionDescriptionSuggestion());
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestionWithXml =
            escapeXmlBusinessObjectDefinitionDescriptionSuggestion(event.getBusinessObjectDefinitionDescriptionSuggestion());

        // Create JSON and XML escaped notification lists.
        List<String> notificationListWithJson = new ArrayList<>();
        List<String> notificationListWithXml = new ArrayList<>();
        for (String userId : notificationList)
        {
            notificationListWithJson.add(escapeJson(userId));
            notificationListWithXml.add(escapeXml(userId));
        }

        // Create a context map of values that can be used when building the message.
        Map<String, Object> velocityContextMap = new HashMap<>();
        addObjectPropertyToContext(velocityContextMap, "businessObjectDefinitionDescriptionSuggestion",
            event.getBusinessObjectDefinitionDescriptionSuggestion(), businessObjectDefinitionDescriptionSuggestionWithJson,
            businessObjectDefinitionDescriptionSuggestionWithXml);
        addObjectPropertyToContext(velocityContextMap, "businessObjectDefinitionDescriptionSuggestionKey",
            event.getBusinessObjectDefinitionDescriptionSuggestion().getBusinessObjectDefinitionDescriptionSuggestionKey(),
            businessObjectDefinitionDescriptionSuggestionWithJson.getBusinessObjectDefinitionDescriptionSuggestionKey(),
            businessObjectDefinitionDescriptionSuggestionWithXml.getBusinessObjectDefinitionDescriptionSuggestionKey());
        addStringPropertyToContext(velocityContextMap, "lastUpdatedByUserId", event.getLastUpdatedByUserId());
        velocityContextMap.put("lastUpdatedOn", event.getLastUpdatedOn());
        addObjectPropertyToContext(velocityContextMap, "notificationList", notificationList, notificationListWithJson, notificationListWithXml);
        addStringPropertyToContext(velocityContextMap, "namespace", event.getNamespace());

        // Return the Velocity context map.
        return velocityContextMap;
    }

    /**
     * Creates a JSON escaped copy of the specified business object definition description suggestion.
     *
     * @param businessObjectDefinitionDescriptionSuggestion the business object definition description suggestion
     *
     * @return the JSON escaped business object definition description suggestion
     */
    private BusinessObjectDefinitionDescriptionSuggestion escapeJsonBusinessObjectDefinitionDescriptionSuggestion(
        final BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion)
    {
        // Build and return a JSON escaped business object definition description suggestion.
        return new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestion.getId(),
            escapeJsonBusinessObjectDefinitionDescriptionSuggestionKey(
                businessObjectDefinitionDescriptionSuggestion.getBusinessObjectDefinitionDescriptionSuggestionKey()),
            escapeJson(businessObjectDefinitionDescriptionSuggestion.getDescriptionSuggestion()),
            escapeJson(businessObjectDefinitionDescriptionSuggestion.getStatus()),
            escapeJson(businessObjectDefinitionDescriptionSuggestion.getCreatedByUserId()), businessObjectDefinitionDescriptionSuggestion.getCreatedOn());
    }

    /**
     * Creates an XML escaped copy of the specified business object definition description suggestion.
     *
     * @param businessObjectDefinitionDescriptionSuggestion the business object definition description suggestion
     *
     * @return the XML escaped business object definition description suggestion
     */
    private BusinessObjectDefinitionDescriptionSuggestion escapeXmlBusinessObjectDefinitionDescriptionSuggestion(
        final BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion)
    {
        // Build and return an XML escaped business object definition description suggestion.
        return new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestion.getId(),
            escapeXmlBusinessObjectDefinitionDescriptionSuggestionKey(
                businessObjectDefinitionDescriptionSuggestion.getBusinessObjectDefinitionDescriptionSuggestionKey()),
            escapeXml(businessObjectDefinitionDescriptionSuggestion.getDescriptionSuggestion()),
            escapeXml(businessObjectDefinitionDescriptionSuggestion.getStatus()), escapeXml(businessObjectDefinitionDescriptionSuggestion.getCreatedByUserId()),
            businessObjectDefinitionDescriptionSuggestion.getCreatedOn());
    }

    /**
     * Creates an XML escaped copy of the specified business object definition description suggestion key.
     *
     * @param businessObjectDefinitionDescriptionSuggestionKey the business object definition description suggestion key
     *
     * @return the XML escaped business object definition description suggestion key
     */
    private BusinessObjectDefinitionDescriptionSuggestionKey escapeXmlBusinessObjectDefinitionDescriptionSuggestionKey(
        final BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey)
    {
        // Build and return an XML escaped business object definition description suggestion key.
        return new BusinessObjectDefinitionDescriptionSuggestionKey(escapeXml(businessObjectDefinitionDescriptionSuggestionKey.getNamespace()),
            escapeXml(businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName()),
            escapeXml(businessObjectDefinitionDescriptionSuggestionKey.getUserId()));
    }

    /**
     * Creates a JSON escaped copy of the specified business object definition description suggestion key.
     *
     * @param businessObjectDefinitionDescriptionSuggestionKey the business object definition description suggestion key
     *
     * @return the JSON escaped business object definition description suggestion key
     */
    private BusinessObjectDefinitionDescriptionSuggestionKey escapeJsonBusinessObjectDefinitionDescriptionSuggestionKey(
        final BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey)
    {
        // Build and return an JSON escaped business object definition description suggestion key.
        return new BusinessObjectDefinitionDescriptionSuggestionKey(escapeJson(businessObjectDefinitionDescriptionSuggestionKey.getNamespace()),
            escapeJson(businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName()),
            escapeJson(businessObjectDefinitionDescriptionSuggestionKey.getUserId()));
    }


}
