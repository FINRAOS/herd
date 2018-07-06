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
package org.finra.herd.service;

import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.NamespaceEntity;

public interface MessageNotificationEventService
{
    /**
     * Handles notifications for the business object data status changes.
     *
     * @param businessObjectDataKey the business object data key
     * @param newBusinessObjectDataStatus the new business object data status
     * @param oldBusinessObjectDataStatus the old business object data status
     *
     * @return the list of notification messages that got queued for publishing
     */
    List<NotificationMessage> processBusinessObjectDataStatusChangeNotificationEvent(BusinessObjectDataKey businessObjectDataKey,
        String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus);

    /**
     * Handles notifications for business object definition description suggestion changes.
     *
     * @param businessObjectDefinitionDescriptionSuggestion the business object definition description suggestion
     * @param lastUpdatedByUserId the User ID of the user who last updated this business object definition description suggestion
     * @param lastUpdatedOn the timestamp when this business object definition description suggestion was last updated on
     * @param namespaceEntity the namespace entity
     *
     * @return the list of notification messages that got queued for publishing
     */
    List<NotificationMessage> processBusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion, String lastUpdatedByUserId,
        XMLGregorianCalendar lastUpdatedOn, NamespaceEntity namespaceEntity);

    /**
     * Handles notifications for the business object format version changes.
     *
     * @param businessObjectFormatKey the business object format key
     * @param oldBusinessObjectFormatVersion the old business object format version
     *
     * @return the list of notification messages that got queued for publishing
     */
    List<NotificationMessage> processBusinessObjectFormatVersionChangeNotificationEvent(BusinessObjectFormatKey businessObjectFormatKey,
        String oldBusinessObjectFormatVersion);

    /**
     * Handles notifications for the storage unit status changes.
     *
     * @param businessObjectDataKey the business object data key
     * @param storageName the storage name
     * @param newStorageUnitStatus the new storage unit status
     * @param oldStorageUnitStatus the old storage unit status
     *
     * @return the list of notification messages that got queued for publishing
     */
    List<NotificationMessage> processStorageUnitStatusChangeNotificationEvent(BusinessObjectDataKey businessObjectDataKey, String storageName,
        String newStorageUnitStatus, String oldStorageUnitStatus);

    /**
     * Handles the system monitor event notification.
     *
     * @param systemMonitorRequestPayload the system monitor incoming payload
     *
     * @return the list of notification messages that got queued for publishing
     */
    List<NotificationMessage> processSystemMonitorNotificationEvent(String systemMonitorRequestPayload);
}
