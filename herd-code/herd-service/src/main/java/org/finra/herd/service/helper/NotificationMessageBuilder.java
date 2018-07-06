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

import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.jpa.NamespaceEntity;

/**
 * The builder that builds notification messages.
 */
public interface NotificationMessageBuilder
{
    /**
     * Builds a list of notification messages for the business object data status change event. The result list might be empty if if no messages should be
     * sent.
     *
     * @param businessObjectDataKey the business object data key for the object whose status changed
     * @param newBusinessObjectDataStatus the new business object data status
     * @param oldBusinessObjectDataStatus the old business object data status, may be null
     *
     * @return the list of business object data status change notification messages
     */
    List<NotificationMessage> buildBusinessObjectDataStatusChangeMessages(BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus,
        String oldBusinessObjectDataStatus);

    /**
     * Builds a list of notification messages for the business object definition description suggestion change event. The result list might be empty if if no
     * messages should be sent.
     *
     * @param businessObjectDefinitionDescriptionSuggestion the business object definition description suggestion
     * @param lastUpdatedByUserId the User ID of the user who last updated this business object definition description suggestion
     * @param lastUpdatedOn the timestamp when this business object definition description suggestion was last updated on
     * @param namespaceEntity the namespace entity
     *
     * @return the list of notification messages
     */
    List<NotificationMessage> buildBusinessObjectDefinitionDescriptionSuggestionChangeMessages(
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion, String lastUpdatedByUserId,
        XMLGregorianCalendar lastUpdatedOn, NamespaceEntity namespaceEntity);

    /**
     * Builds a list of notification messages for the business object format version change event. The result list might be empty if if no messages should be
     * sent.
     *
     * @param businessObjectFormatKey the business object format key for the object whose version changed
     * @param oldBusinessObjectFormatVersion the old business object format version
     *
     * @return the list of business object data status change notification messages
     */
    List<NotificationMessage> buildBusinessObjectFormatVersionChangeMessages(BusinessObjectFormatKey businessObjectFormatKey,
        String oldBusinessObjectFormatVersion);

    /**
     * Builds a list of notification messages for the business object data status change event. The result list might be empty if if no messages should be
     * sent.
     *
     * @param businessObjectDataKey the business object data key for the object whose status changed
     * @param storageName the storage name
     * @param newStorageUnitStatus the new storage unit status
     * @param oldStorageUnitStatus the old storage unit status, may be null
     *
     * @return the list of business object data status change notification messages
     */
    List<NotificationMessage> buildStorageUnitStatusChangeMessages(BusinessObjectDataKey businessObjectDataKey, String storageName, String newStorageUnitStatus,
        String oldStorageUnitStatus);

    /**
     * Builds the message for the ESB system monitor response.
     *
     * @param systemMonitorRequestPayload the system monitor request payload
     *
     * @return the outgoing system monitor notification message or null if no message should be sent
     */
    NotificationMessage buildSystemMonitorResponse(String systemMonitorRequestPayload);
}
