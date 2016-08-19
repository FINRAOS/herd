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
package org.finra.herd.dao;

import java.util.List;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationFilter;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;

public interface BusinessObjectDataNotificationRegistrationDao extends BaseJpaDao
{
    /**
     * Retrieves a business object data notification registration entity by alternate key.
     *
     * @param key the business object data notification registration key (case-insensitive)
     *
     * @return the business object data notification registration entity
     */
    public BusinessObjectDataNotificationRegistrationEntity getBusinessObjectDataNotificationRegistrationByAltKey(NotificationRegistrationKey key);

    /**
     * Retrieves a list of business object data notification registration keys defined for the specified business object data notification registration
     * namespace.
     *
     * @param namespace the namespace of the business object data notification registration (case-insensitive)
     *
     * @return the list of business object data notification registration keys
     */
    public List<NotificationRegistrationKey> getBusinessObjectDataNotificationRegistrationKeysByNamespace(String namespace);

    /**
     * Gets a list of keys for all existing business object data notification registrations that match the specified business object data notification filter
     * parameters.
     *
     * @param businessObjectDataNotificationFilter the filter for the business object data notification. Only the following four filter parameters are used to
     * match the business object data notification registrations: <p><ul> <li>the namespace of the business object definition (required, case-insensitive)
     * <li>the name of the business object definition (required, case-insensitive) <li>the usage of the business object format (optional, case-insensitive)
     * <li>the file type of the business object format (optional, case-insensitive) </ul>
     *
     * @return the list of business object data notification registration keys
     */
    public List<NotificationRegistrationKey> getBusinessObjectDataNotificationRegistrationKeysByNotificationFilter(
        BusinessObjectDataNotificationFilter businessObjectDataNotificationFilter);

    /**
     * Retrieves a list of business object data notification registration entities that match given input parameters.
     *
     * @param notificationEventTypeCode the notification event type code (case-insensitive)
     * @param businessObjectDataKey the business object data key (case-insensitive)
     * @param newBusinessObjectDataStatus the new business object data status (case-insensitive)
     * @param oldBusinessObjectDataStatus the old (previous) business object data status (case-insensitive). This parameter will be null for business object
     * data registration
     * @param notificationRegistrationStatus the status of the notification registration (case-insensitive)
     *
     * @return the list of business object data notification entities
     */
    public List<BusinessObjectDataNotificationRegistrationEntity> getBusinessObjectDataNotificationRegistrations(String notificationEventTypeCode,
        BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus,
        String notificationRegistrationStatus);
}
