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
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;

public interface BusinessObjectDataNotificationRegistrationDao
{
    /**
     * Retrieves a business object data notification registration entity by alternate key.
     *
     * @param key the business object data notification registration key (case-insensitive)
     *
     * @return the business object data notification registration entity
     */
    public BusinessObjectDataNotificationRegistrationEntity getBusinessObjectDataNotificationRegistrationByAltKey(
        NotificationRegistrationKey key);

    /**
     * Retrieves a list of business object data notification registration keys defined for the specified namespace.
     *
     * @param namespaceCode the namespace code (case-insensitive)
     *
     * @return the list of business object data notification registration keys
     */
    public List<NotificationRegistrationKey> getBusinessObjectDataNotificationRegistrationKeys(String namespaceCode);

    /**
     * Retrieves a list of business object data notification entities that match given input parameters. Gets only ENABLED notification registrations.
     *
     * @param notificationEventTypeCode the notification event type code (case-insensitive)
     * @param businessObjectDataKey the business object data key (case-insensitive)
     * @param newBusinessObjectDataStatus the new business object data status (case-insensitive)
     * @param oldBusinessObjectDataStatus the old (previous) business object data status (case-insensitive). This parameter will be null for business object
     *            data registration
     *
     * @return the list of business object data notification entities
     */
    public List<BusinessObjectDataNotificationRegistrationEntity> getBusinessObjectDataNotificationRegistrations(String notificationEventTypeCode,
        BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus);

    /**
     * Retrieves a list of business object data notification entities that match given input parameters.
     *
     * @param notificationEventTypeCode the notification event type code (case-insensitive)
     * @param businessObjectDataKey the business object data key (case-insensitive)
     * @param newBusinessObjectDataStatus the new business object data status (case-insensitive)
     * @param oldBusinessObjectDataStatus the old (previous) business object data status (case-insensitive). This parameter will be null for business object
     *            data registration
     * @param notificationRegistrationStatus The status of the notification registration. Optional. Defaults to any. Case-insensitive.
     *
     * @return the list of business object data notification entities
     */
    public List<BusinessObjectDataNotificationRegistrationEntity> getBusinessObjectDataNotificationRegistrations(String notificationEventTypeCode,
        BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus,
        String notificationRegistrationStatus);
}
