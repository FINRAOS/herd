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

import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.StorageUnitNotificationFilter;
import org.finra.herd.model.jpa.StorageUnitNotificationRegistrationEntity;

public interface StorageUnitNotificationRegistrationDao extends BaseJpaDao
{
    /**
     * Retrieves a storage unit notification registration entity by alternate key.
     *
     * @param key the storage unit notification registration key (case-insensitive)
     *
     * @return the storage unit notification registration entity
     */
    public StorageUnitNotificationRegistrationEntity getStorageUnitNotificationRegistrationByAltKey(NotificationRegistrationKey key);

    /**
     * Retrieves a list of storage unit notification registration keys defined for the specified storage unit notification registration namespace.
     *
     * @param namespace the namespace of the storage unit notification registration (case-insensitive)
     *
     * @return the list of storage unit notification registration keys
     */
    public List<NotificationRegistrationKey> getStorageUnitNotificationRegistrationKeysByNamespace(String namespace);

    /**
     * Gets a list of keys for all existing storage unit notification registrations that match the specified storage unit notification filter parameters.
     *
     * @param storageUnitNotificationFilter the filter for the storage unit notification. Only the following four filter parameters are used to match the
     * storage unit notification registrations: <p><ul> <li>the namespace of the business object definition (required, case-insensitive) <li>the name of the
     * business object definition (required, case-insensitive) <li>the usage of the business object format (optional, case-insensitive) <li>the file type of the
     * business object format (optional, case-insensitive) </ul>
     *
     * @return the list of storage unit notification registration keys
     */
    public List<NotificationRegistrationKey> getStorageUnitNotificationRegistrationKeysByNotificationFilter(
        StorageUnitNotificationFilter storageUnitNotificationFilter);
}
