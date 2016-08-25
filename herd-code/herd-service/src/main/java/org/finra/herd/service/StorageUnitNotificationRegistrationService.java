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

import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.StorageUnitNotificationFilter;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistration;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationCreateRequest;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationKeys;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationUpdateRequest;

/**
 * The storage unit notification registration service.
 */
public interface StorageUnitNotificationRegistrationService
{
    /**
     * Creates a new storage unit notification registration.
     *
     * @param request the information needed to create a storage unit notification
     *
     * @return the newly created storage unit notification registration
     */
    public StorageUnitNotificationRegistration createStorageUnitNotificationRegistration(StorageUnitNotificationRegistrationCreateRequest request);

    /**
     * Deletes an existing storage unit notification registration by key.
     *
     * @param notificationRegistrationKey the storage unit notification registration key
     *
     * @return the storage unit notification registration that got deleted
     */
    public StorageUnitNotificationRegistration deleteStorageUnitNotificationRegistration(NotificationRegistrationKey notificationRegistrationKey);

    /**
     * Gets an existing storage unit notification by key.
     *
     * @param notificationRegistrationKey the storage unit notification registration key
     *
     * @return the storage unit notification registration
     */
    public StorageUnitNotificationRegistration getStorageUnitNotificationRegistration(NotificationRegistrationKey notificationRegistrationKey);

    /**
     * Gets a list of keys for all existing storage unit notification registrations for the specified storage unit notification registration namespace.
     *
     * @param namespace the namespace of the storage unit notification registration
     *
     * @return the storage unit notification registration keys
     */
    public StorageUnitNotificationRegistrationKeys getStorageUnitNotificationRegistrationsByNamespace(String namespace);

    /**
     * Gets a list of keys for all existing storage unit notification registrations that match the specified storage unit notification filter.
     *
     * @param storageUnitNotificationFilter the filter for the storage unit notification registration
     *
     * @return the storage unit notification registration keys
     */
    public StorageUnitNotificationRegistrationKeys getStorageUnitNotificationRegistrationsByNotificationFilter(
        StorageUnitNotificationFilter storageUnitNotificationFilter);

    /**
     * Updates an existing storage unit notification by key.
     *
     * @param notificationRegistrationKey the storage unit notification registration key
     *
     * @return the storage unit notification registration that got updated
     */
    public StorageUnitNotificationRegistration updateStorageUnitNotificationRegistration(NotificationRegistrationKey notificationRegistrationKey,
        StorageUnitNotificationRegistrationUpdateRequest request);
}
