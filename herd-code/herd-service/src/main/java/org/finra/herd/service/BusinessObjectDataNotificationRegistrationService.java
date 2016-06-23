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

import org.finra.herd.model.api.xml.BusinessObjectDataNotificationFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistration;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationKeys;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationUpdateRequest;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;

/**
 * The business object data notification service.
 */
public interface BusinessObjectDataNotificationRegistrationService
{
    /**
     * Creates a new business object data notification.
     *
     * @param request the information needed to create a business object data notification
     *
     * @return the newly created business object data notification
     */
    public BusinessObjectDataNotificationRegistration createBusinessObjectDataNotificationRegistration(
        BusinessObjectDataNotificationRegistrationCreateRequest request);

    /**
     * Gets an existing business object data notification by key.
     *
     * @param key the business object data notification registration key
     *
     * @return the business object data notification information
     */
    public BusinessObjectDataNotificationRegistration getBusinessObjectDataNotificationRegistration(NotificationRegistrationKey key);

    /**
     * Updates an existing business object data notification by key.
     *
     * @param key the business object data notification registration key
     *
     * @return the business object data notification that got updated
     */
    public BusinessObjectDataNotificationRegistration updateBusinessObjectDataNotificationRegistration(NotificationRegistrationKey key,
        BusinessObjectDataNotificationRegistrationUpdateRequest request);

    /**
     * Deletes an existing business object data notification by key.
     *
     * @param key the business object data notification registration key
     *
     * @return the business object data notification that got deleted
     */
    public BusinessObjectDataNotificationRegistration deleteBusinessObjectDataNotificationRegistration(NotificationRegistrationKey key);

    /**
     * Gets a list of keys for all existing business object data notification registrations for the specified business object data notification registration
     * namespace.
     *
     * @param namespace the namespace of the business object data notification registration
     *
     * @return the business object data notification registration keys
     */
    public BusinessObjectDataNotificationRegistrationKeys getBusinessObjectDataNotificationRegistrationsByNamespace(String namespace);

    /**
     * Gets a list of keys for all existing business object data notification registrations that match the specified business object data notification filter.
     *
     * @param businessObjectDataNotificationFilter the filter for the business object data notification
     *
     * @return the business object data notification registration keys
     */
    public BusinessObjectDataNotificationRegistrationKeys getBusinessObjectDataNotificationRegistrationsByNotificationFilter(
        BusinessObjectDataNotificationFilter businessObjectDataNotificationFilter);
}
