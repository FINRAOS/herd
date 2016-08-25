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
import java.util.concurrent.Future;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;

public interface NotificationEventService
{
    /**
     * Asynchronously handles the notification for the business object data changes.
     *
     * @param notificationEventType the notification event type
     * @param businessObjectDataKey the business object data key
     * @param newBusinessObjectDataStatus the new business object data status
     * @param oldBusinessObjectDataStatus the old business object data status
     *
     * @return a future to know the asynchronous state of this method
     */
    public Future<Void> processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata notificationEventType,
        BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus);

    /**
     * Synchronously handles the notification for the business object data changes.
     *
     * @param notificationEventType the notification event type
     * @param businessObjectDataKey the business object data key.
     * @param newBusinessObjectDataStatus the new business object data status
     * @param oldBusinessObjectDataStatus the old business object data status
     *
     * @return a list of actions that were performed. For example: Job for a jobAction
     */
    public List<Object> processBusinessObjectDataNotificationEventSync(NotificationEventTypeEntity.EventTypesBdata notificationEventType,
        BusinessObjectDataKey businessObjectDataKey, String newBusinessObjectDataStatus, String oldBusinessObjectDataStatus);

    /**
     * Asynchronously handles the notification for the storage unit changes.
     *
     * @param notificationEventType the notification event type
     * @param businessObjectDataKey the business object data key
     * @param storageName the name of the storage
     * @param newStorageUnitStatus the new storage unit status
     * @param oldStorageUnitStatus the old storage unit status
     *
     * @return a future to know the asynchronous state of this method
     */
    public Future<Void> processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit notificationEventType,
        BusinessObjectDataKey businessObjectDataKey, String storageName, String newStorageUnitStatus, String oldStorageUnitStatus);

    /**
     * Synchronously handles the notification for the storage unit changes.
     *
     * @param notificationEventType the notification event type
     * @param businessObjectDataKey the business object data key
     * @param storageName the name of the storage
     * @param newStorageUnitStatus the new storage unit status
     * @param oldStorageUnitStatus the old storage unit status
     *
     * @return a list of actions that were performed. For example: Job for a jobAction
     */
    public List<Object> processStorageUnitNotificationEventSync(NotificationEventTypeEntity.EventTypesStorageUnit notificationEventType,
        BusinessObjectDataKey businessObjectDataKey, String storageName, String newStorageUnitStatus, String oldStorageUnitStatus);
}
