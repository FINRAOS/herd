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
package org.finra.dm.model.dto;

import org.finra.dm.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.dm.model.jpa.NotificationJobActionEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;

/**
 * A DTO that holds various parameters needed to trigger Business object data notification.
 */
public class BusinessObjectDataNotificationEventParamsDto extends NotificationEventParamsDto
{
    /**
     * The business object data notification registration entity.
     */
    private BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistration;

    /**
     * The notification job action entity.
     */
    private NotificationJobActionEntity notificationJobAction;

    /**
     * The business object data event type.
     */
    private String eventType;

    /**
     * The business object data key.
     */
    private BusinessObjectDataKey businessObjectDataKey;

    /**
     * The storage name.
     */
    private String storageName;

    public BusinessObjectDataNotificationRegistrationEntity getBusinessObjectDataNotificationRegistration()
    {
        return businessObjectDataNotificationRegistration;
    }

    public void setBusinessObjectDataNotificationRegistration(BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistrationEntity)
    {
        this.businessObjectDataNotificationRegistration = businessObjectDataNotificationRegistrationEntity;
    }

    public NotificationJobActionEntity getNotificationJobAction()
    {
        return notificationJobAction;
    }

    public void setNotificationJobAction(NotificationJobActionEntity notificationJobAction)
    {
        this.notificationJobAction = notificationJobAction;
    }

    public String getEventType()
    {
        return eventType;
    }

    public void setEventType(String eventType)
    {
        this.eventType = eventType;
    }

    public BusinessObjectDataKey getBusinessObjectDataKey()
    {
        return businessObjectDataKey;
    }

    public void setBusinessObjectDataKey(BusinessObjectDataKey businessObjectDataKey)
    {
        this.businessObjectDataKey = businessObjectDataKey;
    }

    public String getStorageName()
    {
        return storageName;
    }

    public void setStorageName(String storageName)
    {
        this.storageName = storageName;
    }
}