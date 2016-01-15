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
package org.finra.herd.model.dto;

import java.util.List;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.herd.model.jpa.NotificationJobActionEntity;

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
     * The business object data.
     */
    private BusinessObjectData businessObjectData;

    /**
     * The list of partition column names limited by the number of primary and sub-partition values specified in the business object data key.
     */
    private List<String> partitionColumnNames;

    /**
     * The list of primary and sub-partition values as specified in the business object data key.
     */
    private List<String> partitionValues;

    /**
     * The storage name.
     */
    private String storageName;

    /**
     * The new business object data status.
     */
    private String newBusinessObjectDataStatus;

    /**
     * The old business object data status.
     */
    private String oldBusinessObjectDataStatus;

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

    public BusinessObjectData getBusinessObjectData()
    {
        return businessObjectData;
    }

    public void setBusinessObjectData(BusinessObjectData businessObjectData)
    {
        this.businessObjectData = businessObjectData;
    }

    public List<String> getPartitionColumnNames()
    {
        return partitionColumnNames;
    }

    public void setPartitionColumnNames(List<String> partitionColumnNames)
    {
        this.partitionColumnNames = partitionColumnNames;
    }

    public List<String> getPartitionValues()
    {
        return partitionValues;
    }

    public void setPartitionValues(List<String> partitionValues)
    {
        this.partitionValues = partitionValues;
    }

    public String getStorageName()
    {
        return storageName;
    }

    public void setStorageName(String storageName)
    {
        this.storageName = storageName;
    }

    public String getNewBusinessObjectDataStatus()
    {
        return newBusinessObjectDataStatus;
    }

    public void setNewBusinessObjectDataStatus(String newBusinessObjectDataStatus)
    {
        this.newBusinessObjectDataStatus = newBusinessObjectDataStatus;
    }

    public String getOldBusinessObjectDataStatus()
    {
        return oldBusinessObjectDataStatus;
    }

    public void setOldBusinessObjectDataStatus(String oldBusinessObjectDataStatus)
    {
        this.oldBusinessObjectDataStatus = oldBusinessObjectDataStatus;
    }
}