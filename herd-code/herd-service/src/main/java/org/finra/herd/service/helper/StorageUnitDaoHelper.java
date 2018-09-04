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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.model.jpa.StorageUnitStatusHistoryEntity;
import org.finra.herd.service.MessageNotificationEventService;

/**
 * Helper for storage unit related operations which require DAO.
 */
@Component
public class StorageUnitDaoHelper
{
    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private MessageNotificationEventService messageNotificationEventService;

    @Autowired
    private StorageUnitDao storageUnitDao;

    @Autowired
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    /**
     * Retrieves a storage unit entity for the business object data in the specified storage and make sure it exists.
     *
     * @param storageName the storage name
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the storage unit entity
     */
    public StorageUnitEntity getStorageUnitEntity(String storageName, BusinessObjectDataEntity businessObjectDataEntity)
    {
        StorageUnitEntity storageUnitEntity = storageUnitDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, storageName);

        if (storageUnitEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", storageName,
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        return storageUnitEntity;
    }

    /**
     * Retrieves a storage unit entity for the business object data in the specified storage and make sure it exists.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param storageEntity the storage entity
     *
     * @return the storage unit entity
     */
    public StorageUnitEntity getStorageUnitEntityByBusinessObjectDataAndStorage(BusinessObjectDataEntity businessObjectDataEntity, StorageEntity storageEntity)
    {
        StorageUnitEntity storageUnitEntity = storageUnitDao.getStorageUnitByBusinessObjectDataAndStorage(businessObjectDataEntity, storageEntity);

        if (storageUnitEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", storageEntity.getName(),
                    businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        return storageUnitEntity;
    }

    /**
     * Retrieves a storage unit entity for the specified business object data storage unit key and makes sure it exists.
     *
     * @param businessObjectDataStorageUnitKey the business object data storage unit key
     *
     * @return the storage unit entity
     */
    public StorageUnitEntity getStorageUnitEntityByKey(BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey)
    {
        StorageUnitEntity storageUnitEntity = storageUnitDao.getStorageUnitByKey(businessObjectDataStorageUnitKey);

        if (storageUnitEntity == null)
        {
            throw new ObjectNotFoundException(String.format(
                "Business object data storage unit {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", " +
                    "businessObjectDataSubPartitionValues: \"%s\", businessObjectDataVersion: %d, storageName: \"%s\"} doesn't exist.",
                businessObjectDataStorageUnitKey.getNamespace(), businessObjectDataStorageUnitKey.getBusinessObjectDefinitionName(),
                businessObjectDataStorageUnitKey.getBusinessObjectFormatUsage(), businessObjectDataStorageUnitKey.getBusinessObjectFormatFileType(),
                businessObjectDataStorageUnitKey.getBusinessObjectFormatVersion(), businessObjectDataStorageUnitKey.getPartitionValue(),
                CollectionUtils.isEmpty(businessObjectDataStorageUnitKey.getSubPartitionValues()) ? "" :
                    StringUtils.join(businessObjectDataStorageUnitKey.getSubPartitionValues(), ","),
                businessObjectDataStorageUnitKey.getBusinessObjectDataVersion(), businessObjectDataStorageUnitKey.getStorageName()));
        }

        return storageUnitEntity;
    }

    /**
     * Sets storage unit status value for a storage unit. This method also generates a storage unit status change notification event as per system
     * configuration.
     *
     * @param storageUnitEntity the storage unit entity
     * @param storageUnitStatusEntity the storage unit status entity
     */
    public void setStorageUnitStatus(StorageUnitEntity storageUnitEntity, StorageUnitStatusEntity storageUnitStatusEntity)
    {
        // Set the storage unit status value.
        storageUnitEntity.setStatus(storageUnitStatusEntity);

        // Send a storage unit status change notification as per system configuration.
        messageNotificationEventService
            .processStorageUnitStatusChangeNotificationEvent(businessObjectDataHelper.getBusinessObjectDataKey(storageUnitEntity.getBusinessObjectData()),
                storageUnitEntity.getStorage().getName(), storageUnitStatusEntity.getCode(), null);
    }

    /**
     * Update the storage unit status.
     *
     * @param storageUnitEntity the storage unit entity
     * @param storageUnitStatus the new storage unit status
     * @param reason the reason for the update
     */
    public void updateStorageUnitStatus(StorageUnitEntity storageUnitEntity, String storageUnitStatus, String reason)
    {
        // Retrieve and ensure the new storage unit status is valid.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(storageUnitStatus);

        // Update the storage unit status.
        updateStorageUnitStatus(storageUnitEntity, storageUnitStatusEntity, reason);
    }

    /**
     * Updates storage unit status value for a storage unit.  This method also updates the storage unit status history and generates a storage unit status
     * change notification event as per system configuration.
     *
     * @param storageUnitEntity the storage unit entity
     * @param storageUnitStatusEntity the new storage unit status entity
     * @param reason the reason for the update
     */
    public void updateStorageUnitStatus(StorageUnitEntity storageUnitEntity, StorageUnitStatusEntity storageUnitStatusEntity, String reason)
    {
        // Save the current status value.
        String oldStatus = storageUnitEntity.getStatus().getCode();

        // Update the entity with the new values.
        storageUnitEntity.setStatus(storageUnitStatusEntity);

        // Add an entry to the storage unit status history table.
        StorageUnitStatusHistoryEntity storageUnitStatusHistoryEntity = new StorageUnitStatusHistoryEntity();
        storageUnitEntity.getHistoricalStatuses().add(storageUnitStatusHistoryEntity);
        storageUnitStatusHistoryEntity.setStorageUnit(storageUnitEntity);
        storageUnitStatusHistoryEntity.setStatus(storageUnitStatusEntity);
        storageUnitStatusHistoryEntity.setReason(reason);

        // Persist the entity.
        storageUnitDao.saveAndRefresh(storageUnitEntity);

        // Send a storage unit status change notification as per system configuration.
        messageNotificationEventService
            .processStorageUnitStatusChangeNotificationEvent(businessObjectDataHelper.getBusinessObjectDataKey(storageUnitEntity.getBusinessObjectData()),
                storageUnitEntity.getStorage().getName(), storageUnitStatusEntity.getCode(), oldStatus);
    }
}
