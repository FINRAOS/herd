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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.HerdDao;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.model.jpa.StorageUnitStatusHistoryEntity;

/**
 * A helper class for storage unit related code.
 */
@Component
public class StorageUnitHelper
{
    @Autowired
    protected HerdDao herdDao;

    @Autowired
    protected StorageDaoHelper storageDaoHelper;

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
        StorageUnitStatusEntity storageUnitStatusEntity = storageDaoHelper.getStorageUnitStatusEntity(storageUnitStatus);

        // Update the storage unit status.
        updateStorageUnitStatus(storageUnitEntity, storageUnitStatusEntity, reason);
    }

    /**
     * Update the storage unit status.
     *
     * @param storageUnitEntity the storage unit entity
     * @param storageUnitStatusEntity the new storage unit status entity
     * @param reason the reason for the update
     */
    public void updateStorageUnitStatus(StorageUnitEntity storageUnitEntity, StorageUnitStatusEntity storageUnitStatusEntity, String reason)
    {
        // Update the entity with the new values.
        storageUnitEntity.setStatus(storageUnitStatusEntity);

        // Add an entry to the storage unit status history table.
        StorageUnitStatusHistoryEntity storageUnitStatusHistoryEntity = new StorageUnitStatusHistoryEntity();
        storageUnitEntity.getHistoricalStatuses().add(storageUnitStatusHistoryEntity);
        storageUnitStatusHistoryEntity.setStorageUnit(storageUnitEntity);
        storageUnitStatusHistoryEntity.setStatus(storageUnitStatusEntity);
        storageUnitStatusHistoryEntity.setReason(reason);

        // Persist the entity.
        herdDao.saveAndRefresh(storageUnitEntity);
    }
}
