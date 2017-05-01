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
package org.finra.herd.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;
import org.finra.herd.model.dto.StorageUnitAlternateKeyDto;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.BusinessObjectDataFinalizeRestoreHelperService;
import org.finra.herd.service.BusinessObjectDataFinalizeRestoreService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.StorageUnitHelper;

/**
 * An implementation of the business object data finalize restore service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDataFinalizeRestoreServiceImpl implements BusinessObjectDataFinalizeRestoreService
{
    @Autowired
    private BusinessObjectDataFinalizeRestoreHelperService businessObjectDataFinalizeRestoreHelperService;

    @Autowired
    private NotificationEventService notificationEventService;

    @Autowired
    private StorageUnitDao storageUnitDao;

    @Autowired
    private StorageUnitHelper storageUnitHelper;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation executes non-transactionally, suspends the current transaction if one exists.
     */
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void finalizeRestore(StorageUnitAlternateKeyDto storageUnitKey)
    {
        finalizeRestoreImpl(storageUnitKey);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<StorageUnitAlternateKeyDto> getS3StorageUnitsToRestore(int maxResult)
    {
        return getS3StorageUnitsToRestoreImpl(maxResult);
    }

    /**
     * Finalizes restore of an S3 storage unit.
     *
     * @param storageUnitKey the storage unit key
     */
    protected void finalizeRestoreImpl(StorageUnitAlternateKeyDto storageUnitKey)
    {
        // Build the business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto = businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(storageUnitKey);

        // Execute the S3 specific steps required to finalize the business object data restore.
        businessObjectDataFinalizeRestoreHelperService.executeS3SpecificSteps(businessObjectDataRestoreDto);

        // Execute the after step regardless if the above step failed or not. Please note that the after step returns true on success and false otherwise.
        businessObjectDataFinalizeRestoreHelperService.completeFinalizeRestore(businessObjectDataRestoreDto);

        // Create storage unit notification for the origin storage unit.
        notificationEventService.processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
            businessObjectDataRestoreDto.getBusinessObjectDataKey(), businessObjectDataRestoreDto.getStorageName(),
            businessObjectDataRestoreDto.getNewStorageUnitStatus(), businessObjectDataRestoreDto.getOldStorageUnitStatus());
    }

    /**
     * Retrieves a list of keys for S3 storage units that are currently being restored.
     *
     * @param maxResult the maximum number of results to retrieve
     *
     * @return the list of storage unit keys
     */
    protected List<StorageUnitAlternateKeyDto> getS3StorageUnitsToRestoreImpl(int maxResult)
    {
        // Retrieves a list of storage units that belong to S3 storage and have the relative S3 storage unit in RESTORING state.
        List<StorageUnitEntity> storageUnitEntities = storageUnitDao.getS3StorageUnitsToRestore(maxResult);

        // Build a list of storage unit keys.
        List<StorageUnitAlternateKeyDto> storageUnitKeys = new ArrayList<>();
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            storageUnitKeys.add(storageUnitHelper.createStorageUnitKeyFromEntity(storageUnitEntity));
        }

        return storageUnitKeys;
    }
}
