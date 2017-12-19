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
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.ExpireRestoredBusinessObjectDataHelperService;
import org.finra.herd.service.ExpireRestoredBusinessObjectDataService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.StorageUnitHelper;

/**
 * An implementation of the business object data finalize restore service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class ExpireRestoredBusinessObjectDataServiceImpl implements ExpireRestoredBusinessObjectDataService
{
    @Autowired
    private ExpireRestoredBusinessObjectDataHelperService expireRestoredBusinessObjectDataHelperService;

    /**
     * The @Lazy annotation below is added to address the following BeanCreationException: - Error creating bean with name 'notificationEventServiceImpl': Bean
     * with name 'notificationEventServiceImpl' has been injected into other beans [...] in its raw version as part of a circular reference, but has eventually
     * been wrapped. This means that said other beans do not use the final version of the bean. This is often the result of over-eager type matching - consider
     * using 'getBeanNamesOfType' with the 'allowEagerInit' flag turned off, for example.
     */
    @Autowired
    @Lazy
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
    public void expireS3StorageUnit(BusinessObjectDataStorageUnitKey storageUnitKey)
    {
        expireS3StorageUnitImpl(storageUnitKey);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<BusinessObjectDataStorageUnitKey> getS3StorageUnitsToExpire(int maxResult)
    {
        return getS3StorageUnitsToExpireImpl(maxResult);
    }

    /**
     * Expires a restored S3 storage unit.
     *
     * @param storageUnitKey the storage unit key
     */
    protected void expireS3StorageUnitImpl(BusinessObjectDataStorageUnitKey storageUnitKey)
    {
        // Build the business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto = expireRestoredBusinessObjectDataHelperService.prepareToExpireStorageUnit(storageUnitKey);

        // Create storage unit notification for the storage unit.
        notificationEventService.processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
            businessObjectDataRestoreDto.getBusinessObjectDataKey(), businessObjectDataRestoreDto.getStorageName(),
            businessObjectDataRestoreDto.getNewStorageUnitStatus(), businessObjectDataRestoreDto.getOldStorageUnitStatus());

        // Execute the S3 specific steps required to expire business object data.
        expireRestoredBusinessObjectDataHelperService.executeS3SpecificSteps(businessObjectDataRestoreDto);

        // Execute the after step.
        expireRestoredBusinessObjectDataHelperService.completeStorageUnitExpiration(businessObjectDataRestoreDto);

        // Create storage unit notification for the storage unit.
        notificationEventService.processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
            businessObjectDataRestoreDto.getBusinessObjectDataKey(), businessObjectDataRestoreDto.getStorageName(),
            businessObjectDataRestoreDto.getNewStorageUnitStatus(), businessObjectDataRestoreDto.getOldStorageUnitStatus());
    }

    /**
     * Retrieves a list of keys for restored S3 storage units that are ready to be expired.
     *
     * @param maxResult the maximum number of results to retrieve
     *
     * @return the list of storage unit keys
     */
    protected List<BusinessObjectDataStorageUnitKey> getS3StorageUnitsToExpireImpl(int maxResult)
    {
        // Retrieves a list of storage units that belong to S3 storage, have RESTORED status, and ready to be expired.
        List<StorageUnitEntity> storageUnitEntities = storageUnitDao.getS3StorageUnitsToExpire(maxResult);

        // Build a list of storage unit keys.
        List<BusinessObjectDataStorageUnitKey> storageUnitKeys = new ArrayList<>();
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            storageUnitKeys.add(storageUnitHelper.createStorageUnitKeyFromEntity(storageUnitEntity));
        }

        return storageUnitKeys;
    }
}
