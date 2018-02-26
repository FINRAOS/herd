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

import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.CleanupDestroyedBusinessObjectDataService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageUnitHelper;

/**
 * An implementation of the cleanup destroyed business object data service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class CleanupDestroyedBusinessObjectDataServiceImpl implements CleanupDestroyedBusinessObjectDataService
{
    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

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
     * This implementation starts a new transaction.
     */
    @PublishNotificationMessages
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void cleanupS3StorageUnit(BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey)
    {
        cleanupS3StorageUnitImpl(businessObjectDataStorageUnitKey);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation executes non-transactionally, suspends the current transaction if one exists.
     */
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public List<BusinessObjectDataStorageUnitKey> getS3StorageUnitsToCleanup(int maxResult)
    {
        return getS3StorageUnitsToCleanupImpl(maxResult);
    }

    /**
     * All traces of BData are removed from the Herd repository: BData record, Attributes, related Storage Units and Storage Files, Status history, parent-child
     * relationships
     *
     * @param businessObjectDataStorageUnitKey the business object data storage unit key
     */
    void cleanupS3StorageUnitImpl(BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey)
    {
        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.createBusinessObjectDataKeyFromStorageUnitKey(businessObjectDataStorageUnitKey);

        // Retrieve the business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Get the number of S3 storage units associated with this Business Object Data.
        long numberOfS3StorageUnitsAssociatedWithThisBusinessObjectData = businessObjectDataEntity.getStorageUnits().stream()
            .filter(storageUnitEntity -> storageUnitEntity.getStorage().getStoragePlatform().getName().equals(StoragePlatformEntity.S3)).count();

        // Validate that there are not multiple S3 storage units associated with this Business Object Data.
        if (numberOfS3StorageUnitsAssociatedWithThisBusinessObjectData > 1)
        {
            // If there are multiple S3 storage units associated with this Business Object Data then throw a runtime exception.
            throw new IllegalArgumentException(String.format("Business object data has multiple (%s) %s storage units. Business object data: {%s}",
                numberOfS3StorageUnitsAssociatedWithThisBusinessObjectData, StoragePlatformEntity.S3,
                businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        // Get a list of children business object data
        List<BusinessObjectDataEntity> childrenBusinessObjectDataEntities = businessObjectDataEntity.getBusinessObjectDataChildren();

        // For each child in the list of children business object data entities remove this business object data from the list of parents
        for (BusinessObjectDataEntity childBusinessObjectDataEntity : childrenBusinessObjectDataEntities)
        {
            List<BusinessObjectDataEntity> parentBusinessObjectDataEntities = childBusinessObjectDataEntity.getBusinessObjectDataParents();
            parentBusinessObjectDataEntities.remove(businessObjectDataEntity);
            businessObjectDataDao.saveAndRefresh(childBusinessObjectDataEntity);
        }

        // Get a list of parent business object data
        List<BusinessObjectDataEntity> parentBusinessObjectDataEntities = businessObjectDataEntity.getBusinessObjectDataParents();

        // For each child in the list of children business object data entities remove this business object data from the list of parents
        for (BusinessObjectDataEntity parentBusinessObjectDataEntity : parentBusinessObjectDataEntities)
        {
            List<BusinessObjectDataEntity> childBusinessObjectDataEntities = parentBusinessObjectDataEntity.getBusinessObjectDataChildren();
            childBusinessObjectDataEntities.remove(businessObjectDataEntity);
            businessObjectDataDao.saveAndRefresh(parentBusinessObjectDataEntity);
        }

        // Delete this business object data.
        businessObjectDataDao.delete(businessObjectDataEntity);

        // If this business object data version is the latest, set the latest flag on the previous version of this object data, if it exists.
        if (businessObjectDataEntity.getLatestVersion())
        {
            // Get the maximum version for this business object data, if it exists.
            Integer maxBusinessObjectDataVersion = businessObjectDataDao.getBusinessObjectDataMaxVersion(businessObjectDataKey);

            if (maxBusinessObjectDataVersion != null)
            {
                // Retrieve the previous version business object data entity. Since we successfully got the maximum
                // version for this business object data, the retrieved entity is not expected to be null.
                BusinessObjectDataEntity previousVersionBusinessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(
                    new BusinessObjectDataKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                        businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                        businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(),
                        businessObjectDataKey.getSubPartitionValues(), maxBusinessObjectDataVersion));

                // Update the previous version business object data entity.
                previousVersionBusinessObjectDataEntity.setLatestVersion(true);
                businessObjectDataDao.saveAndRefresh(previousVersionBusinessObjectDataEntity);
            }
        }

        // Create a storage unit notification for the storage unit status change event.
        notificationEventService
            .processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG, businessObjectDataKey,
                businessObjectDataStorageUnitKey.getStorageName(), null, StorageUnitStatusEntity.DISABLED);

        // Create a business object data notification for the business object data status change event.
        notificationEventService
            .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG, businessObjectDataKey, null,
                BusinessObjectDataStatusEntity.DELETED);
    }

    /**
     * Retrieves a list of keys for destroyed S3 storage units that are ready for cleanup.
     *
     * @param maxResult the maximum number of results to retrieve
     *
     * @return the list of storage unit keys
     */
    List<BusinessObjectDataStorageUnitKey> getS3StorageUnitsToCleanupImpl(int maxResult)
    {
        // Retrieves a list of storage units that belong to S3 storage, and has a final destroy on timestamp < current time, has a DELETED status,
        // and associated BData has a DELETED status.
        List<StorageUnitEntity> storageUnitEntities = storageUnitDao.getS3StorageUnitsToCleanup(maxResult);

        // Build a list of storage unit keys.
        List<BusinessObjectDataStorageUnitKey> storageUnitKeys = new ArrayList<>();
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            storageUnitKeys.add(storageUnitHelper.createStorageUnitKeyFromEntity(storageUnitEntity));
        }

        return storageUnitKeys;
    }
}
