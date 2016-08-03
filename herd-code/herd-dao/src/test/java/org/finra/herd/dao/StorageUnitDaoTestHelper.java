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
package org.finra.herd.dao;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

@Component
public class StorageUnitDaoTestHelper
{
    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataDaoTestHelper businessObjectDataDaoTestHelper;

    @Autowired
    private StorageDao storageDao;

    @Autowired
    private StorageDaoTestHelper storageDaoTestHelper;

    @Autowired
    private StorageUnitDao storageUnitDao;

    @Autowired
    private StorageUnitStatusDao storageUnitStatusDao;

    @Autowired
    private StorageUnitStatusDaoTestHelper storageUnitStatusDaoTestHelper;

    /**
     * Create and persist business object data entity in "restoring" state.
     *
     * @param businessObjectDataKey the business object data key
     * @param originStorageName the origin S3 storage name
     * @param originStorageUnitStatus the origin S3 storage unit status
     * @param glacierStorageName the Glacier storage name
     * @param glacierStorageUnitStatus the Glacier storage unit status
     *
     * @return the business object data entity
     */
    public BusinessObjectDataEntity createBusinessObjectDataEntityInRestoringState(BusinessObjectDataKey businessObjectDataKey, String originStorageName,
        String originStorageUnitStatus, String glacierStorageName, String glacierStorageUnitStatus)
    {
        // Create and persist a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectDataKey, AbstractDaoTest.LATEST_VERSION_FLAG_SET, AbstractDaoTest.BDATA_STATUS);

        // Create and persist an origin S3 storage entity, if not exists.
        StorageEntity originStorageEntity = storageDao.getStorageByName(originStorageName);
        if (originStorageEntity == null)
        {
            originStorageEntity = storageDaoTestHelper.createStorageEntity(originStorageName, StoragePlatformEntity.S3);
        }

        // Create and persist a Glacier storage entity, if not exists.
        StorageEntity glacierStorageEntity = storageDao.getStorageByName(glacierStorageName);
        if (glacierStorageEntity == null)
        {
            glacierStorageEntity = storageDaoTestHelper.createStorageEntity(glacierStorageName, StoragePlatformEntity.GLACIER);
        }

        // Create and persist an S3 storage unit entity.
        StorageUnitEntity originStorageUnitEntity = null;
        if (originStorageUnitStatus != null)
        {
            originStorageUnitEntity =
                createStorageUnitEntity(originStorageEntity, businessObjectDataEntity, originStorageUnitStatus, AbstractDaoTest.NO_STORAGE_DIRECTORY_PATH);
        }

        // Create and persist a Glacier storage unit entity.
        if (glacierStorageUnitStatus != null)
        {
            StorageUnitEntity glacierStorageUnitEntity =
                createStorageUnitEntity(glacierStorageEntity, businessObjectDataEntity, glacierStorageUnitStatus, AbstractDaoTest.NO_STORAGE_DIRECTORY_PATH);

            // Set a parent storage unit for the Glacier storage unit.
            glacierStorageUnitEntity.setParentStorageUnit(originStorageUnitEntity);
        }

        // Return the business object data entity.
        return businessObjectDataEntity;
    }

    /**
     * Creates and persists a new storage unit entity.
     *
     * @param storageName the storage name
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataLatestVersion specifies if the business object data is flagged as latest version or not
     * @param businessObjectDataStatusCode the business object data status
     * @param storageUnitStatus the storage unit status
     * @param storageDirectoryPath the storage directory path
     *
     * @return the newly created storage unit entity
     */
    public StorageUnitEntity createStorageUnitEntity(String storageName, BusinessObjectDataKey businessObjectDataKey, Boolean businessObjectDataLatestVersion,
        String businessObjectDataStatusCode, String storageUnitStatus, String storageDirectoryPath)
    {
        return createStorageUnitEntity(storageName, StoragePlatformEntity.S3, businessObjectDataKey, businessObjectDataLatestVersion,
            businessObjectDataStatusCode, storageUnitStatus, storageDirectoryPath);
    }

    /**
     * Creates and persists a new storage unit entity.
     *
     * @param storageName the storage name
     * @param storagePlatform the storage platform
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataLatestVersion specifies if the business object data is flagged as latest version or not
     * @param businessObjectDataStatusCode the business object data status
     * @param storageUnitStatus the storage unit status
     * @param storageDirectoryPath the storage directory path
     *
     * @return the newly created storage unit entity
     */
    public StorageUnitEntity createStorageUnitEntity(String storageName, String storagePlatform, BusinessObjectDataKey businessObjectDataKey,
        Boolean businessObjectDataLatestVersion, String businessObjectDataStatusCode, String storageUnitStatus, String storageDirectoryPath)
    {
        // Create a storage entity, if not exists.
        StorageEntity storageEntity = storageDao.getStorageByName(storageName);
        if (storageEntity == null)
        {
            storageEntity = storageDaoTestHelper.createStorageEntity(storageName, storagePlatform);
        }

        // Create a business object data entity, if not exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey);
        if (businessObjectDataEntity == null)
        {
            businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(businessObjectDataKey, businessObjectDataLatestVersion, businessObjectDataStatusCode);
        }

        // Create and return a storage unit entity.
        return createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatus, storageDirectoryPath);
    }

    /**
     * Creates and persists a new storage unit entity.
     *
     * @param storageName the storage name
     * @param storagePlatform the storage platform
     * @param businessObjectDataEntity the business object data entity
     * @param storageUnitStatus the storage unit status
     * @param storageDirectoryPath the storage directory path
     *
     * @return the newly created storage unit entity
     */
    public StorageUnitEntity createStorageUnitEntity(String storageName, String storagePlatform, BusinessObjectDataEntity businessObjectDataEntity,
        String storageUnitStatus, String storageDirectoryPath)
    {
        // Create a storage entity, if not exists.
        StorageEntity storageEntity = storageDao.getStorageByName(storageName);
        if (storageEntity == null)
        {
            storageEntity = storageDaoTestHelper.createStorageEntity(storageName, storagePlatform);
        }

        // Create and return a storage unit entity.
        return createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatus, storageDirectoryPath);
    }

    /**
     * Creates and persists a new storage unit entity.
     *
     * @param storageEntity the storage entity
     * @param businessObjectDataEntity the business object data entity
     * @param storageUnitStatus the storage unit status
     * @param directoryPath the storage directory path
     *
     * @return the newly created storage unit entity.
     */
    public StorageUnitEntity createStorageUnitEntity(StorageEntity storageEntity, BusinessObjectDataEntity businessObjectDataEntity, String storageUnitStatus,
        String directoryPath)
    {
        // Create a storage entity, if not exists.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDao.getStorageUnitStatusByCode(storageUnitStatus);
        if (storageUnitStatusEntity == null)
        {
            storageUnitStatusEntity = storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(storageUnitStatus);
        }

        return createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatusEntity, directoryPath);
    }

    /**
     * Creates and persists a new storage unit entity.
     *
     * @param storageEntity the storage entity
     * @param businessObjectDataEntity the business object data entity
     * @param storageUnitStatusEntity the storage unit status entity
     * @param directoryPath the storage directory path
     *
     * @return the newly created storage unit entity.
     */
    public StorageUnitEntity createStorageUnitEntity(StorageEntity storageEntity, BusinessObjectDataEntity businessObjectDataEntity,
        StorageUnitStatusEntity storageUnitStatusEntity, String directoryPath)
    {
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setDirectoryPath(directoryPath);
        storageUnitEntity.setStatus(storageUnitStatusEntity);
        return storageUnitDao.saveAndRefresh(storageUnitEntity);
    }

    /**
     * Creates and persists a new storage unit entity.
     *
     * @param storageName the storage name
     * @param namespace the namespace
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the primary partition value
     * @param subPartitionValues the list of sub-partition values
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataLatestVersion specifies if the business object data is flagged as latest version or not
     * @param businessObjectDataStatusCode the business object data status
     * @param storageUnitStatus the storage unit status
     * @param storageDirectoryPath the storage directory path
     *
     * @return the newly created storage unit entity
     */
    public StorageUnitEntity createStorageUnitEntity(String storageName, String namespace, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue,
        List<String> subPartitionValues, Integer businessObjectDataVersion, Boolean businessObjectDataLatestVersion, String businessObjectDataStatusCode,
        String storageUnitStatus, String storageDirectoryPath)
    {
        return createStorageUnitEntity(storageName,
            new BusinessObjectDataKey(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, subPartitionValues, businessObjectDataVersion), businessObjectDataLatestVersion,
            businessObjectDataStatusCode, storageUnitStatus, storageDirectoryPath);
    }
}
