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

import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;

public interface StorageUnitDao extends BaseJpaDao
{
    /**
     * Retrieves a list of storage units that belong to storage of the specified storage platform type and to business object format of the specified file type.
     * Only storage units that belong to the latest versions of the relative business object format and business object data are selected.
     *
     * @param storagePlatform the storage platform
     * @param businessObjectFormatFileType the business object format file type
     *
     * @return the list of storage units
     */
    List<StorageUnitEntity> getLatestVersionStorageUnitsByStoragePlatformAndFileType(String storagePlatform, String businessObjectFormatFileType);

    /**
     * Retrieves a list of storage units that belong to S3 storage, and has a final destroy on timestamp < current time, has a DISABLED status, and associated
     * BData has a DELETED status. The returned list is ordered by the "finalDestroyOn" timestamp of the S3 storage units, starting with an S3 storage unit that
     * is final destroy on the longest.
     *
     * @param maxResult the maximum number of results to retrieve
     *
     * @return the list of storage unit entities
     */
    List<StorageUnitEntity> getS3StorageUnitsToCleanup(int maxResult);

    /**
     * Retrieves a list of storage units that belong to S3 storage, have RESTORED status, and ready to be expired. The returned list is ordered by the
     * "restoreExpirationOn" timestamp of the S3 storage units, starting with an S3 storage unit that is ready to be expired the longest.
     *
     * @param maxResult the maximum number of results to retrieve
     *
     * @return the list of storage unit entities
     */
    List<StorageUnitEntity> getS3StorageUnitsToExpire(int maxResult);

    /**
     * Retrieves a list of storage units that belong to S3 storage and have the relative S3 storage unit in RESTORING state. The returned list is ordered by the
     * "updated on" timestamp of the S3 storage units, starting with an S3 storage unit that has the RESTORING status the longest.
     *
     * @param maxResult the maximum number of results to retrieve
     *
     * @return the list of storage unit entities
     */
    List<StorageUnitEntity> getS3StorageUnitsToRestore(int maxResult);

    /**
     * Gets a storage unit identified by the given business object data entity and storage entity. Returns {@code null} if storage unit entity does not exist.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param storageEntity the storage entity
     *
     * @return {@link StorageUnitEntity} or {@code null}
     */
    StorageUnitEntity getStorageUnitByBusinessObjectDataAndStorage(BusinessObjectDataEntity businessObjectDataEntity, StorageEntity storageEntity);

    /**
     * Gets a storage unit identified by the given business object data entity and storage name. Returns {@code null} if storage unit entity does not exist.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param storageName the storage name (case-insensitive)
     *
     * @return {@link StorageUnitEntity} or {@code null}
     */
    StorageUnitEntity getStorageUnitByBusinessObjectDataAndStorageName(BusinessObjectDataEntity businessObjectDataEntity, String storageName);

    /**
     * Gets a storage unit identified by the given business object data storage unit key. Returns {@code null} if storage unit does not exist.
     *
     * @param businessObjectDataStorageUnitKey the business object data storage unit key
     *
     * @return {@link StorageUnitEntity} or {@code null}
     */
    StorageUnitEntity getStorageUnitByKey(BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey);

    /**
     * Returns a first discovered storage unit in the specified storage that overlaps with the directory path.
     *
     * @param storageEntity the storage entity
     * @param directoryPath the directory path
     *
     * @return the first found storage unit in the specified storage that overlaps with the directory path or null if none were found
     */
    StorageUnitEntity getStorageUnitByStorageAndDirectoryPath(StorageEntity storageEntity, String directoryPath);

    /**
     * Retrieves a list of storage unit entities per specified parameters.
     *
     * @param businessObjectFormatKey the business object format key (case-insensitive). If a business object format version isn't specified, the latest
     * available format version for each partition value will be used.
     * @param partitionFilters the list of partition filter to be used to select business object data instances. Each partition filter contains a list of
     * primary and sub-partition values in the right order up to the maximum partition levels allowed by business object data registration - with partition
     * values for the relative partitions not to be used for selection passed as nulls.
     * @param businessObjectDataVersion the business object data version. If a business object data version isn't specified, the latest data version based on
     * the specified business object data status is returned.
     * @param businessObjectDataStatus the business object data status. This parameter is ignored when the business object data version is specified. When
     * business object data version and business object data status both are not specified, the latest data version for each set of partition values will be
     * used regardless of the status.
     * @param storageNames the optional list of storage names where the business object data storage units should be looked for (case-insensitive)
     * @param storagePlatformType the optional storage platform type, e.g. S3 for Hive DDL. It is ignored when the list of storages is not empty
     * @param excludedStoragePlatformType the optional storage platform type to be excluded from search. It is ignored when the list of storages is not empty or
     * the storage platform type is specified
     * @param selectOnlyAvailableStorageUnits specifies if only available storage units will be selected or any storage units regardless of their status
     *
     * @return the list of storage unit entities sorted by partition values and storage names
     */
    List<StorageUnitEntity> getStorageUnitsByPartitionFiltersAndStorages(BusinessObjectFormatKey businessObjectFormatKey, List<List<String>> partitionFilters,
        Integer businessObjectDataVersion, String businessObjectDataStatus, List<String> storageNames, String storagePlatformType,
        String excludedStoragePlatformType, boolean selectOnlyAvailableStorageUnits);

    /**
     * Retrieves a list of storage units that belong to the specified storage for the specified business object data.
     *
     * @param storageEntity the storage entity
     * @param businessObjectDataEntities the list of business object data entities
     *
     * @return the list of storage unit entities
     */
    List<StorageUnitEntity> getStorageUnitsByStorageAndBusinessObjectData(StorageEntity storageEntity,
        List<BusinessObjectDataEntity> businessObjectDataEntities);

    /**
     * Retrieves a list of storage units that belong to storage of the specified storage platform for the specified business object data.
     *
     * @param storagePlatform the storage platform
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the list of storage unit entities
     */
    List<StorageUnitEntity> getStorageUnitsByStoragePlatformAndBusinessObjectData(String storagePlatform, BusinessObjectDataEntity businessObjectDataEntity);
}
