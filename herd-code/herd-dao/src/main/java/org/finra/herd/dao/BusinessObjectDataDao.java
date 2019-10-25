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
import java.util.Map;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.dto.StoragePolicyPriorityLevel;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;

public interface BusinessObjectDataDao extends BaseJpaDao
{
    /**
     * Retrieves business object data by it's key. If a format version isn't specified, the latest available format version (for this partition value) will be
     * used. If a business object data version isn't specified, the latest data version is returned regardless of the business object data status.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the business object data
     */
    BusinessObjectDataEntity getBusinessObjectDataByAltKey(BusinessObjectDataKey businessObjectDataKey);

    /**
     * Retrieves business object data by it's key. If a format version isn't specified, the latest available format version (for this partition value) will be
     * used. If a business object data version isn't specified, the latest data version based on the specified business object data status is returned. When
     * both business object data version and business object data status both are not specified, the latest data version for each set of partition values will
     * be used regardless of the status.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataStatusEntity the optional business object data status entity. This parameter is ignored when the business object data version is
     * specified
     *
     * @return the business object data
     */
    BusinessObjectDataEntity getBusinessObjectDataByAltKeyAndStatus(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity);

    /**
     * Gets a maximum available version of the specified business object data.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the maximum available version of the specified business object data
     */
    Integer getBusinessObjectDataMaxVersion(BusinessObjectDataKey businessObjectDataKey);

    /**
     * Retrieves a maximum available partition value per specified parameters.
     *
     * @param partitionColumnPosition the partition column position (1-based numbering)
     * @param businessObjectFormatKey the business object format key (case-insensitive). If a business object format version isn't specified, business object
     * data partition value is selected across all business object format versions
     * @param businessObjectDataVersion the optional business object data version. If business object data version isn't specified, business object data
     * partition value is selected from all available business object data as per specified business object data status entity
     * @param businessObjectDataStatusEntity the optional business object data status entity. This parameter is ignored when the business object data version is
     * specified
     * @param storageEntities the optional list of storage entities
     * @param storagePlatformEntity the optional storage platform entity, e.g. S3 for Hive DDL. It is ignored when the list of storage entities is not empty
     * @param excludedStoragePlatformEntity the optional storage platform entity to be excluded from search. It is ignored when the list of storage entities is
     * not empty or the storage platform entity is specified
     * @param upperBoundPartitionValue the optional inclusive upper bound for the maximum available partition value
     * @param lowerBoundPartitionValue the optional inclusive lower bound for the maximum available partition value
     *
     * @return the maximum available partition value
     */
    String getBusinessObjectDataMaxPartitionValue(int partitionColumnPosition, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, BusinessObjectDataStatusEntity businessObjectDataStatusEntity, List<StorageEntity> storageEntities,
        StoragePlatformEntity storagePlatformEntity, StoragePlatformEntity excludedStoragePlatformEntity, String upperBoundPartitionValue,
        String lowerBoundPartitionValue);

    /**
     * Retrieves a minimum available partition value per specified parameters.
     *
     * @param partitionColumnPosition the partition column position (1-based numbering)
     * @param businessObjectFormatKey the business object format key (case-insensitive). If a business object format version isn't specified, business object
     * data partition value is selected across all business object format versions
     * @param businessObjectDataVersion the optional business object data version. If business object data version isn't specified, business object data
     * partition value is selected from all available business object data as per specified business object data status entity
     * @param businessObjectDataStatusEntity the optional business object data status entity. This parameter is ignored when the business object data version is
     * specified
     * @param storageEntities the optional list of storage entities
     * @param storagePlatformEntity the optional storage platform entity, e.g. S3 for Hive DDL. It is ignored when the list of storage entities is not empty
     * @param excludedStoragePlatformEntity the optional storage platform entity to be excluded from search. It is ignored when the list of storage entities is
     * not empty or the storage platform entity is specified
     *
     * @return the maximum available partition value
     */
    String getBusinessObjectDataMinPartitionValue(int partitionColumnPosition, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, BusinessObjectDataStatusEntity businessObjectDataStatusEntity, List<StorageEntity> storageEntities,
        StoragePlatformEntity storagePlatformEntity, StoragePlatformEntity excludedStoragePlatformEntity);

    /**
     * Returns a number of business object data instances registered with this business object format.
     *
     * @param businessObjectFormatKey the business object format key
     *
     * @return the number of business object data instances registered with this business object format
     */
    Long getBusinessObjectDataCount(BusinessObjectFormatKey businessObjectFormatKey);

    /**
     * Retrieves business object data versions that match the specified business object data key with potentially missing business object format and/or data
     * version values.
     *
     * @param businessObjectDataKey the business object data key with potentially missing business object format and/or data version values
     *
     * @return the business object data
     */
    List<BusinessObjectDataEntity> getBusinessObjectDataEntities(BusinessObjectDataKey businessObjectDataKey);

    /**
     * Selects business object data having storage files associated with the specified storage and status. Only tbe business object data records that are older
     * than threshold minutes will be selected.
     *
     * @param storageEntity the storage entity
     * @param thresholdMinutes the expiration time in minutes
     * @param businessObjectDataStatuses the list of business object data statuses
     *
     * @return the list of business object data entities sorted by created on timestamp
     */
    List<BusinessObjectDataEntity> getBusinessObjectDataFromStorageOlderThan(StorageEntity storageEntity, int thresholdMinutes,
        List<String> businessObjectDataStatuses);

    /**
     * Retrieves a map of business object data entities to their corresponding storage policy entities, where the business object data status is supported by
     * the storage policy feature and the business object data alternate key values match storage policy's filter and transition (not taking into account
     * storage policy rules). The storage policy priority level identifies a particular storage policy priority that will be selected by the query. The returned
     * map is ordered by the business object data "created on" timestamp, starting with the oldest business object data entity.
     *
     * @param storagePolicyPriorityLevel the storage policy priority level
     * @param supportedBusinessObjectDataStatuses the list of business object data statuses that storage policies apply to (case-sensitive)
     * @param storagePolicyTransitionMaxAllowedAttempts the maximum number of failed storage policy transition attempts before the relative storage unit gets
     * excluded from being selected. 0 means the maximum is not set
     * @param startPosition the position of the first result, numbered from 0
     * @param maxResult the maximum number of results to retrieve
     *
     * @return the map of business object data entities to their corresponding storage policy entities
     */
    Map<BusinessObjectDataEntity, StoragePolicyEntity> getBusinessObjectDataEntitiesMatchingStoragePolicies(
        StoragePolicyPriorityLevel storagePolicyPriorityLevel, List<String> supportedBusinessObjectDataStatuses, int storagePolicyTransitionMaxAllowedAttempts,
        int startPosition, int maxResult);

    /**
     * Retrieves a list of business object data by their partition value.
     *
     * @param partitionValue The partition value
     *
     * @return A list of business object data entity
     */
    List<BusinessObjectDataEntity> getBusinessObjectDataEntitiesByPartitionValue(String partitionValue);

    /**
     * Gets a list of keys for business object data registered under specified business object definition entity. The results are sorted by primary partition
     * value descending, sub-partition values (if present) descending, and business object data version descending.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param maxResults the optional maximum number of results to return
     *
     * @return the list of business object data keys
     */
    List<BusinessObjectDataKey> getBusinessObjectDataByBusinessObjectDefinition(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        Integer maxResults);

    /**
     * Gets a list of keys for business object data registered under specified business object format entity.
     *
     * @param businessObjectFormatEntity the business object format entity
     * @param maxResults optional maximum number of results to return
     *
     * @return the list of business object data keys
     */
    List<BusinessObjectDataKey> getBusinessObjectDataByBusinessObjectFormat(BusinessObjectFormatEntity businessObjectFormatEntity, Integer maxResults);

    /**
     * Retrieves business object data record count up to to the specified record limit as per business object data search key.
     *
     * @param businessObjectDataSearchKey the business object data search key
     * @param recordCountLimit the business object data record count limit
     *
     * @return the record count of business object data up to to the specified limit
     */
    Integer getBusinessObjectDataLimitedCountBySearchKey(BusinessObjectDataSearchKey businessObjectDataSearchKey, Integer recordCountLimit);

    /**
     * Retrieves a list of business object data per specified business object data search key.
     *
     * @param businessObjectDataSearchKey the business object data search key
     * @param pageNum if pageNum parameter is specified, results contain the appropriate page specified. Page numbers are one-based - that is the first page
     * number is one
     * @param pageSize if pageSize parameter is specified, results contain that number of business object data (unless it is the end of the result set)
     *
     * @return the list of business object data
     */
    List<BusinessObjectData> searchBusinessObjectData(BusinessObjectDataSearchKey businessObjectDataSearchKey, Integer pageNum, Integer pageSize);
}
