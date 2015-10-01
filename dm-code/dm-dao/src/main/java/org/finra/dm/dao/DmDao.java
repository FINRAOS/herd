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
package org.finra.dm.dao;

import java.util.List;

import org.finra.dm.model.dto.DateRangeDto;
import org.finra.dm.model.dto.StorageAlternateKeyDto;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.ConfigurationEntity;
import org.finra.dm.model.jpa.CustomDdlEntity;
import org.finra.dm.model.jpa.DataProviderEntity;
import org.finra.dm.model.jpa.EmrClusterDefinitionEntity;
import org.finra.dm.model.jpa.ExpectedPartitionValueEntity;
import org.finra.dm.model.jpa.FileTypeEntity;
import org.finra.dm.model.jpa.JmsMessageEntity;
import org.finra.dm.model.jpa.JobDefinitionEntity;
import org.finra.dm.model.jpa.NamespaceEntity;
import org.finra.dm.model.jpa.NotificationEventTypeEntity;
import org.finra.dm.model.jpa.OnDemandPriceEntity;
import org.finra.dm.model.jpa.PartitionKeyGroupEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.jpa.StorageFileEntity;
import org.finra.dm.model.jpa.StoragePlatformEntity;
import org.finra.dm.model.jpa.StorageUnitEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationKey;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.dm.model.api.xml.BusinessObjectFormatKey;
import org.finra.dm.model.api.xml.CustomDdlKey;
import org.finra.dm.model.api.xml.EmrClusterDefinitionKey;
import org.finra.dm.model.api.xml.ExpectedPartitionValueKey;
import org.finra.dm.model.api.xml.FileTypeKey;
import org.finra.dm.model.api.xml.NamespaceKey;
import org.finra.dm.model.api.xml.PartitionKeyGroupKey;
import org.finra.dm.model.api.xml.PartitionValueRange;
import org.finra.dm.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStats;
import org.finra.dm.model.api.xml.StorageDailyUploadStats;
import org.finra.dm.model.api.xml.StorageKey;

/**
 * The data management DAO.
 */
// TODO: This class is too big and should be split up into smaller classes (e.g. NamespaceDao, StorageDao, etc.). When this is fixed,
// we can remove the PMD suppress warning below.
@SuppressWarnings("PMD.ExcessivePublicCount")
public interface DmDao extends BaseJpaDao
{
    /**
     * A default date mask for a single day formatted as yyyy-MM-dd.
     */
    public static final String DEFAULT_SINGLE_DAY_DATE_MASK = "yyyy-MM-dd";

    // Configuration

    /**
     * Gets a configuration by it's key.
     *
     * @param key the configuration key (case-insensitive)
     *
     * @return the configuration value for the specified key
     */
    public ConfigurationEntity getConfigurationByKey(String key);

    // Namespace

    /**
     * Gets a namespace by it's key.
     *
     * @param namespaceKey the namespace key (case-insensitive)
     *
     * @return the namespace entity for the specified key
     */
    public NamespaceEntity getNamespaceByKey(NamespaceKey namespaceKey);

    /**
     * Gets a namespace by it's code.
     *
     * @param namespaceCode the namespace code (case-insensitive)
     *
     * @return the namespace entity for the specified code
     */
    public NamespaceEntity getNamespaceByCd(String namespaceCode);

    /**
     * Gets a list of namespace keys for all namespaces defined in the system.
     *
     * @return the list of namespace keys
     */
    public List<NamespaceKey> getNamespaces();

    // DataProvider

    /**
     * Gets a data provider by it's name.
     *
     * @param name the data provider name (case-insensitive)
     *
     * @return the data provider for the specified name
     */
    public DataProviderEntity getDataProviderByName(String name);

    // BusinessObjectDefinition

    /**
     * Gets a business object definition by key.
     *
     * @param businessObjectDefinitionKey the business object definition key (case-insensitive)
     *
     * @return the business object definition for the specified key
     */
    public BusinessObjectDefinitionEntity getBusinessObjectDefinitionByKey(BusinessObjectDefinitionKey businessObjectDefinitionKey);

    /**
     * Gets a legacy business object definition by it's name.
     *
     * @param businessObjectDefinitionName the business object definition name (case-insensitive)
     *
     * @return the legacy business object definition for the specified name
     */
    public BusinessObjectDefinitionEntity getLegacyBusinessObjectDefinitionByName(String businessObjectDefinitionName);

    /**
     * Gets all business object definition keys.
     *
     * @return the list of all business object definition keys
     */
    public List<BusinessObjectDefinitionKey> getBusinessObjectDefinitions();

    /**
     * Gets a list of all business object definition keys for a specified namespace, or, if, namespace is not specified, for all namespaces in the system.
     *
     * @param namespaceCode the optional namespace code (case-insensitive)
     *
     * @return the list of all business object definition keys
     */
    public List<BusinessObjectDefinitionKey> getBusinessObjectDefinitions(String namespaceCode);

    // FileType

    /**
     * Gets a file type by it's code.
     *
     * @param code the file type code (case-insensitive)
     *
     * @return the file type for the specified code
     */
    public FileTypeEntity getFileTypeByCode(String code);

    /**
     * Gets a list of file type keys for all file types defined in the system.
     *
     * @return the list of file type keys
     */
    public List<FileTypeKey> getFileTypes();

    // BusinessObjectFormat

    /**
     * Gets a business object format based on it's key. If a format version isn't specified, the latest available format version will be used.
     *
     * @param businessObjectFormatKey the business object format key (case-insensitive)
     *
     * @return the business object format
     */
    public BusinessObjectFormatEntity getBusinessObjectFormatByAltKey(BusinessObjectFormatKey businessObjectFormatKey);

    /**
     * Gets the maximum available version of the specified business object format.
     *
     * @param businessObjectFormatKey the business object format key (case-insensitive)
     *
     * @return the maximum available version of the specified business object format
     */
    public Integer getBusinessObjectFormatMaxVersion(BusinessObjectFormatKey businessObjectFormatKey);

    /**
     * Returns a number of business object format instances that reference a specified partition key group.
     *
     * @param partitionKeyGroupEntity the partition key group entity
     *
     * @return the number of business object format instances that reference this partition key group
     */
    public Long getBusinessObjectFormatCount(PartitionKeyGroupEntity partitionKeyGroupEntity);

    /**
     * Gets a list of business object format keys for the specified business object definition key.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param latestBusinessObjectFormatVersion specifies if only the latest (maximum) versions of the relative business object formats are returned
     *
     * @return the list of business object format keys
     */
    public List<BusinessObjectFormatKey> getBusinessObjectFormats(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        boolean latestBusinessObjectFormatVersion);

    // PartitionKeyGroup

    /**
     * Gets a partition key group entity.
     *
     * @param partitionKeyGroupKey the partition key group key (case-insensitive)
     *
     * @return the partition key group entity
     */
    public PartitionKeyGroupEntity getPartitionKeyGroupByKey(PartitionKeyGroupKey partitionKeyGroupKey);

    /**
     * Gets a partition key group entity.
     *
     * @param partitionKeyGroupName the name of the partition key group (case-insensitive)
     *
     * @return the partition key group entity
     */
    public PartitionKeyGroupEntity getPartitionKeyGroupByName(String partitionKeyGroupName);

    /**
     * Gets a list of all existing partition key groups.
     *
     * @return the list of partition key group keys
     */
    public List<PartitionKeyGroupKey> getPartitionKeyGroups();

    // ExpectedPartitionValue

    /**
     * Gets an expected partition value entity by partition key group name, expected partition value, and an optional offset.
     *
     * @param expectedPartitionValueKey the expected partition value key (case-insensitive)
     * @param offset the optional offset
     *
     * @return the expected partition value
     */
    public ExpectedPartitionValueEntity getExpectedPartitionValue(ExpectedPartitionValueKey expectedPartitionValueKey, int offset);

    /**
     * Gets a list of expected partition values by group.
     *
     * @param partitionKeyGroupName the partition key group name (case-insensitive)
     * @param partitionValueRange the optional partition value range
     *
     * @return the list of expected partition values
     */
    public List<ExpectedPartitionValueEntity> getExpectedPartitionValuesByGroupAndRange(String partitionKeyGroupName, PartitionValueRange partitionValueRange);

    // CustomDdl

    /**
     * Gets a custom DDL based on the key.
     *
     * @param customDdlKey the custom DDL key
     *
     * @return the custom DDL
     */
    public CustomDdlEntity getCustomDdlByKey(CustomDdlKey customDdlKey);

    /**
     * Gets the custom DDLs defined for the specified business object format.
     *
     * @param businessObjectFormatKey the business object format key
     *
     * @return the list of custom DDL keys
     */
    public List<CustomDdlKey> getCustomDdls(BusinessObjectFormatKey businessObjectFormatKey);

    // BusinessObjectDataStatus

    /**
     * Gets a business object data status by it's code.
     *
     * @param code the business object data status code (case-insensitive)
     *
     * @return the business object data status for the specified code
     */
    public BusinessObjectDataStatusEntity getBusinessObjectDataStatusByCode(String code);

    // BusinessObjectData

    /**
     * Retrieves business object data by it's key. If a format version isn't specified, the latest available format version (for this partition value) will be
     * used. If a business object data version isn't specified, the latest data version is returned regardless of the business object data status.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the business object data
     */
    public BusinessObjectDataEntity getBusinessObjectDataByAltKey(BusinessObjectDataKey businessObjectDataKey);

    /**
     * Retrieves business object data by it's key. If a format version isn't specified, the latest available format version (for this partition value) will be
     * used. If a business object data version isn't specified, the latest data version based on the specified business object data status is returned. When
     * both business object data version and business object data status both are not specified, the latest data version for each set of partition values will
     * be used regardless of the status.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataStatus the business object data status. This parameter is ignored when the business object data version is specified.
     *
     * @return the business object data
     */
    public BusinessObjectDataEntity getBusinessObjectDataByAltKeyAndStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus);

    /**
     * Gets a maximum available version of the specified business object data.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the maximum available version of the specified business object data
     */
    public Integer getBusinessObjectDataMaxVersion(BusinessObjectDataKey businessObjectDataKey);

    /**
     * Retrieves a maximum available partition value per specified parameters.
     *
     * @param partitionColumnPosition the partition column position (1-based numbering)
     * @param businessObjectFormatKey the business object format key (case-insensitive). If a business object format version isn't specified, the latest
     * available format version for each partition value will be used.
     * @param businessObjectDataVersion the business object data version. If a business object data version isn't specified, the latest data version for each
     * partition value will be used.
     * @param storageName the name of the storage where the business object data storage unit is located (case-insensitive)
     * @param upperBoundPartitionValue the optional inclusive upper bound for the maximum available partition value
     * @param lowerBoundPartitionValue the optional inclusive lower bound for the maximum available partition value
     *
     * @return the maximum available partition value
     */
    public String getBusinessObjectDataMaxPartitionValue(int partitionColumnPosition, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, String storageName, String upperBoundPartitionValue, String lowerBoundPartitionValue);

    /**
     * Retrieves a minimum available partition value per specified parameters.
     *
     * @param partitionColumnPosition the partition column position (1-based numbering)
     * @param businessObjectFormatKey the business object format key (case-insensitive). If a business object format version isn't specified, the latest
     * available format version for each partition value will be used.
     * @param businessObjectDataVersion the business object data version. If a business object data version isn't specified, the latest data version for each
     * partition value will be used.
     * @param storageName the name of the storage where the business object data storage unit is located (case-insensitive)
     *
     * @return the maximum available partition value
     */
    public String getBusinessObjectDataMinPartitionValue(int partitionColumnPosition, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, String storageName);

    /**
     * Returns a number of business object data instances registered with this business object format.
     *
     * @param businessObjectFormatKey the business object format key
     *
     * @return the number of business object data instances registered with this business object format
     */
    public Long getBusinessObjectDataCount(BusinessObjectFormatKey businessObjectFormatKey);

    /**
     * Retrieves business object data versions that match the specified business object data key with potentially missing business object format and/or data
     * version values.
     *
     * @param businessObjectDataKey the business object data key with potentially missing business object format and/or data version values
     *
     * @return the business object data
     */
    public List<BusinessObjectDataEntity> getBusinessObjectDataEntities(BusinessObjectDataKey businessObjectDataKey);

    /**
     * Retrieves a list of business object data entities per specified parameters.
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
     * @param storageName the name of the storage where the business object data storage unit is located (case-insensitive)
     *
     * @return the list of business object data entities sorted by partition values
     */
    public List<BusinessObjectDataEntity> getBusinessObjectDataEntities(BusinessObjectFormatKey businessObjectFormatKey, List<List<String>> partitionFilters,
        Integer businessObjectDataVersion, String businessObjectDataStatus, String storageName);

    /**
     * Selects business object data having storage files associated with the specified storage and with status not listed as ignored. Only tbe business object
     * data records that are older than threshold minutes will be selected.
     *
     * @param storageName the storage name
     * @param thresholdMinutes the expiration time in minutes
     * @param businessObjectDataStatusesToIgnore the list of business object data statuses to ignore
     *
     * @return the list of business object data entities sorted by created on
     */
    public List<BusinessObjectDataEntity> getBusinessObjectDataFromStorageOlderThan(String storageName, int thresholdMinutes,
        List<String> businessObjectDataStatusesToIgnore);

    // StoragePlatform

    /**
     * Gets a storage platform by it's name.
     *
     * @param name the storage platform name (case-insensitive)
     *
     * @return the storage platform for the specified name
     */
    public StoragePlatformEntity getStoragePlatformByName(String name);

    // Storage

    /**
     * Gets a storage by it's name.
     *
     * @param storageName the storage name (case-insensitive)
     *
     * @return the storage for the specified name
     */
    public StorageEntity getStorageByName(String storageName);

    /**
     * Gets a list of storage keys for all storages defined in the system.
     *
     * @return the list of storage keys
     */
    public List<StorageKey> getStorages();

    // StorageUnit

    /**
     * Gets a storage unit identified by the given business object data entity and storage name. Returns {@code null} if storage does not exist.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param storageName the storage name (case-insensitive)
     *
     * @return {@link StorageUnitEntity} or {@code null}
     */
    public StorageUnitEntity getStorageUnitByBusinessObjectDataAndStorageName(BusinessObjectDataEntity businessObjectDataEntity, String storageName);

    /**
     * Returns a first discovered storage unit in the specified storage that overlaps with the directory path.
     *
     * @param storageName the storage name (case-insensitive)
     * @param directoryPath the directory path
     *
     * @return the first found storage unit in the specified storage that overlaps with the directory path or null if none were found
     */
    public StorageUnitEntity getStorageUnitByStorageNameAndDirectoryPath(String storageName, String directoryPath);

    /**
     * Retrieves a list of storage units that belong to the specified storage for the specified business object data.
     *
     * @param storageEntity the storage entity
     * @param businessObjectDataEntities the list of business object data entities
     *
     * @return the list of storage unit entities
     */
    public List<StorageUnitEntity> getStorageUnitsByStorageAndBusinessObjectData(StorageEntity storageEntity,
        List<BusinessObjectDataEntity> businessObjectDataEntities);

    // StorageFile

    /**
     * Retrieves storage file by storage name and file path.
     *
     * @param storageName the storage name (case-insensitive)
     * @param filePath the file path
     *
     * @return the storage file
     */
    public StorageFileEntity getStorageFileByStorageNameAndFilePath(String storageName, String filePath);

    /**
     * Counts all storage files matching the file path prefix in the specified storage.
     *
     * @param storageName the storage name (case-insensitive)
     * @param filePathPrefix the file path prefix that file paths should match
     *
     * @return the storage file count
     */
    public Long getStorageFileCount(String storageName, String filePathPrefix);

    /**
     * Retrieves a sorted list of storage files matching S3 key prefix in the specified storage.
     *
     * @param storageName the storage name (case-insensitive)
     * @param filePathPrefix the file path prefix that file paths should match
     *
     * @return the list of storage file entities sorted by file path
     */
    public List<StorageFileEntity> getStorageFileEntities(String storageName, String filePathPrefix);

    /**
     * Retrieves a sorted list of storage files that belong to the specified storage for the specified business object data.
     *
     * @param storageEntity the storage entity
     * @param businessObjectDataEntities the list of business object data entities
     *
     * @return the list of storage file entities sorted by file path
     */
    public List<StorageFileEntity> getStorageFilesByStorageAndBusinessObjectData(StorageEntity storageEntity,
        List<BusinessObjectDataEntity> businessObjectDataEntities);

    // StorageUploadStatistics

    /**
     * Retrieves cumulative daily upload statistics for the storage for the specified upload date range.
     *
     * @param storageAlternateKey the storage alternate key (case-insensitive)
     * @param dateRange the upload date range
     *
     * @return the upload statistics
     */
    public StorageDailyUploadStats getStorageUploadStats(StorageAlternateKeyDto storageAlternateKey, DateRangeDto dateRange);

    /**
     * Retrieves daily upload statistics for the storage by business object definition for the specified upload date range.
     *
     * @param storageAlternateKey the storage alternate key (case-insensitive)
     * @param dateRange the upload date range
     *
     * @return the upload statistics
     */
    public StorageBusinessObjectDefinitionDailyUploadStats getStorageUploadStatsByBusinessObjectDefinition(StorageAlternateKeyDto storageAlternateKey,
        DateRangeDto dateRange);

    // JobDefinition

    /**
     * Retrieves job definition entity by alternate key.
     *
     * @param namespace the namespace (case-insensitive)
     * @param jobName the job name (case-insensitive)
     *
     * @return the job definition entity
     */
    public JobDefinitionEntity getJobDefinitionByAltKey(String namespace, String jobName);

    // EmrClusterDefinition

    /**
     * Retrieves EMR cluster definition entity by alternate key.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     *
     * @return the EMR cluster definition entity
     */
    public EmrClusterDefinitionEntity getEmrClusterDefinitionByAltKey(EmrClusterDefinitionKey emrClusterDefinitionKey);

    /**
     * Retrieves EMR cluster definition entity by alternate key.
     *
     * @param namespaceCd the namespace (case-insensitive)
     * @param definitionName the EMR cluster definition name (case-insensitive)
     *
     * @return the EMR cluster definition entity
     */
    public EmrClusterDefinitionEntity getEmrClusterDefinitionByAltKey(String namespaceCd, String definitionName);

    // NotificationEvent

    /**
     * Gets a notification event type by it's code.
     *
     * @param code the notification event type code (case-insensitive)
     *
     * @return the notification event type for the specified code
     */
    public NotificationEventTypeEntity getNotificationEventTypeByCode(String code);

    // BusinessObjectDataNotification

    /**
     * Retrieves a business object data notification registration entity by alternate key.
     *
     * @param key the business object data notification registration key (case-insensitive)
     *
     * @return the business object data notification registration entity
     */
    public BusinessObjectDataNotificationRegistrationEntity getBusinessObjectDataNotificationByAltKey(BusinessObjectDataNotificationRegistrationKey key);

    /**
     * Retrieves a list of business object data notification registration keys defined for the specified namespace.
     *
     * @param namespaceCode the namespace code (case-insensitive)
     *
     * @return the list of business object data notification registration keys
     */
    public List<BusinessObjectDataNotificationRegistrationKey> getBusinessObjectDataNotificationRegistrationKeys(String namespaceCode);

    /**
     * Retrieves a list of business object data notification entities that match given input parameters.
     *
     * @param notificationEventTypeCode the notification event type code (case-insensitive)
     * @param businessObjectDataKey the business object data key (case-insensitive)
     *
     * @return the list of business object data notification entities
     */
    public List<BusinessObjectDataNotificationRegistrationEntity> getBusinessObjectDataNotificationRegistrations(String notificationEventTypeCode,
        BusinessObjectDataKey businessObjectDataKey);

    // SecurityFunction

    /**
     * Gets a list of functions for the role.
     *
     * @param roleCd the role code
     *
     * @return the list of functions
     */
    public List<String> getSecurityFunctionsForRole(String roleCd);

    /**
     * Gets a list of security functions.
     *
     * @return the list of functions
     */
    public List<String> getSecurityFunctions();

    // JmsMessage

    /**
     * Selects the oldest JMS message (a message with the lowest sequence generated id) from the queue.
     *
     * @return the JMS message
     */
    public JmsMessageEntity getOldestJmsMessage();

    // OnDemandPricing

    /**
     * Returns the on-demand price with the specified region and instance type. Returns {@code null} if no on-demand price is found. Throws an exception when
     * more than 1 on-demand price is found.
     *
     * @param region The on-demand price's region.
     * @param instanceType The on-demand price's instance type.
     *
     * @return The on-demand price.
     */
    public OnDemandPriceEntity getOnDemandPrice(String region, String instanceType);
}
