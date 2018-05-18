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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.ExpectedPartitionValueDao;
import org.finra.herd.dao.StorageFileDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusHistoryEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.ExpectedPartitionValueEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.BusinessObjectDataService;
import org.finra.herd.service.MessageNotificationEventService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.S3Service;

/**
 * Helper for business object data related operations which require DAO.
 */
@Component
public class BusinessObjectDataDaoHelper
{
    private static final List<String> NULL_VALUE_LIST = Arrays.asList((String) null);

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private AttributeDaoHelper attributeDaoHelper;

    @Autowired
    private AttributeHelper attributeHelper;

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectDataStatusDaoHelper businessObjectDataStatusDaoHelper;

    @Autowired
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private ExpectedPartitionValueDao expectedPartitionValueDao;

    @Autowired
    private MessageNotificationEventService messageNotificationEventService;

    @Autowired
    private NotificationEventService notificationEventService;

    @Autowired
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private StorageFileDao storageFileDao;

    @Autowired
    private StorageFileHelper storageFileHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StorageUnitDao storageUnitDao;

    @Autowired
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Autowired
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    /**
     * Build partition filters based on the specified partition value filters.  This method also validates the partition value filters (including partition
     * keys) against the business object format schema.  When request contains multiple partition value filters, the system will check business object data
     * availability for n-fold Cartesian product of the partition values specified, where n is a number of partition value filters (partition value sets).
     *
     * @param partitionValueFilters the list of partition value filters
     * @param standalonePartitionValueFilter the standalone partition value filter
     * @param businessObjectFormatKey the business object format key
     * @param businessObjectDataVersion the business object data version
     * @param storageNames the optional list of storage names (case-insensitive)
     * @param storagePlatformType the optional storage platform type, e.g. S3 for Hive DDL. It is ignored when the list of storages is not empty
     * @param excludedStoragePlatformType the optional storage platform type to be excluded from search. It is ignored when the list of storages is not empty or
     * the storage platform type is specified
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return the list of partition filters
     */
    public List<List<String>> buildPartitionFilters(List<PartitionValueFilter> partitionValueFilters, PartitionValueFilter standalonePartitionValueFilter,
        BusinessObjectFormatKey businessObjectFormatKey, Integer businessObjectDataVersion, List<String> storageNames, String storagePlatformType,
        String excludedStoragePlatformType, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        // Build a list of partition value filters to process based on the specified partition value filters.
        List<PartitionValueFilter> partitionValueFiltersToProcess = getPartitionValuesToProcess(partitionValueFilters, standalonePartitionValueFilter);

        // Build a map of column positions and the relative partition values.
        Map<Integer, List<String>> partitionValues = new HashMap<>();

        // Initialize the map with null partition values for all possible primary and sub-partition values. Partition column position uses one-based numbering.
        for (int i = 0; i < BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1; i++)
        {
            partitionValues.put(i, NULL_VALUE_LIST);
        }

        // Process all partition value filters one by one and populate the relative entries in the map.
        for (PartitionValueFilter partitionValueFilter : partitionValueFiltersToProcess)
        {
            // Get the partition key.  If partition key is not specified, use the primary partition column.
            String partitionKey = StringUtils.isNotBlank(partitionValueFilter.getPartitionKey()) ? partitionValueFilter.getPartitionKey() :
                businessObjectFormatEntity.getPartitionKey();

            // Get the partition column position (one-based numbering).
            int partitionColumnPosition = getPartitionColumnPosition(partitionKey, businessObjectFormatEntity);

            // Get unique and sorted list of partition values to check the availability for.
            List<String> uniqueAndSortedPartitionValues =
                getPartitionValues(partitionValueFilter, partitionKey, partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion,
                    storageNames, storagePlatformType, excludedStoragePlatformType, businessObjectFormatEntity);

            // Add this partition value filter to the map.
            List<String> previousPartitionValues = partitionValues.put(partitionColumnPosition - 1, uniqueAndSortedPartitionValues);

            // Check if this partition column has not been already added.
            if (!NULL_VALUE_LIST.equals(previousPartitionValues))
            {
                throw new IllegalArgumentException("Partition value filters specify duplicate partition columns.");
            }
        }

        // When request contains multiple partition value filters, the system will check business object data availability for n-fold Cartesian product
        // of the partition values specified, where n is a number of partition value filters (partition value sets).
        List<String[]> crossProductResult = getCrossProduct(partitionValues);
        List<List<String>> partitionFilters = new ArrayList<>();
        for (String[] crossProductRow : crossProductResult)
        {
            partitionFilters.add(Arrays.asList(crossProductRow));
        }

        return partitionFilters;
    }

    /**
     * Creates a new business object data from the request information.
     *
     * @param request the request
     *
     * @return the newly created and persisted business object data
     */
    public BusinessObjectData createBusinessObjectData(BusinessObjectDataCreateRequest request)
    {
        // By default, fileSize value is required.
        return createBusinessObjectData(request, true);
    }

    /**
     * Creates a new business object data from the request information.
     *
     * @param request the request
     * @param fileSizeRequired specifies if fileSizeBytes value is required or not
     *
     * @return the newly created and persisted business object data
     */
    public BusinessObjectData createBusinessObjectData(BusinessObjectDataCreateRequest request, boolean fileSizeRequired)
    {
        if (StringUtils.isBlank(request.getStatus()))
        {
            request.setStatus(BusinessObjectDataStatusEntity.VALID);
        }
        else
        {
            request.setStatus(request.getStatus().trim());
        }

        // Get the status entity if status is specified else set it to VALID
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(request.getStatus());

        // Perform the validation.
        validateBusinessObjectDataCreateRequest(request, fileSizeRequired, businessObjectDataStatusEntity);

        // Get the business object format for the specified parameters and make sure it exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(
            new BusinessObjectFormatKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()));

        attributeDaoHelper
            .validateAttributesAgainstBusinessObjectDataAttributeDefinitions(request.getAttributes(), businessObjectFormatEntity.getAttributeDefinitions());

        // Ensure the specified partition key matches what's configured within the business object format.
        Assert.isTrue(businessObjectFormatEntity.getPartitionKey().equalsIgnoreCase(request.getPartitionKey()), String
            .format("Partition key \"%s\" doesn't match configured business object format partition key \"%s\".", request.getPartitionKey(),
                businessObjectFormatEntity.getPartitionKey()));

        // Get the latest format version for this business object data, if it exists.
        BusinessObjectDataEntity existingBusinessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(
            new BusinessObjectDataKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion(), request.getPartitionValue(),
                request.getSubPartitionValues(), null));

        // Throw an error if this business object data already exists and createNewVersion flag is not set.
        if (existingBusinessObjectDataEntity != null && (!Boolean.TRUE.equals(request.isCreateNewVersion()) ||
            Boolean.TRUE.equals(existingBusinessObjectDataEntity.getStatus().getPreRegistrationStatus())))
        {
            throw new AlreadyExistsException("Unable to create business object data because it already exists.");
        }

        // Create a business object data entity from the request information.
        // Please note that simply adding 1 to the latest version without "DB locking" is sufficient here,
        // even for multi-threading, since we are relying on the DB having version as part of the alternate key.
        Integer businessObjectDataVersion = existingBusinessObjectDataEntity == null ? BusinessObjectDataEntity.BUSINESS_OBJECT_DATA_INITIAL_VERSION :
            existingBusinessObjectDataEntity.getVersion() + 1;
        BusinessObjectDataEntity newVersionBusinessObjectDataEntity =
            createBusinessObjectDataEntity(request, businessObjectFormatEntity, businessObjectDataVersion, businessObjectDataStatusEntity);

        // Update the existing latest business object data version entity, so it would not be flagged as the latest version anymore.
        if (existingBusinessObjectDataEntity != null)
        {
            existingBusinessObjectDataEntity.setLatestVersion(Boolean.FALSE);
            businessObjectDataDao.saveAndRefresh(existingBusinessObjectDataEntity);
        }

        // Add an entry to the business object data status history table.
        BusinessObjectDataStatusHistoryEntity businessObjectDataStatusHistoryEntity = new BusinessObjectDataStatusHistoryEntity();
        businessObjectDataStatusHistoryEntity.setBusinessObjectData(newVersionBusinessObjectDataEntity);
        businessObjectDataStatusHistoryEntity.setStatus(businessObjectDataStatusEntity);
        List<BusinessObjectDataStatusHistoryEntity> businessObjectDataStatusHistoryEntities = new ArrayList<>();
        businessObjectDataStatusHistoryEntities.add(businessObjectDataStatusHistoryEntity);
        newVersionBusinessObjectDataEntity.setHistoricalStatuses(businessObjectDataStatusHistoryEntities);

        // Persist the new entity.
        newVersionBusinessObjectDataEntity = businessObjectDataDao.saveAndRefresh(newVersionBusinessObjectDataEntity);

        // Create a status change notification to be sent on create business object data event.
        messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataHelper.getBusinessObjectDataKey(newVersionBusinessObjectDataEntity),
                businessObjectDataStatusEntity.getCode(), null);

        // Create and return the business object data object from the persisted entity.
        return businessObjectDataHelper.createBusinessObjectDataFromEntity(newVersionBusinessObjectDataEntity);
    }

    /**
     * Creates a storage unit entity per specified parameters.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param storageEntity the storage entity
     * @param storageDirectory the storage directory
     * @param storageFiles the list of storage files
     * @param isDiscoverStorageFiles specifies if
     *
     * @return the newly created storage unit entity
     */
    public StorageUnitEntity createStorageUnitEntity(BusinessObjectDataEntity businessObjectDataEntity, StorageEntity storageEntity,
        StorageDirectory storageDirectory, List<StorageFile> storageFiles, Boolean isDiscoverStorageFiles)
    {
        // Get the storage unit status entity for the ENABLED status.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.ENABLED);

        // Set up flags which are used to make flow logic easier.
        boolean isS3StoragePlatform = storageEntity.getStoragePlatform().getName().equals(StoragePlatformEntity.S3);
        boolean isStorageDirectorySpecified = (storageDirectory != null);
        boolean validatePathPrefix = storageHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true);
        boolean validateFileExistence = storageHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true);
        boolean validateFileSize = storageHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE), storageEntity,
                false, true);

        // Ensure that file size validation is not enabled without file existence validation.
        if (validateFileSize)
        {
            Assert.isTrue(validateFileExistence,
                String.format("Storage \"%s\" has file size validation enabled without file existence validation.", storageEntity.getName()));
        }

        String expectedS3KeyPrefix = null;
        // Retrieve S3 key prefix velocity template storage attribute value and store it in memory.
        // Please note that it is not required, so we pass in a "false" flag.
        String s3KeyPrefixVelocityTemplate = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE), storageEntity,
                false);

        if (StringUtils.isNotBlank(s3KeyPrefixVelocityTemplate))
        {
            // If the storage has any validation configured, get the expected S3 key prefix.
            expectedS3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(s3KeyPrefixVelocityTemplate, businessObjectDataEntity.getBusinessObjectFormat(),
                businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity), storageEntity.getName());
        }

        if ((validatePathPrefix || validateFileExistence) && isS3StoragePlatform)
        {
            // If path prefix validation is enabled, validate that S3 key prefix velocity template is configured.
            Assert.isTrue(!validatePathPrefix || StringUtils.isNotBlank(s3KeyPrefixVelocityTemplate),
                String.format("Storage \"%s\" has enabled path validation without S3 key prefix velocity template configured.", storageEntity.getName()));
        }

        // Process storage directory path if it is specified.
        String directoryPath = null;
        if (isStorageDirectorySpecified)
        {
            // Get the specified directory path.
            directoryPath = storageDirectory.getDirectoryPath();

            // If the validate path prefix flag is configured for this storage, validate the directory path value.
            if (validatePathPrefix && isS3StoragePlatform)
            {
                // Ensure the directory path adheres to the S3 naming convention.
                Assert.isTrue(directoryPath.equals(expectedS3KeyPrefix),
                    String.format("Specified directory path \"%s\" does not match the expected S3 key prefix \"%s\".", directoryPath, expectedS3KeyPrefix));

                // Ensure that the directory path is not already registered with another business object data instance.
                StorageUnitEntity alreadyRegisteredStorageUnitEntity = storageUnitDao.getStorageUnitByStorageAndDirectoryPath(storageEntity, directoryPath);
                if (alreadyRegisteredStorageUnitEntity != null)
                {
                    throw new AlreadyExistsException(String
                        .format("Storage directory \"%s\" in \"%s\" storage is already registered by the business object data {%s}.", directoryPath,
                            storageEntity.getName(),
                            businessObjectDataHelper.businessObjectDataEntityAltKeyToString(alreadyRegisteredStorageUnitEntity.getBusinessObjectData())));
                }
            }
        }
        else if (Boolean.TRUE.equals(businessObjectDataEntity.getStatus().getPreRegistrationStatus()))
        {
            directoryPath = expectedS3KeyPrefix;
        }

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitDaoHelper.setStorageUnitStatus(storageUnitEntity, storageUnitStatusEntity);
        storageUnitEntity.setDirectoryPath(directoryPath);

        // Discover storage files if storage file discovery is enabled. Otherwise, get the storage files specified in the request, if any.
        List<StorageFile> resultStorageFiles = BooleanUtils.isTrue(isDiscoverStorageFiles) ? discoverStorageFiles(storageEntity, directoryPath) : storageFiles;

        // Create the storage file entities.
        createStorageFileEntitiesFromStorageFiles(resultStorageFiles, storageEntity, BooleanUtils.isTrue(isDiscoverStorageFiles), expectedS3KeyPrefix,
            storageUnitEntity, directoryPath, validatePathPrefix, validateFileExistence, validateFileSize, isS3StoragePlatform);

        return storageUnitEntity;
    }

    /**
     * Gets business object data based on the key information.
     *
     * @param businessObjectDataKey the business object data key.
     *
     * @return the business object data.
     */
    public BusinessObjectDataEntity getBusinessObjectDataEntity(BusinessObjectDataKey businessObjectDataKey)
    {
        // Retrieve the business object data entity regardless of its status.
        return getBusinessObjectDataEntityByKeyAndStatus(businessObjectDataKey, null);
    }

    /**
     * Retrieves business object data by it's key. If a format version isn't specified, the latest available format version (for this partition value) will be
     * used. If a business object data version isn't specified, the latest data version based on the specified business object data status is returned. When
     * both business object data version and business object data status are not specified, the latest data version for each set of partition values will be
     * used regardless of the status.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataStatus the business object data status. This parameter is ignored when the business object data version is specified.
     *
     * @return the business object data
     */
    public BusinessObjectDataEntity getBusinessObjectDataEntityByKeyAndStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
    {
        // Get the business object data based on the specified parameters.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDao.getBusinessObjectDataByAltKeyAndStatus(businessObjectDataKey, businessObjectDataStatus);

        // Make sure that business object data exists.
        if (businessObjectDataEntity == null)
        {
            throw new ObjectNotFoundException(
                String.format("Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", " +
                    "businessObjectDataSubPartitionValues: \"%s\", businessObjectDataVersion: %d, businessObjectDataStatus: \"%s\"} doesn't exist.",
                    businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                    businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                    businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(),
                    CollectionUtils.isEmpty(businessObjectDataKey.getSubPartitionValues()) ? "" :
                        StringUtils.join(businessObjectDataKey.getSubPartitionValues(), ","), businessObjectDataKey.getBusinessObjectDataVersion(),
                    businessObjectDataStatus));
        }

        // Return the retrieved business object data entity.
        return businessObjectDataEntity;
    }

    /**
     * Builds a list of partition values from the partition value filter. The partition range takes precedence over the list of partition values in the filter.
     * If a range is specified the list of values will come from the expected partition values table for values within the specified range. If the list is
     * specified, duplicates will be removed. In both cases, the list will be ordered ascending.
     *
     * @param partitionValueFilter the partition value filter that was validated to have exactly one partition value filter option
     * @param partitionKey the partition key
     * @param partitionColumnPosition the partition column position (one-based numbering)
     * @param businessObjectFormatKey the business object format key
     * @param businessObjectDataVersion the business object data version
     * @param storageNames the optional list of storage names (case-insensitive)
     * @param storagePlatformType the optional storage platform type, e.g. S3 for Hive DDL. It is ignored when the list of storages is not empty
     * @param excludedStoragePlatformType the optional storage platform type to be excluded from search. It is ignored when the list of storages is not empty or
     * the storage platform type is specified
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return the unique and sorted partition value list
     */
    public List<String> getPartitionValues(PartitionValueFilter partitionValueFilter, String partitionKey, int partitionColumnPosition,
        BusinessObjectFormatKey businessObjectFormatKey, Integer businessObjectDataVersion, List<String> storageNames, String storagePlatformType,
        String excludedStoragePlatformType, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        List<String> partitionValues = new ArrayList<>();

        if (partitionValueFilter.getPartitionValueRange() != null)
        {
            // A "partition value range" filter option is specified.
            partitionValues = processPartitionValueRangeFilterOption(partitionValueFilter.getPartitionValueRange(), businessObjectFormatEntity);
        }
        else if (partitionValueFilter.getPartitionValues() != null)
        {
            // A "partition value list" filter option is specified.
            partitionValues =
                processPartitionValueListFilterOption(partitionValueFilter.getPartitionValues(), partitionKey, partitionColumnPosition, businessObjectFormatKey,
                    businessObjectDataVersion, storageNames, storagePlatformType, excludedStoragePlatformType);
        }
        else if (partitionValueFilter.getLatestBeforePartitionValue() != null)
        {
            // A "latest before partition value" filter option is specified.

            // Retrieve the maximum partition value before (inclusive) the specified partition value.
            // If a business object data version isn't specified, the latest VALID business object data version will be used.
            String maxPartitionValue = businessObjectDataDao
                .getBusinessObjectDataMaxPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion,
                    BusinessObjectDataStatusEntity.VALID, storageNames, storagePlatformType, excludedStoragePlatformType,
                    partitionValueFilter.getLatestBeforePartitionValue().getPartitionValue(), null);
            if (maxPartitionValue != null)
            {
                partitionValues.add(maxPartitionValue);
            }
            else
            {
                throw new ObjectNotFoundException(
                    getLatestPartitionValueNotFoundErrorMessage("before", partitionValueFilter.getLatestBeforePartitionValue().getPartitionValue(),
                        partitionKey, businessObjectFormatKey, businessObjectDataVersion, storageNames));
            }
        }
        else
        {
            // A "latest after partition value" filter option is specified.

            // Retrieve the maximum partition value before (inclusive) the specified partition value.
            // If a business object data version isn't specified, the latest VALID business object data version will be used.
            String maxPartitionValue = businessObjectDataDao
                .getBusinessObjectDataMaxPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion,
                    BusinessObjectDataStatusEntity.VALID, storageNames, storagePlatformType, excludedStoragePlatformType, null,
                    partitionValueFilter.getLatestAfterPartitionValue().getPartitionValue());
            if (maxPartitionValue != null)
            {
                partitionValues.add(maxPartitionValue);
            }
            else
            {
                throw new ObjectNotFoundException(
                    getLatestPartitionValueNotFoundErrorMessage("after", partitionValueFilter.getLatestAfterPartitionValue().getPartitionValue(), partitionKey,
                        businessObjectFormatKey, businessObjectDataVersion, storageNames));
            }
        }

        // If a max partition values limit has been set, check the limit and throw an exception if the limit has been exceeded.
        Integer availabilityDdlMaxPartitionValues = configurationHelper.getProperty(ConfigurationValue.AVAILABILITY_DDL_MAX_PARTITION_VALUES, Integer.class);
        if ((availabilityDdlMaxPartitionValues != null) && (partitionValues.size() > availabilityDdlMaxPartitionValues))
        {
            throw new IllegalArgumentException(
                "The number of partition values (" + partitionValues.size() + ") exceeds the system limit of " + availabilityDdlMaxPartitionValues + ".");
        }

        // Return the partition values.
        return partitionValues;
    }

    /**
     * Trigger business object data and storage unit notification for business object data creation event.
     *
     * @param businessObjectData the business object data
     */
    public void triggerNotificationsForCreateBusinessObjectData(BusinessObjectData businessObjectData)
    {
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectData);

        // Create business object data notifications.
        for (NotificationEventTypeEntity.EventTypesBdata eventType : Arrays
            .asList(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN, NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG))
        {
            notificationEventService.processBusinessObjectDataNotificationEventAsync(eventType, businessObjectDataKey, businessObjectData.getStatus(), null);
        }

        // Create storage unit notifications.
        for (StorageUnit storageUnit : businessObjectData.getStorageUnits())
        {
            notificationEventService
                .processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG, businessObjectDataKey,
                    storageUnit.getStorage().getName(), storageUnit.getStorageUnitStatus(), null);
        }
    }

    /**
     * Trigger business object data and storage unit notification for unregistered business object data invalidation event.
     *
     * @param businessObjectDataInvalidateUnregisteredResponse the business object data invalidate unregistered response
     */
    public void triggerNotificationsForInvalidateUnregisteredBusinessObjectData(
        BusinessObjectDataInvalidateUnregisteredResponse businessObjectDataInvalidateUnregisteredResponse)
    {
        for (BusinessObjectData businessObjectData : businessObjectDataInvalidateUnregisteredResponse.getRegisteredBusinessObjectDataList())
        {
            triggerNotificationsForCreateBusinessObjectData(businessObjectData);
        }
    }

    /**
     * Update the business object data status.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param status the status
     */
    public void updateBusinessObjectDataStatus(BusinessObjectDataEntity businessObjectDataEntity, String status)
    {
        // Retrieve and ensure the status is valid.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(status);

        // Save the current status value.
        String oldStatus = businessObjectDataEntity.getStatus().getCode();

        // Update the entity with the new values.
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);

        // Add an entry to the business object data status history table
        BusinessObjectDataStatusHistoryEntity businessObjectDataStatusHistoryEntity = new BusinessObjectDataStatusHistoryEntity();
        businessObjectDataEntity.getHistoricalStatuses().add(businessObjectDataStatusHistoryEntity);
        businessObjectDataStatusHistoryEntity.setBusinessObjectData(businessObjectDataEntity);
        businessObjectDataStatusHistoryEntity.setStatus(businessObjectDataStatusEntity);

        // Persist the entity.
        businessObjectDataDao.saveAndRefresh(businessObjectDataEntity);

        // Sent a business object data status change notification.
        messageNotificationEventService
            .processBusinessObjectDataStatusChangeNotificationEvent(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity),
                businessObjectDataStatusEntity.getCode(), oldStatus);
    }

    /**
     * Returns a cloned version of the specified business object data key where all fields are made lowercase.
     *
     * @param businessObjectDataKey the business object data.
     *
     * @return the cloned business object data.
     */
    private BusinessObjectDataKey cloneToLowerCase(BusinessObjectDataKey businessObjectDataKey)
    {
        BusinessObjectDataKey businessObjectDataKeyClone = new BusinessObjectDataKey();

        businessObjectDataKeyClone.setNamespace(businessObjectDataKey.getNamespace().toLowerCase());
        businessObjectDataKeyClone.setBusinessObjectDefinitionName(businessObjectDataKey.getBusinessObjectDefinitionName().toLowerCase());
        businessObjectDataKeyClone.setBusinessObjectFormatUsage(businessObjectDataKey.getBusinessObjectFormatUsage().toLowerCase());
        businessObjectDataKeyClone.setBusinessObjectFormatFileType(businessObjectDataKey.getBusinessObjectFormatFileType().toLowerCase());
        businessObjectDataKeyClone.setBusinessObjectFormatVersion(businessObjectDataKey.getBusinessObjectFormatVersion());
        businessObjectDataKeyClone.setPartitionValue(businessObjectDataKey.getPartitionValue());
        businessObjectDataKeyClone.setBusinessObjectDataVersion(businessObjectDataKey.getBusinessObjectDataVersion());
        businessObjectDataKeyClone.setSubPartitionValues(businessObjectDataKey.getSubPartitionValues());

        return businessObjectDataKeyClone;
    }

    /**
     * Creates a new business object data entity from the request information.
     *
     * @param request the request.
     * @param businessObjectFormatEntity the business object format entity.
     * @param businessObjectDataVersion the business object data version.
     *
     * @return the newly created business object data entity.
     */
    private BusinessObjectDataEntity createBusinessObjectDataEntity(BusinessObjectDataCreateRequest request,
        BusinessObjectFormatEntity businessObjectFormatEntity, Integer businessObjectDataVersion, BusinessObjectDataStatusEntity businessObjectDataStatusEntity)
    {
        // Create a new entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(request.getPartitionValue());
        int subPartitionValuesCount = CollectionUtils.size(request.getSubPartitionValues());
        businessObjectDataEntity.setPartitionValue2(subPartitionValuesCount > 0 ? request.getSubPartitionValues().get(0) : null);
        businessObjectDataEntity.setPartitionValue3(subPartitionValuesCount > 1 ? request.getSubPartitionValues().get(1) : null);
        businessObjectDataEntity.setPartitionValue4(subPartitionValuesCount > 2 ? request.getSubPartitionValues().get(2) : null);
        businessObjectDataEntity.setPartitionValue5(subPartitionValuesCount > 3 ? request.getSubPartitionValues().get(3) : null);
        businessObjectDataEntity.setVersion(businessObjectDataVersion);
        businessObjectDataEntity.setLatestVersion(true);
        businessObjectDataEntity.setStatus(businessObjectDataStatusEntity);

        // Create the storage unit entities.
        businessObjectDataEntity.setStorageUnits(createStorageUnitEntitiesFromStorageUnits(request.getStorageUnits(), businessObjectDataEntity));

        // Create the attributes.
        List<BusinessObjectDataAttributeEntity> attributeEntities = new ArrayList<>();
        businessObjectDataEntity.setAttributes(attributeEntities);

        if (CollectionUtils.isNotEmpty(request.getAttributes()))
        {
            for (Attribute attribute : request.getAttributes())
            {
                BusinessObjectDataAttributeEntity attributeEntity = new BusinessObjectDataAttributeEntity();
                attributeEntities.add(attributeEntity);
                attributeEntity.setBusinessObjectData(businessObjectDataEntity);
                attributeEntity.setName(attribute.getName());
                attributeEntity.setValue(attribute.getValue());
            }
        }

        // Create the parents.
        List<BusinessObjectDataEntity> businessObjectDataParents = new ArrayList<>();
        businessObjectDataEntity.setBusinessObjectDataParents(businessObjectDataParents);

        // Loop through all the business object data parents.
        if (request.getBusinessObjectDataParents() != null)
        {
            for (BusinessObjectDataKey businessObjectDataKey : request.getBusinessObjectDataParents())
            {
                // Look up the business object data for each parent.
                BusinessObjectDataEntity businessObjectDataParent = getBusinessObjectDataEntity(businessObjectDataKey);

                // Add our newly created entity as a dependent (i.e. child) of the looked up parent.
                businessObjectDataParent.getBusinessObjectDataChildren().add(businessObjectDataEntity);

                // Add the looked up parent as a parent of our newly created entity.
                businessObjectDataParents.add(businessObjectDataParent);
            }
        }

        // Return the newly created entity.
        return businessObjectDataEntity;
    }

    /**
     * Creates a list of storage file entities from a list of storage files.
     *
     * @param storageFiles the list of storage files
     * @param storageEntity the storage entity
     * @param storageFilesDiscovered specifies whether the storage files were actually discovered in the storage
     * @param expectedS3KeyPrefix the expected S3 key prefix
     * @param storageUnitEntity the storage unit entity that storage file entities will belong to
     * @param directoryPath the storage directory path
     * @param validatePathPrefix specifies whether the storage has S3 key prefix validation enabled
     * @param validateFileExistence specifies whether the storage has file existence enabled
     * @param validateFileSize specifies whether the storage has file validation enabled
     * @param isS3StoragePlatform specifies whether the storage platform type is S3
     *
     * @return the list of storage file entities
     */
    private List<StorageFileEntity> createStorageFileEntitiesFromStorageFiles(List<StorageFile> storageFiles, StorageEntity storageEntity,
        boolean storageFilesDiscovered, String expectedS3KeyPrefix, StorageUnitEntity storageUnitEntity, String directoryPath, boolean validatePathPrefix,
        boolean validateFileExistence, boolean validateFileSize, boolean isS3StoragePlatform)
    {
        List<StorageFileEntity> storageFileEntities = null;

        // Process storage files if they are specified.
        if (CollectionUtils.isNotEmpty(storageFiles))
        {
            storageFileEntities = new ArrayList<>();
            storageUnitEntity.setStorageFiles(storageFileEntities);

            // If the validate file existence flag is configured for this storage and storage files were not discovered, prepare for S3 file validation.
            S3FileTransferRequestParamsDto params = null;
            Map<String, StorageFile> actualS3Keys = null;
            if (validateFileExistence && isS3StoragePlatform && !storageFilesDiscovered)
            {
                // Get the validate file parameters.
                params = getFileValidationParams(storageEntity, expectedS3KeyPrefix, storageUnitEntity, validatePathPrefix);

                // When listing S3 files, we ignore 0 byte objects that represent S3 directories.
                actualS3Keys = storageFileHelper.getStorageFilesMapFromS3ObjectSummaries(s3Service.listDirectory(params, true));
            }

            // If the validate path prefix flag is configured, ensure that there are no storage files already registered in this
            // storage by some other business object data that start with the expected S3 key prefix.
            if (validatePathPrefix && isS3StoragePlatform)
            {
                // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
                String expectedS3KeyPrefixWithTrailingSlash = expectedS3KeyPrefix + "/";
                Long registeredStorageFileCount = storageFileDao.getStorageFileCount(storageEntity.getName(), expectedS3KeyPrefixWithTrailingSlash);
                if (registeredStorageFileCount > 0)
                {
                    throw new AlreadyExistsException(String.format(
                        "Found %d storage file(s) matching \"%s\" S3 key prefix in \"%s\" " + "storage that is registered with another business object data.",
                        registeredStorageFileCount, expectedS3KeyPrefix, storageEntity.getName()));
                }
            }

            for (StorageFile storageFile : storageFiles)
            {
                StorageFileEntity storageFileEntity = new StorageFileEntity();
                storageFileEntities.add(storageFileEntity);
                storageFileEntity.setStorageUnit(storageUnitEntity);
                storageFileEntity.setPath(storageFile.getFilePath());
                storageFileEntity.setFileSizeBytes(storageFile.getFileSizeBytes());
                storageFileEntity.setRowCount(storageFile.getRowCount());

                // Skip storage file validation if storage files were discovered.
                if (!storageFilesDiscovered)
                {
                    // Validate that the storage file path matches the key prefix if the validate path prefix flag is configured for this storage.
                    // Otherwise, if a directory path is specified, ensure it is consistent with the file path.
                    if (validatePathPrefix && isS3StoragePlatform)
                    {
                        // Ensure the S3 file key prefix adheres to the S3 naming convention.
                        Assert.isTrue(storageFileEntity.getPath().startsWith(expectedS3KeyPrefix), String
                            .format("Specified storage file path \"%s\" does not match the expected S3 key prefix \"%s\".", storageFileEntity.getPath(),
                                expectedS3KeyPrefix));
                    }
                    else if (directoryPath != null)
                    {
                        // When storage directory path is specified, ensure that storage file path starts with it.
                        Assert.isTrue(storageFileEntity.getPath().startsWith(directoryPath), String
                            .format("Storage file path \"%s\" does not match the storage directory path \"%s\".", storageFileEntity.getPath(), directoryPath));
                    }

                    // Ensure the file exists in S3 if the validate file existence flag is configured for this storage.
                    if (validateFileExistence && isS3StoragePlatform)
                    {
                        // Validate storage file.
                        storageFileHelper.validateStorageFile(storageFile, params.getS3BucketName(), actualS3Keys, validateFileSize);
                    }
                }
            }
        }

        return storageFileEntities;
    }

    /**
     * Creates a list of storage unit entities from a list of storage unit create requests.
     *
     * @param storageUnitCreateRequests the storage unit create requests
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the list of storage unit entities.
     */
    private List<StorageUnitEntity> createStorageUnitEntitiesFromStorageUnits(List<StorageUnitCreateRequest> storageUnitCreateRequests,
        BusinessObjectDataEntity businessObjectDataEntity)
    {
        // Create the storage units for the data.
        List<StorageUnitEntity> storageUnitEntities = new ArrayList<>();

        for (StorageUnitCreateRequest storageUnit : storageUnitCreateRequests)
        {
            // Get the storage entity per request and verify that it exists.
            StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageUnit.getStorageName());

            // Create storage unit and add it to the result list.
            storageUnitEntities.add(
                createStorageUnitEntity(businessObjectDataEntity, storageEntity, storageUnit.getStorageDirectory(), storageUnit.getStorageFiles(),
                    storageUnit.isDiscoverStorageFiles()));
        }

        return storageUnitEntities;
    }

    /**
     * Discovers storage files in the specified S3 storage that match the S3 key prefix.
     *
     * @param storageEntity the storage entity
     * @param s3KeyPrefix the S3 key prefix
     *
     * @return the list of discovered storage files
     */
    private List<StorageFile> discoverStorageFiles(StorageEntity storageEntity, String s3KeyPrefix)
    {
        // Only S3 storage platform is currently supported for storage file discovery.
        Assert.isTrue(storageEntity.getStoragePlatform().getName().equals(StoragePlatformEntity.S3),
            String.format("Cannot discover storage files at \"%s\" storage platform.", storageEntity.getStoragePlatform().getName()));

        // Get S3 bucket access parameters.
        S3FileTransferRequestParamsDto params = storageHelper.getS3BucketAccessParams(storageEntity);
        // Retrieve a list of all keys/objects from the S3 bucket matching the specified S3 key prefix.
        // Since S3 key prefix represents the directory, we add a trailing '/' character to it, unless it is already present.
        params.setS3KeyPrefix(StringUtils.appendIfMissing(s3KeyPrefix, "/"));
        // When listing S3 files, we ignore 0 byte objects that represent S3 directories.
        List<S3ObjectSummary> s3ObjectSummaries = s3Service.listDirectory(params, true);

        // Fail registration if no storage files were discovered.
        if (CollectionUtils.isEmpty(s3ObjectSummaries))
        {
            throw new ObjectNotFoundException(String.format("Found no files at \"s3://%s/%s\" location.", params.getS3BucketName(), params.getS3KeyPrefix()));
        }

        return storageFileHelper.createStorageFilesFromS3ObjectSummaries(s3ObjectSummaries);
    }

    /**
     * Returns a Cartesian product of the lists of values specified.
     *
     * @param lists the lists of values
     *
     * @return the Cartesian product
     */
    private List<String[]> getCrossProduct(Map<Integer, List<String>> lists)
    {
        List<String[]> results = new ArrayList<>();
        getCrossProduct(results, lists, 0, new String[lists.size()]);
        return results;
    }

    /**
     * A helper function used to compute a Cartesian product of the lists of values specified.
     */
    private void getCrossProduct(List<String[]> results, Map<Integer, List<String>> lists, int depth, String[] current)
    {
        for (int i = 0; i < lists.get(depth).size(); i++)
        {
            current[depth] = lists.get(depth).get(i);
            if (depth < lists.keySet().size() - 1)
            {
                getCrossProduct(results, lists, depth + 1, current);
            }
            else
            {
                results.add(Arrays.copyOf(current, current.length));
            }
        }
    }

    /**
     * Gets the file validation parameters that can be used for getting a list of files by the S3 service. The returned DTO will contain the expected S3 key
     * prefix when the "validate path prefix" flag is set or it will contain the directory of the storage entity if not.
     *
     * @param storageEntity the storage entity.
     * @param expectedS3KeyPrefix the expected key prefix.
     * @param storageUnitEntity the storage unit entity.
     * @param validatePathPrefix the validate path prefix flag.
     *
     * @return the parameters.
     * @throws IllegalArgumentException if the "validate path prefix" flag is not present and no directory is configured on the storage entity.
     */
    private S3FileTransferRequestParamsDto getFileValidationParams(StorageEntity storageEntity, String expectedS3KeyPrefix, StorageUnitEntity storageUnitEntity,
        boolean validatePathPrefix) throws IllegalArgumentException
    {
        // Use the expected S3 key prefix by default (which is set when the validatePathPrefix flag is set).
        String actualFileS3KeyPrefix = expectedS3KeyPrefix;

        // In the case where the validate path prefix flag isn't set, we will either use the specified directory if it exists or it's an error
        // since we have no way of knowing how to validate the files. It isn't reasonable to get a list of all files from S3 individually one by
        // one since this would cause performance problems if a large number of files are present. Getting all files within a single key
        // prefix is reasonable on the other hand since we can list all files at once starting at the key prefix.
        if (!validatePathPrefix)
        {
            if (StringUtils.isNotBlank(storageUnitEntity.getDirectoryPath()))
            {
                actualFileS3KeyPrefix = storageUnitEntity.getDirectoryPath();
            }
            else
            {
                throw new IllegalArgumentException("Unable to validate file existence because storage \"" + storageEntity.getName() +
                    "\" does not validate path prefix and storage unit doesn't have a directory configured.");
            }
        }

        // Add a trailing backslash if it doesn't already exist.
        actualFileS3KeyPrefix = StringUtils.appendIfMissing(actualFileS3KeyPrefix, "/");

        // Get S3 bucket access parameters and set the actual key prefix.
        S3FileTransferRequestParamsDto params = storageHelper.getS3BucketAccessParams(storageEntity);
        params.setS3KeyPrefix(actualFileS3KeyPrefix);

        // Return the newly created parameters.
        return params;
    }

    private String getLatestPartitionValueNotFoundErrorMessage(String boundType, String boundPartitionValue, String partitionKey,
        BusinessObjectFormatKey businessObjectFormatKey, Integer businessObjectDataVersion, List<String> storageNames)
    {
        return String.format("Failed to find partition value which is the latest %s partition value = \"%s\" for partition key = \"%s\" due to " +
                "no available business object data in \"%s\" storage that satisfies the search criteria. " +
                "Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataVersion: %d}", boundType, boundPartitionValue,
            partitionKey, StringUtils.join(storageNames, ","), businessObjectFormatKey.getNamespace(),
            businessObjectFormatKey.getBusinessObjectDefinitionName(), businessObjectFormatKey.getBusinessObjectFormatUsage(),
            businessObjectFormatKey.getBusinessObjectFormatFileType(), businessObjectFormatKey.getBusinessObjectFormatVersion(), businessObjectDataVersion);
    }

    /**
     * Returns the partition key column position (one-based numbering).
     *
     * @param partitionKey the partition key
     * @param businessObjectFormatEntity, the business object format entity
     *
     * @return the partition key column position
     * @throws IllegalArgumentException if partition key is not found in schema
     */
    private int getPartitionColumnPosition(String partitionKey, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        int partitionColumnPosition = 0;

        if (partitionKey.equalsIgnoreCase(businessObjectFormatEntity.getPartitionKey()))
        {
            partitionColumnPosition = BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION;
        }
        else
        {
            // Get business object format model object to directly access schema columns and partitions.
            BusinessObjectFormat businessObjectFormat = businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);

            Assert.notNull(businessObjectFormat.getSchema(), String.format(
                "Partition key \"%s\" doesn't match configured business object format partition key \"%s\" and " +
                    "there is no schema defined to check subpartition columns for business object format {%s}.", partitionKey,
                businessObjectFormatEntity.getPartitionKey(), businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));

            for (int i = 0; i < Math.min(BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1, businessObjectFormat.getSchema().getPartitions().size()); i++)
            {
                if (partitionKey.equalsIgnoreCase(businessObjectFormat.getSchema().getPartitions().get(i).getName()))
                {
                    // Partition key found in schema.
                    partitionColumnPosition = i + 1;
                    break;
                }
            }

            Assert.isTrue(partitionColumnPosition > 0, String
                .format("The partition key \"%s\" does not exist in first %d partition columns in the schema for business object format {%s}.", partitionKey,
                    BusinessObjectDataEntity.MAX_SUBPARTITIONS + 1,
                    businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
        }

        return partitionColumnPosition;
    }

    private String getPartitionValueNotFoundErrorMessage(String partitionValueType, String partitionKey, BusinessObjectFormatKey businessObjectFormatKey,
        Integer businessObjectDataVersion, List<String> storageNames)
    {
        return String.format("Failed to find %s partition value for partition key = \"%s\" due to " +
                "no available business object data in \"%s\" storage(s) that is registered using that partition. " +
                "Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataVersion: %d}", partitionValueType, partitionKey,
            StringUtils.join(storageNames, ","), businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
            businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(),
            businessObjectFormatKey.getBusinessObjectFormatVersion(), businessObjectDataVersion);
    }

    /**
     * Gets the partition values to process.
     *
     * @param partitionValueFilters the list of partition values.
     * @param standalonePartitionValueFilter the standalone partition value.
     *
     * @return the list of partition values to process.
     */
    private List<PartitionValueFilter> getPartitionValuesToProcess(List<PartitionValueFilter> partitionValueFilters,
        PartitionValueFilter standalonePartitionValueFilter)
    {
        // Build a list of partition value filters to process based on the specified partition value filters.
        List<PartitionValueFilter> partitionValueFiltersToProcess = new ArrayList<>();
        if (partitionValueFilters != null)
        {
            partitionValueFiltersToProcess.addAll(partitionValueFilters);
        }
        if (standalonePartitionValueFilter != null)
        {
            partitionValueFiltersToProcess.add(standalonePartitionValueFilter);
        }
        return partitionValueFiltersToProcess;
    }

    /**
     * Builds a list of partition values from a "partition value list" partition value filter option. Duplicates will be removed and the list will be ordered
     * ascending.
     *
     * @param partitionValues the partition values passed in the partition value list filter option
     * @param partitionKey the partition key
     * @param partitionColumnPosition the partition column position (one-based numbering)
     * @param businessObjectFormatKey the business object format key
     * @param businessObjectDataVersion the business object data version
     * @param storageNames the optional list of storage names (case-insensitive)
     * @param storagePlatformType the optional storage platform type, e.g. S3 for Hive DDL. It is ignored when the list of storages is not empty
     * @param excludedStoragePlatformType the optional storage platform type to be excluded from search. It is ignored when the list of storages is not empty or
     * the storage platform type is specified
     *
     * @return the unique and sorted partition value list
     */
    private List<String> processPartitionValueListFilterOption(List<String> partitionValues, String partitionKey, int partitionColumnPosition,
        BusinessObjectFormatKey businessObjectFormatKey, Integer businessObjectDataVersion, List<String> storageNames, String storagePlatformType,
        String excludedStoragePlatformType)
    {
        List<String> resultPartitionValues = new ArrayList<>();

        // Remove any duplicates and sort the request partition values.
        List<String> uniqueAndSortedPartitionValues = new ArrayList<>();
        uniqueAndSortedPartitionValues.addAll(new TreeSet<>(partitionValues));

        // This flag to be used to indicate if we updated partition value list per special partition value tokens.
        boolean partitionValueListUpdated = false;

        // If the maximum partition value token is specified, substitute special partition value token with the actual partition value.
        // If a business object data version isn't specified, the latest VALID business object data version will be used.
        if (uniqueAndSortedPartitionValues.contains(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN))
        {
            String maxPartitionValue = businessObjectDataDao
                .getBusinessObjectDataMaxPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion,
                    BusinessObjectDataStatusEntity.VALID, storageNames, storagePlatformType, excludedStoragePlatformType, null, null);
            if (maxPartitionValue == null)
            {
                throw new ObjectNotFoundException(
                    getPartitionValueNotFoundErrorMessage("maximum", partitionKey, businessObjectFormatKey, businessObjectDataVersion, storageNames));
            }
            uniqueAndSortedPartitionValues.remove(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN);
            uniqueAndSortedPartitionValues.add(maxPartitionValue);
            partitionValueListUpdated = true;
        }

        // If the minimum partition value token is specified, substitute special partition value token with the actual partition value.
        // If a business object data version isn't specified, the latest VALID business object data version will be used.
        if (uniqueAndSortedPartitionValues.contains(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN))
        {
            String minPartitionValue = businessObjectDataDao
                .getBusinessObjectDataMinPartitionValue(partitionColumnPosition, businessObjectFormatKey, businessObjectDataVersion,
                    BusinessObjectDataStatusEntity.VALID, storageNames, storagePlatformType, excludedStoragePlatformType);
            if (minPartitionValue == null)
            {
                throw new ObjectNotFoundException(
                    getPartitionValueNotFoundErrorMessage("minimum", partitionKey, businessObjectFormatKey, businessObjectDataVersion, storageNames));
            }
            uniqueAndSortedPartitionValues.remove(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN);
            uniqueAndSortedPartitionValues.add(minPartitionValue);
            partitionValueListUpdated = true;
        }

        // Build the final list of partition values.
        if (partitionValueListUpdated)
        {
            // Remove any duplicates and sort the list of partition values is we updated the list per special partition value tokens.
            resultPartitionValues.addAll(new TreeSet<>(uniqueAndSortedPartitionValues));
        }
        else
        {
            resultPartitionValues.addAll(uniqueAndSortedPartitionValues);
        }

        return resultPartitionValues;
    }

    /**
     * Builds a list of partition values from a "partition value range" partition value filter option. The list of partition values will come from the expected
     * partition values table for values within the specified range. The list will be ordered ascending.
     *
     * @param partitionValueRange the partition value range
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return the unique and sorted partition value list
     */
    private List<String> processPartitionValueRangeFilterOption(PartitionValueRange partitionValueRange, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        List<String> resultPartitionValues = new ArrayList<>();

        Assert.notNull(businessObjectFormatEntity.getPartitionKeyGroup(), String
            .format("A partition key group, which is required to use partition value ranges, is not specified for the business object format {%s}.",
                businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));

        List<ExpectedPartitionValueEntity> expectedPartitionValueEntities = expectedPartitionValueDao
            .getExpectedPartitionValuesByGroupAndRange(businessObjectFormatEntity.getPartitionKeyGroup().getPartitionKeyGroupName(), partitionValueRange);

        // Populate the partition values returned from the range query.
        for (ExpectedPartitionValueEntity expectedPartitionValueEntity : expectedPartitionValueEntities)
        {
            String partitionValue = expectedPartitionValueEntity.getPartitionValue();

            // Validate that expected partition value does not match to one of the partition value tokens.
            Assert.isTrue(!partitionValue.equals(BusinessObjectDataService.MAX_PARTITION_VALUE_TOKEN) &&
                    !partitionValue.equals(BusinessObjectDataService.MIN_PARTITION_VALUE_TOKEN),
                "A partition value token cannot be specified as one of the expected partition values.");

            resultPartitionValues.add(partitionValue);
        }

        // Validate that our partition value range results in a non-empty partition value list.
        Assert.notEmpty(resultPartitionValues, String
            .format("Partition value range [\"%s\", \"%s\"] contains no valid partition values in partition key group \"%s\". Business object format: {%s}",
                partitionValueRange.getStartPartitionValue(), partitionValueRange.getEndPartitionValue(),
                businessObjectFormatEntity.getPartitionKeyGroup().getPartitionKeyGroupName(),
                businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));

        return resultPartitionValues;
    }

    /**
     * Validates the business object data create request. This method also trims appropriate request parameters.
     *
     * @param request the request
     * @param fileSizeRequired specifies if fileSizeBytes value is required or not
     * @param businessObjectDataStatusEntity The status entity in the request
     *
     * @throws IllegalArgumentException if any validation errors were found.
     */
    private void validateBusinessObjectDataCreateRequest(BusinessObjectDataCreateRequest request, boolean fileSizeRequired,
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity)
    {
        // Validate and trim the request parameters.
        request.setNamespace(alternateKeyHelper.validateStringParameter("namespace", request.getNamespace()));
        request.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", request.getBusinessObjectDefinitionName()));
        request
            .setBusinessObjectFormatUsage(alternateKeyHelper.validateStringParameter("business object format usage", request.getBusinessObjectFormatUsage()));
        request.setBusinessObjectFormatFileType(
            alternateKeyHelper.validateStringParameter("business object format file type", request.getBusinessObjectFormatFileType()));
        Assert.notNull(request.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
        request.setPartitionKey(alternateKeyHelper.validateStringParameter("partition key", request.getPartitionKey()));
        request.setPartitionValue(alternateKeyHelper.validateStringParameter("partition value", request.getPartitionValue()));
        businessObjectDataHelper.validateSubPartitionValues(request.getSubPartitionValues());

        Assert.isTrue(CollectionUtils.isNotEmpty(request.getStorageUnits()), "At least one storage unit must be specified.");
        for (StorageUnitCreateRequest storageUnit : request.getStorageUnits())
        {
            Assert.notNull(storageUnit, "A storage unit can't be null.");

            // Validate and trim the storage name.
            Assert.hasText(storageUnit.getStorageName(), "A storage name is required for each storage unit.");
            storageUnit.setStorageName(storageUnit.getStorageName().trim());

            if (BooleanUtils.isTrue(storageUnit.isDiscoverStorageFiles()))
            {
                // The auto-discovery of storage files is enabled, thus a storage directory is required and storage files cannot be specified.
                Assert.isTrue(storageUnit.getStorageDirectory() != null, "A storage directory must be specified when discovery of storage files is enabled.");
                Assert.isTrue(CollectionUtils.isEmpty(storageUnit.getStorageFiles()),
                    "Storage files cannot be specified when discovery of storage files is enabled.");
            }
            else if (!Boolean.TRUE.equals(businessObjectDataStatusEntity.getPreRegistrationStatus()))
            {
                // Since auto-discovery is disabled, a storage directory or at least one storage file are required for each storage unit.
                Assert.isTrue(storageUnit.getStorageDirectory() != null || CollectionUtils.isNotEmpty(storageUnit.getStorageFiles()),
                    "A storage directory or at least one storage file must be specified for each storage unit.");
            }

            // If storageDirectory element is present in the request, we require it to contain a non-empty directoryPath element.
            if (storageUnit.getStorageDirectory() != null)
            {
                Assert.hasText(storageUnit.getStorageDirectory().getDirectoryPath(), "A storage directory path must be specified.");
                storageUnit.getStorageDirectory().setDirectoryPath(storageUnit.getStorageDirectory().getDirectoryPath().trim());
            }

            if (CollectionUtils.isNotEmpty(storageUnit.getStorageFiles()))
            {
                for (StorageFile storageFile : storageUnit.getStorageFiles())
                {
                    Assert.hasText(storageFile.getFilePath(), "A file path must be specified.");
                    storageFile.setFilePath(storageFile.getFilePath().trim());

                    if (fileSizeRequired)
                    {
                        Assert.notNull(storageFile.getFileSizeBytes(), "A file size must be specified.");
                    }

                    // Ensure row count is not negative.
                    if (storageFile.getRowCount() != null)
                    {
                        Assert.isTrue(storageFile.getRowCount() >= 0, "File \"" + storageFile.getFilePath() + "\" has a row count which is < 0.");
                    }
                }
            }
        }

        // Validate and trim the parents' keys.
        validateBusinessObjectDataKeys(request.getBusinessObjectDataParents());

        // Validate attributes.
        attributeHelper.validateAttributes(request.getAttributes());
    }

    /**
     * Validates the business object data keys. This will validate, trim, and make lowercase appropriate fields.
     *
     * @param keys the business object data keys to validate
     */
    private void validateBusinessObjectDataKeys(List<BusinessObjectDataKey> keys)
    {
        // Create a cloned business object data keys list where all keys are lowercase. This will be used for ensuring no duplicates are present in a
        // case-insensitive way.
        List<BusinessObjectDataKey> businessObjectDataLowercaseKeys = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(keys))
        {
            for (BusinessObjectDataKey key : keys)
            {
                Assert.notNull(key, "A business object data key must be specified.");

                // Validate and trim the alternate key parameter values.
                key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
                key.setBusinessObjectDefinitionName(
                    alternateKeyHelper.validateStringParameter("business object definition name", key.getBusinessObjectDefinitionName()));
                key.setBusinessObjectFormatUsage(
                    alternateKeyHelper.validateStringParameter("business object format usage", key.getBusinessObjectFormatUsage()));
                key.setBusinessObjectFormatFileType(
                    alternateKeyHelper.validateStringParameter("business object format file type", key.getBusinessObjectFormatFileType()));
                Assert.notNull(key.getBusinessObjectFormatVersion(), "A business object format version must be specified.");
                key.setPartitionValue(alternateKeyHelper.validateStringParameter("partition value", key.getPartitionValue()));
                businessObjectDataHelper.validateSubPartitionValues(key.getSubPartitionValues());
                Assert.notNull(key.getBusinessObjectDataVersion(), "A business object data version must be specified.");

                // Add a lowercase clone to the lowercase key list.
                businessObjectDataLowercaseKeys.add(cloneToLowerCase(key));
            }
        }

        // Check for duplicates by ensuring the lowercase list size and the hash set (removes duplicates) size are the same.
        if (businessObjectDataLowercaseKeys.size() != new HashSet<>(businessObjectDataLowercaseKeys).size())
        {
            throw new IllegalArgumentException("Business object data keys can not contain duplicates.");
        }
    }
}
