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

import static org.finra.herd.service.helper.DdlGenerator.NON_PARTITIONED_TABLE_LOCATION_CUSTOM_DDL_TOKEN;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.StorageFileDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataPartitionsRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.Partition;
import org.finra.herd.model.api.xml.PartitionColumn;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.HivePartitionDto;
import org.finra.herd.model.dto.StorageUnitAvailabilityDto;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.CustomDdlEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;

/**
 * Helper methods used for both DDL generator and Partitions generator
 */
@Component
public class BusinessObjectDataDdlPartitionsHelper
{
    /**
     * The partition key value for business object data without partitioning.
     */
    public static final String NO_PARTITIONING_PARTITION_KEY = "partition";

    /**
     * The partition value for business object data without partitioning.
     */
    public static final String NO_PARTITIONING_PARTITION_VALUE = "none";

    /**
     * The regular expression that represents an empty partition in S3, this is because hadoop file system implements directory support in S3 by creating empty
     * files with the "directoryname_$folder$" suffix.
     */
    public static final String REGEX_S3_EMPTY_PARTITION = "_\\$folder\\$";

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private BusinessObjectDataStatusDaoHelper businessObjectDataStatusDaoHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private StoragePlatformHelper storagePlatformHelper;

    @Autowired
    private StorageUnitDao storageUnitDao;

    @Autowired
    private StorageFileDao storageFileDao;

    @Autowired
    private StorageFileHelper storageFileHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StorageUnitHelper storageUnitHelper;

    /**
     * Builds business object data DDL request object instance and initialise it with the business object data partitions request field values.
     *
     * @param request the business object data partitions request
     *
     * @return the newly created BusinessObjectDataDdlRequest object instance
     */
    public BusinessObjectDataDdlRequest buildBusinessObjectDataDdlRequest(BusinessObjectDataPartitionsRequest request)
    {
        BusinessObjectDataDdlRequest ddlRequest = new BusinessObjectDataDdlRequest();
        ddlRequest.setNamespace(request.getNamespace());
        ddlRequest.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName());
        ddlRequest.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage());
        ddlRequest.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType());
        ddlRequest.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());
        ddlRequest.setPartitionValueFilters(request.getPartitionValueFilters());
        ddlRequest.setPartitionValueFilter(null);
        ddlRequest.setBusinessObjectDataVersion(request.getBusinessObjectDataVersion());
        ddlRequest.setStorageNames(request.getStorageNames());
        ddlRequest.setStorageName(null);
        ddlRequest.setAllowMissingData(request.isAllowMissingData());
        ddlRequest.setIncludeAllRegisteredSubPartitions(request.isIncludeAllRegisteredSubPartitions());
        ddlRequest.setSuppressScanForUnregisteredSubPartitions(request.isSuppressScanForUnregisteredSubPartitions());
        return ddlRequest;
    }

    /**
     * Generates the create table Hive 13 DDL as per specified business object data DDL request.
     *
     * @param request the business object data DDL request
     * @param businessObjectFormatEntity the business object format entity
     * @param customDdlEntity the optional custom DDL entity
     * @param storageNames the list of storage names
     * @param requestedStorageEntities the list of storage entities per storage names specified in the request
     * @param cachedStorageEntities the map of storage names in upper case to the relative storage entities
     * @param cachedS3BucketNames the map of storage names in upper case to the relative S3 bucket names
     *
     * @return the generateDDL and generatePartitions Wrapper class object
     */
    protected GenerateDdlRequestWrapper buildGenerateDdlPartitionsWrapper(BusinessObjectDataDdlRequest request,
        BusinessObjectFormatEntity businessObjectFormatEntity, CustomDdlEntity customDdlEntity, List<String> storageNames,
        List<StorageEntity> requestedStorageEntities, Map<String, StorageEntity> cachedStorageEntities, Map<String, String> cachedS3BucketNames)
    {
        // Get business object format key from the request.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion());

        // Get storage platform entity for S3 storage platform type.
        StoragePlatformEntity s3StoragePlatformEntity = storagePlatformHelper.getStoragePlatformEntity(StoragePlatformEntity.S3);

        // Build partition filters based on the specified partition value filters.
        // We do validate that all specified storage entities are of "S3" storage platform type, so we specify S3 storage platform type in
        // the call below, so we select storage units only from all S3 storage, when the specified list of storage entities is empty.
        List<List<String>> partitionFilters = businessObjectDataDaoHelper
            .buildPartitionFilters(request.getPartitionValueFilters(), request.getPartitionValueFilter(), businessObjectFormatKey,
                request.getBusinessObjectDataVersion(), requestedStorageEntities, s3StoragePlatformEntity, null, businessObjectFormatEntity);

        // If the partitionKey="partition" and partitionValue="none", then DDL should
        // return a DDL which treats business object data as a table, not a partition.
        boolean isPartitioned = !businessObjectFormatEntity.getPartitionKey().equalsIgnoreCase(NO_PARTITIONING_PARTITION_KEY) || partitionFilters.size() != 1 ||
            !partitionFilters.get(0).get(0).equalsIgnoreCase(NO_PARTITIONING_PARTITION_VALUE);

        // Generate the create table Hive 13 DDL.
        BusinessObjectDataDdlPartitionsHelper.GenerateDdlRequestWrapper generateDdlRequest = getGenerateDdlRequestWrapperInstance();
        generateDdlRequest.allowMissingData = request.isAllowMissingData();
        generateDdlRequest.businessObjectDataVersion = request.getBusinessObjectDataVersion();
        generateDdlRequest.businessObjectFormatEntity = businessObjectFormatEntity;
        generateDdlRequest.businessObjectFormatVersion = request.getBusinessObjectFormatVersion();
        generateDdlRequest.customDdlEntity = customDdlEntity;
        generateDdlRequest.includeAllRegisteredSubPartitions = request.isIncludeAllRegisteredSubPartitions();
        generateDdlRequest.includeDropPartitions = request.isIncludeDropPartitions();
        generateDdlRequest.includeDropTableStatement = request.isIncludeDropTableStatement();
        generateDdlRequest.includeIfNotExistsOption = request.isIncludeIfNotExistsOption();
        generateDdlRequest.isPartitioned = isPartitioned;
        generateDdlRequest.partitionFilters = partitionFilters;
        generateDdlRequest.cachedS3BucketNames = cachedS3BucketNames;
        generateDdlRequest.cachedStorageEntities = cachedStorageEntities;
        generateDdlRequest.storageNames = storageNames;
        generateDdlRequest.requestedStorageEntities = requestedStorageEntities;
        generateDdlRequest.suppressScanForUnregisteredSubPartitions = request.isSuppressScanForUnregisteredSubPartitions();
        generateDdlRequest.combineMultiplePartitionsInSingleAlterTable = request.isCombineMultiplePartitionsInSingleAlterTable();
        generateDdlRequest.tableName = request.getTableName();

        // getOutputFormat == null, means it's used in generatePartitions request since getOutputFormat must not be null for generateDDL request
        if (request.getOutputFormat() == null)
        {
            generateDdlRequest.isGeneratePartitionsRequest = true;
            Assert.isTrue(isPartitioned, "Generate-partitions request does not support singleton partitions.");
        }
        else
        {
            generateDdlRequest.isGeneratePartitionsRequest = false;
        }
        return generateDdlRequest;
    }

    /**
     * Validate partionFilters and business format
     *
     * @param generateDdlRequest the generateDdlRequestWrapper object
     *
     * @return BusinessObjectFormat object
     */
    public BusinessObjectFormat validatePartitionFiltersAndFormat(GenerateDdlRequestWrapper generateDdlRequest)
    {
        // Validate that partition values passed in the list of partition filters do not contain '/' character.
        if (generateDdlRequest.isPartitioned && !CollectionUtils.isEmpty(generateDdlRequest.partitionFilters))
        {
            // Validate that partition values do not contain '/' characters.
            for (List<String> partitionFilter : generateDdlRequest.partitionFilters)
            {
                for (String partitionValue : partitionFilter)
                {
                    Assert.doesNotContain(partitionValue, "/", String.format("Partition value \"%s\" can not contain a '/' character.", partitionValue));
                }
            }
        }

        // Get business object format model object to directly access schema columns and partitions.
        BusinessObjectFormat businessObjectFormat =
            businessObjectFormatHelper.createBusinessObjectFormatFromEntity(generateDdlRequest.businessObjectFormatEntity);

        // Validate that we have at least one column specified in the business object format schema.
        assertSchemaColumnsNotEmpty(businessObjectFormat, generateDdlRequest.businessObjectFormatEntity);

        if (generateDdlRequest.isPartitioned)
        {
            // Validate that we have at least one partition column specified in the business object format schema.
            Assert.notEmpty(businessObjectFormat.getSchema().getPartitions(), String.format("No schema partitions specified for business object format {%s}.",
                businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(generateDdlRequest.businessObjectFormatEntity)));

            // Validate that partition column names do not contain '/' characters.
            for (SchemaColumn partitionColumn : businessObjectFormat.getSchema().getPartitions())
            {
                Assert.doesNotContain(partitionColumn.getName(), "/", String
                    .format("Partition column name \"%s\" can not contain a '/' character. Business object format: {%s}", partitionColumn.getName(),
                        businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(generateDdlRequest.businessObjectFormatEntity)));
            }
        }
        return businessObjectFormat;
    }

    /**
     * Process partitionFilters to retrieve storage unit availability dtos
     *
     * @param generateDdlRequest the generateDdlRequestWrapper object
     *
     * @return the list of storage unit availability dtos
     */
    public List<StorageUnitAvailabilityDto> processPartitionFiltersForGenerateDdlPartitions(GenerateDdlRequestWrapper generateDdlRequest)
    {
        // Get the business object format key from the entity.
        BusinessObjectFormatKey businessObjectFormatKey = businessObjectFormatHelper.getBusinessObjectFormatKey(generateDdlRequest.businessObjectFormatEntity);

        // Override the business object format version with the original (optional) value from the request.
        businessObjectFormatKey.setBusinessObjectFormatVersion(generateDdlRequest.businessObjectFormatVersion);

        // Get business object data status entity for the VALID status.
        BusinessObjectDataStatusEntity validBusinessObjectDataStatusEntity =
            businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(BusinessObjectDataStatusEntity.VALID);

        // Get storage platform entity for S3 storage platform type.
        StoragePlatformEntity s3StoragePlatformEntity = storagePlatformHelper.getStoragePlatformEntity(StoragePlatformEntity.S3);

        // Retrieve a list of storage unit availability DTOs for the specified list of partition filters. The list will be sorted by partition values and
        // storage names. For a non-partitioned table, there should only exist a single business object data entity (with partitionValue equals to "none").
        // We do validate that all specified storage entities are of "S3" storage platform type, so we specify S3 storage platform type in the herdDao call
        // below, so we select storage units only from all S3 storage entities, when the specified list of storage names is empty. We also specify to select
        // only "available" storage units.
        List<StorageUnitAvailabilityDto> storageUnitAvailabilityDtos = storageUnitDao
            .getStorageUnitsByPartitionFilters(generateDdlRequest.businessObjectFormatEntity.getBusinessObjectDefinition(),
                businessObjectFormatKey.getBusinessObjectFormatUsage(), generateDdlRequest.businessObjectFormatEntity.getFileType(),
                businessObjectFormatKey.getBusinessObjectFormatVersion(), generateDdlRequest.partitionFilters, generateDdlRequest.businessObjectDataVersion,
                validBusinessObjectDataStatusEntity, generateDdlRequest.requestedStorageEntities, s3StoragePlatformEntity, null, true);

        // Exclude duplicate business object data per specified list of storage names.
        // If storage names are not specified, the method fails on business object data instances registered with multiple storage.
        storageUnitAvailabilityDtos = excludeDuplicateBusinessObjectData(storageUnitAvailabilityDtos, generateDdlRequest.storageNames);

        // Build a list of matched partition filters. Please note that each request partition
        // filter might result in multiple available business object data entities.
        List<List<String>> matchedAvailablePartitionFilters = new ArrayList<>();
        List<List<String>> availablePartitions = new ArrayList<>();
        for (StorageUnitAvailabilityDto storageUnitAvailabilityDto : storageUnitAvailabilityDtos)
        {
            BusinessObjectDataKey businessObjectDataKey = storageUnitAvailabilityDto.getBusinessObjectDataKey();
            matchedAvailablePartitionFilters
                .add(businessObjectDataHelper.getPartitionFilter(businessObjectDataKey, generateDdlRequest.partitionFilters.get(0)));
            availablePartitions.add(businessObjectDataHelper.getPrimaryAndSubPartitionValues(businessObjectDataKey));
        }

        // If request specifies to include all registered sub-partitions, fail if any of "non-available" registered sub-partitions are found.
        if (generateDdlRequest.businessObjectDataVersion == null && BooleanUtils.isTrue(generateDdlRequest.includeAllRegisteredSubPartitions) &&
            !CollectionUtils.isEmpty(matchedAvailablePartitionFilters))
        {
            notAllowNonAvailableRegisteredSubPartitions(generateDdlRequest.businessObjectFormatEntity.getBusinessObjectDefinition(),
                businessObjectFormatKey.getBusinessObjectFormatUsage(), generateDdlRequest.businessObjectFormatEntity.getFileType(),
                businessObjectFormatKey.getBusinessObjectFormatVersion(), matchedAvailablePartitionFilters, availablePartitions,
                generateDdlRequest.storageNames, generateDdlRequest.requestedStorageEntities, s3StoragePlatformEntity);
        }

        // Fail on any missing business object data unless the flag is set to allow missing business object data.
        if (!BooleanUtils.isTrue(generateDdlRequest.allowMissingData))
        {
            // Get a list of unmatched partition filters.
            List<List<String>> unmatchedPartitionFilters = new ArrayList<>(generateDdlRequest.partitionFilters);
            unmatchedPartitionFilters.removeAll(matchedAvailablePartitionFilters);

            // Throw an exception if we have any unmatched partition filters.
            if (!unmatchedPartitionFilters.isEmpty())
            {
                // Get the first unmatched partition filter and throw exception.
                List<String> unmatchedPartitionFilter = getFirstUnmatchedPartitionFilter(unmatchedPartitionFilters);
                throw new ObjectNotFoundException(String.format(
                    "Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                        "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, partitionValue: \"%s\", " +
                        "subpartitionValues: \"%s\", businessObjectDataVersion: %d} is not available in \"%s\" storage(s).",
                    businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
                    businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(),
                    businessObjectFormatKey.getBusinessObjectFormatVersion(), unmatchedPartitionFilter.get(0),
                    StringUtils.join(unmatchedPartitionFilter.subList(1, unmatchedPartitionFilter.size()), ","), generateDdlRequest.businessObjectDataVersion,
                    StringUtils.join(generateDdlRequest.storageNames, ",")));
            }
        }
        return storageUnitAvailabilityDtos;
    }

    /**
     * Adds the relative "alter table add partition" statements for each storage unit entity. Please note that each request partition value might result in
     * multiple available storage unit entities (subpartitions).
     *
     * @param generateDdlRequest the generate ddl request wrapper object
     * @param sb the string builder to be updated with the "alter table add partition" statements
     * @param partitions the list to store business object data partitions
     * @param replacements the hash map of string values to be used to substitute the custom DDL tokens with their actual values
     * @param businessObjectFormatForSchema the business object format to be used for schema
     * @param ifNotExistsOption specifies if generated DDL contains "if not exists" option
     * @param storageUnitAvailabilityDtos the list of storage unit availability DTOs
     */
    public void processStorageUnitsForGenerateDdlPartitions(GenerateDdlRequestWrapper generateDdlRequest, StringBuilder sb, List<Partition> partitions,
        HashMap<String, String> replacements, BusinessObjectFormat businessObjectFormatForSchema, String ifNotExistsOption,
        List<StorageUnitAvailabilityDto> storageUnitAvailabilityDtos)
    {
        // If flag is not set to suppress scan for unregistered sub-partitions, retrieve all storage
        // file paths for the relative storage units loaded in a multi-valued map for easy access.
        MultiValuedMap<Integer, String> storageUnitIdToStorageFilePathsMap =
            BooleanUtils.isTrue(generateDdlRequest.suppressScanForUnregisteredSubPartitions) ? new ArrayListValuedHashMap<>() :
                storageFileDao.getStorageFilePathsByStorageUnitIds(storageUnitHelper.getStorageUnitIds(storageUnitAvailabilityDtos));

        // Crete a map of storage names in upper case to their relative S3 key prefix velocity templates.
        Map<String, String> s3KeyPrefixVelocityTemplates = new HashMap<>();

        // Crete a map of business object format keys to their relative business object format instances.
        Map<BusinessObjectFormatKey, BusinessObjectFormat> businessObjectFormats = new HashMap<>();

        // Get data provider for the business object definition.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(
            new BusinessObjectDefinitionKey(businessObjectFormatForSchema.getNamespace(), businessObjectFormatForSchema.getBusinessObjectDefinitionName()));
        String dataProviderName = businessObjectDefinitionEntity.getDataProvider().getName();

        // Generate the beginning of the alter table statement.
        String alterTableFirstToken = null;
        if (!generateDdlRequest.isGeneratePartitionsRequest)
        {
            alterTableFirstToken = String.format("ALTER TABLE `%s` ADD %s", generateDdlRequest.tableName, ifNotExistsOption).trim();
        }

        // Process all available business object data instances.
        List<String> addPartitionStatements = new ArrayList<>();
        for (StorageUnitAvailabilityDto storageUnitAvailabilityDto : storageUnitAvailabilityDtos)
        {
            // Get storage name in upper case for this storage unit.
            String upperCaseStorageName = storageUnitAvailabilityDto.getStorageName().toUpperCase();

            // Get storage entity for this storage unit.
            StorageEntity storageEntity = getStorageEntity(upperCaseStorageName, generateDdlRequest.cachedStorageEntities);

            // Get business object data key for this business object data.
            BusinessObjectDataKey businessObjectDataKey = storageUnitAvailabilityDto.getBusinessObjectDataKey();

            // Get business object format key for this business object data.
            BusinessObjectFormatKey businessObjectFormatKey = businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectDataKey);

            // Retrieve s3 key prefix velocity template for this storage.
            String s3KeyPrefixVelocityTemplate = getS3KeyPrefixVelocityTemplate(upperCaseStorageName, storageEntity, s3KeyPrefixVelocityTemplates);

            // Retrieve business object format for this business object data.
            BusinessObjectFormat businessObjectFormat = getBusinessObjectFormat(businessObjectFormatKey, businessObjectFormats);

            // Build the expected S3 key prefix for this storage unit.
            String s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(s3KeyPrefixVelocityTemplate, dataProviderName, businessObjectFormat, businessObjectDataKey,
                storageUnitAvailabilityDto.getStorageName());

            // If flag is set to suppress scan for unregistered sub-partitions, use the directory path or the S3 key prefix
            // as the partition's location, otherwise, use storage files to discover all unregistered sub-partitions.
            Collection<String> storageFilePaths = new ArrayList<>();
            if (BooleanUtils.isTrue(generateDdlRequest.suppressScanForUnregisteredSubPartitions))
            {
                // Validate the directory path value if it is present.
                if (storageUnitAvailabilityDto.getStorageUnitDirectoryPath() != null)
                {
                    Assert.isTrue(storageUnitAvailabilityDto.getStorageUnitDirectoryPath().equals(s3KeyPrefix), String.format(
                        "Storage directory path \"%s\" registered with business object data {%s} " +
                            "in \"%s\" storage does not match the expected S3 key prefix \"%s\".", storageUnitAvailabilityDto.getStorageUnitDirectoryPath(),
                        businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), storageUnitAvailabilityDto.getStorageName(),
                        s3KeyPrefix));
                }

                // Add the S3 key prefix to the list of storage files.
                // We add a trailing '/' character to the prefix, since it represents a directory.
                storageFilePaths.add(StringUtils.appendIfMissing(s3KeyPrefix, "/"));
            }
            else
            {
                // Retrieve storage file paths registered with this business object data in the specified storage.
                storageFilePaths = storageUnitIdToStorageFilePathsMap.containsKey(storageUnitAvailabilityDto.getStorageUnitId()) ?
                    storageUnitIdToStorageFilePathsMap.get(storageUnitAvailabilityDto.getStorageUnitId()) : new ArrayList<>();

                // Validate storage file paths registered with this business object data in the specified storage.
                // The validation check below is required even if we have no storage files registered.
                storageFileHelper.validateStorageFilePaths(storageFilePaths, s3KeyPrefix, businessObjectDataKey, storageUnitAvailabilityDto.getStorageName());

                // If there are no storage files registered for this storage unit, we should use the storage directory path value.
                if (storageFilePaths.isEmpty())
                {
                    // Validate that directory path value is present and it matches the S3 key prefix.
                    Assert.isTrue(storageUnitAvailabilityDto.getStorageUnitDirectoryPath() != null &&
                        storageUnitAvailabilityDto.getStorageUnitDirectoryPath().startsWith(s3KeyPrefix), String.format(
                        "Storage directory path \"%s\" registered with business object data {%s} " +
                            "in \"%s\" storage does not match the expected S3 key prefix \"%s\".", storageUnitAvailabilityDto.getStorageUnitDirectoryPath(),
                        businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), storageUnitAvailabilityDto.getStorageName(),
                        s3KeyPrefix));
                    // Add storage directory path the empty storage files list.
                    // We add a trailing '/' character to the path, since it represents a directory.
                    storageFilePaths.add(storageUnitAvailabilityDto.getStorageUnitDirectoryPath() + "/");
                }
            }

            // Retrieve the s3 bucket name.
            String s3BucketName = getS3BucketName(upperCaseStorageName, storageEntity, generateDdlRequest.cachedS3BucketNames);

            // For partitioned table, add the relative partitions to the generated DDL.
            if (generateDdlRequest.isPartitioned)
            {
                // If flag is set to suppress scan for unregistered sub-partitions, validate that the number of primary and sub-partition values specified for
                // the business object data equals to the number of partition columns defined in schema for the format selected for DDL/Partitions generation.
                if (BooleanUtils.isTrue(generateDdlRequest.suppressScanForUnregisteredSubPartitions))
                {
                    int businessObjectDataRegisteredPartitions = 1 + CollectionUtils.size(businessObjectDataKey.getSubPartitionValues());
                    Assert.isTrue(businessObjectFormatForSchema.getSchema().getPartitions().size() == businessObjectDataRegisteredPartitions, String.format(
                        "Number of primary and sub-partition values (%d) specified for the business object data is not equal to the number of partition columns"
                            + " (%d) defined in the schema of the business object format selected for DDL/Partitions generation. " +
                            "Business object data: {%s},  business object format: {%s}", businessObjectDataRegisteredPartitions,
                        businessObjectFormatForSchema.getSchema().getPartitions().size(),
                        businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), businessObjectFormatHelper
                            .businessObjectFormatKeyToString(businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormatForSchema))));
                }
                // Otherwise, since the format version selected for DDL/Partitions generation might not match the relative business object format version that
                // business bject data is registered against, validate that the number of sub-partition values specified for the business object data is less
                // than the number of partition columns defined in schema for the format selected for DDL/Partitions generation.
                else
                {
                    Assert.isTrue(
                        businessObjectFormatForSchema.getSchema().getPartitions().size() > CollectionUtils.size(businessObjectDataKey.getSubPartitionValues()),
                        String.format("Number of subpartition values specified for the business object data is greater than or equal to " +
                                "the number of partition columns defined in the schema of the business object format selected for DDL/Partitions generation. " +
                                "Business object data: {%s},  business object format: {%s}",
                            businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), businessObjectFormatHelper
                                .businessObjectFormatKeyToString(businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormatForSchema))));
                }

                // Get partition information. For multiple level partitioning, auto-discover subpartitions (subdirectories) not already included into the S3 key
                // prefix. Each discovered partition requires a standalone "add partition" clause. Please note that due to the above validation check, there
                // should be no auto discoverable sub-partition columns, when flag is set to suppress scan for unregistered sub-partitions.
                List<SchemaColumn> autoDiscoverableSubPartitionColumns = businessObjectFormatForSchema.getSchema().getPartitions()
                    .subList(1 + CollectionUtils.size(businessObjectDataKey.getSubPartitionValues()),
                        businessObjectFormatForSchema.getSchema().getPartitions().size());

                // Get and process Hive partitions.
                for (HivePartitionDto hivePartition : getHivePartitions(businessObjectDataKey, autoDiscoverableSubPartitionColumns, s3KeyPrefix,
                    storageFilePaths, storageUnitAvailabilityDto.getStorageName()))
                {
                    if (!generateDdlRequest.isGeneratePartitionsRequest)
                    {
                        // Build an add partition statement for this hive partition.
                        StringBuilder addPartitionStatement = new StringBuilder();
                        addPartitionStatement.append(String.format("%s PARTITION (",
                            BooleanUtils.isTrue(generateDdlRequest.combineMultiplePartitionsInSingleAlterTable) ? "   " : alterTableFirstToken));
                        // Specify all partition column values.
                        List<String> partitionKeyValuePairs = new ArrayList<>();
                        for (int i = 0; i < businessObjectFormatForSchema.getSchema().getPartitions().size(); i++)
                        {
                            String partitionColumnName = businessObjectFormatForSchema.getSchema().getPartitions().get(i).getName();
                            String partitionValue = hivePartition.getPartitionValues().get(i);
                            partitionKeyValuePairs.add(String.format("`%s`='%s'", partitionColumnName, partitionValue));
                        }
                        addPartitionStatement.append(StringUtils.join(partitionKeyValuePairs, ", "));
                        addPartitionStatement.append(String.format(") LOCATION 's3n://%s/%s%s'", s3BucketName, s3KeyPrefix,
                            StringUtils.isNotBlank(hivePartition.getPath()) ? hivePartition.getPath() : ""));

                        // Add this add partition statement to the list.
                        addPartitionStatements.add(addPartitionStatement.toString());
                    }
                    else
                    {
                        // Specify all partition column values.
                        Partition partition = new Partition();
                        List<PartitionColumn> partitionColumns = new ArrayList<>();
                        partition.setPartitionColumns(partitionColumns);
                        for (int i = 0; i < businessObjectFormatForSchema.getSchema().getPartitions().size(); i++)
                        {
                            String partitionColumnName = businessObjectFormatForSchema.getSchema().getPartitions().get(i).getName();
                            String partitionValue = hivePartition.getPartitionValues().get(i);
                            partitionColumns.add(new PartitionColumn(partitionColumnName, partitionValue));
                        }
                        partition.setPartitionLocation(String
                            .format("%s/%s%s", s3BucketName, s3KeyPrefix, StringUtils.isNotBlank(hivePartition.getPath()) ? hivePartition.getPath() : ""));
                        partitions.add(partition);
                    }
                }
            }
            else // This is a non-partitioned table.
            {
                // Get location for this non-partitioned table.
                String tableLocation = String.format("s3n://%s/%s", s3BucketName, s3KeyPrefix);

                if (generateDdlRequest.customDdlEntity == null)
                {
                    // Since custom DDL was not specified and this table is not partitioned, add a LOCATION clause.
                    // This is the last line in the non-partitioned table DDL.
                    sb.append(String.format("LOCATION '%s';", tableLocation));
                }
                else
                {
                    // Since custom DDL was used for a non-partitioned table, substitute the relative custom DDL token with the actual table location.
                    replacements.put(NON_PARTITIONED_TABLE_LOCATION_CUSTOM_DDL_TOKEN, tableLocation);
                }
            }
        }

        // Add all add partition statements to the main string builder.
        if (CollectionUtils.isNotEmpty(addPartitionStatements))
        {
            // If specified, combine adding multiple partitions in a single ALTER TABLE statement.
            if (BooleanUtils.isTrue(generateDdlRequest.combineMultiplePartitionsInSingleAlterTable))
            {
                sb.append(alterTableFirstToken).append('\n');
            }

            sb.append(
                StringUtils.join(addPartitionStatements, BooleanUtils.isTrue(generateDdlRequest.combineMultiplePartitionsInSingleAlterTable) ? ",\n" : ";\n"))
                .append(";\n");
        }
    }

    /**
     * Asserts that there exists at least one column specified in the business object format schema.
     *
     * @param businessObjectFormat The {@link BusinessObjectFormat} containing schema columns.
     * @param businessObjectFormatEntity The entity used to generate the error message.
     */

    private void assertSchemaColumnsNotEmpty(BusinessObjectFormat businessObjectFormat, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        Assert.notEmpty(businessObjectFormat.getSchema().getColumns(), String.format("No schema columns specified for business object format {%s}.",
            businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
    }

    /**
     * Eliminate storage units that belong to the same business object data by picking storage unit registered in a storage listed earlier in the list of
     * storage names specified in the request. If storage names are not specified, simply fail on business object data instances registered with multiple
     * storage.
     *
     * @param storageUnitAvailabilityDtos the list of storage unit availability DTOs
     * @param storageNames the list of storage names
     *
     * @return the updated list of storage unit availability DTOs
     * @throws IllegalArgumentException on business object data being registered in multiple storage and storage names are not specified to resolve this
     */
    protected List<StorageUnitAvailabilityDto> excludeDuplicateBusinessObjectData(List<StorageUnitAvailabilityDto> storageUnitAvailabilityDtos,
        List<String> storageNames) throws IllegalArgumentException
    {
        // Convert the list of storage names to upper case.
        List<String> upperCaseStorageNames = new ArrayList(storageNames);
        upperCaseStorageNames.replaceAll(String::toUpperCase);

        // If storage names are not specified, fail on business object data instance registered with multiple storage.
        // Otherwise, in a case when the same business object data is registered with multiple storage,
        // pick storage unit registered in a storage listed earlier in the list of storage names specified in the request.
        Map<BusinessObjectDataKey, StorageUnitAvailabilityDto> businessObjectDataToStorageUnitMap = new LinkedHashMap<>();
        for (StorageUnitAvailabilityDto storageUnitAvailabilityDto : storageUnitAvailabilityDtos)
        {
            BusinessObjectDataKey businessObjectDataKey = storageUnitAvailabilityDto.getBusinessObjectDataKey();

            if (businessObjectDataToStorageUnitMap.containsKey(businessObjectDataKey))
            {
                // Duplicate business object data is found, so check if storage names are specified.
                if (CollectionUtils.isEmpty(upperCaseStorageNames))
                {
                    // Fail on business object data registered in multiple storage.
                    throw new IllegalArgumentException(String.format("Found business object data registered in more than one storage. " +
                            "Please specify storage(s) in the request to resolve this. Business object data {%s}",
                        businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
                }
                else
                {
                    // Replace the storage unit entity if it belongs to a "higher priority" storage.
                    String currentUpperCaseStorageName = businessObjectDataToStorageUnitMap.get(businessObjectDataKey).getStorageName().toUpperCase();
                    int currentStorageIndex = upperCaseStorageNames.indexOf(currentUpperCaseStorageName);
                    int newStorageIndex = upperCaseStorageNames.indexOf(storageUnitAvailabilityDto.getStorageName().toUpperCase());
                    if (newStorageIndex < currentStorageIndex)
                    {
                        businessObjectDataToStorageUnitMap.put(businessObjectDataKey, storageUnitAvailabilityDto);
                    }
                }
            }
            else
            {
                businessObjectDataToStorageUnitMap.put(businessObjectDataKey, storageUnitAvailabilityDto);
            }
        }

        return new ArrayList<>(businessObjectDataToStorageUnitMap.values());
    }

    /**
     * Searches for and fails on any of "non-available" registered sub-partitions as per list of "matched" partition filters.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param businessObjectFormatUsage the business object format usage (case-insensitive)
     * @param fileTypeEntity the file type entity
     * @param businessObjectFormatVersion the optional business object format version. If a business object format version isn't specified, the latest available
     * format version for each partition value will be used
     * @param matchedAvailablePartitionFilters the list of "matched" partition filters
     * @param availablePartitions the list of already discovered "available" partitions, where each partition consists of primary and optional sub-partition
     * values
     * @param storageNames the list of storage names
     * @param storageEntities the list of storage entities
     * @param s3StoragePlatformEntity the S3 storage platform entity
     */
    protected void notAllowNonAvailableRegisteredSubPartitions(BusinessObjectDefinitionEntity businessObjectDefinitionEntity, String businessObjectFormatUsage,
        FileTypeEntity fileTypeEntity, Integer businessObjectFormatVersion, List<List<String>> matchedAvailablePartitionFilters,
        List<List<String>> availablePartitions, List<String> storageNames, List<StorageEntity> storageEntities, StoragePlatformEntity s3StoragePlatformEntity)
    {
        // Query all matched partition filters to discover any non-available registered sub-partitions. Retrieve latest business object data per list of
        // matched filters regardless of business object data and/or storage unit statuses. This is done to discover all registered sub-partitions regardless
        // of business object data or storage unit statuses. We do validate that all specified storage entities are of "S3" storage platform type, so we
        // specify S3 storage platform in the herdDao call below to select storage units only from S3 storage (when the list of storage names is empty).
        // We want to select any existing storage units regardless of their status, so we pass "false" for selectOnlyAvailableStorageUnits parameter.
        List<StorageUnitAvailabilityDto> matchedNotAvailableStorageUnitAvailabilityDtos = storageUnitDao
            .getStorageUnitsByPartitionFilters(businessObjectDefinitionEntity, businessObjectFormatUsage, fileTypeEntity, businessObjectFormatVersion,
                matchedAvailablePartitionFilters, null, null, storageEntities, s3StoragePlatformEntity, null, false);

        // Exclude all storage units with business object data having "DELETED" status.
        matchedNotAvailableStorageUnitAvailabilityDtos =
            storageUnitHelper.excludeBusinessObjectDataStatus(matchedNotAvailableStorageUnitAvailabilityDtos, BusinessObjectDataStatusEntity.DELETED);

        // Exclude all already discovered "available" partitions. Please note that, since we got here, the list of matched partitions can not be empty.
        matchedNotAvailableStorageUnitAvailabilityDtos =
            storageUnitHelper.excludePartitions(matchedNotAvailableStorageUnitAvailabilityDtos, availablePartitions);

        // Fail on any "non-available" registered sub-partitions.
        if (!CollectionUtils.isEmpty(matchedNotAvailableStorageUnitAvailabilityDtos))
        {
            // Get the business object data key for the first "non-available" registered sub-partition.
            BusinessObjectDataKey businessObjectDataKey = matchedNotAvailableStorageUnitAvailabilityDtos.get(0).getBusinessObjectDataKey();

            // Throw an exception.
            throw new ObjectNotFoundException(String.format(
                "Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, partitionValue: \"%s\", " +
                    "subpartitionValues: \"%s\", businessObjectDataVersion: %d} is not available in \"%s\" storage(s).",
                businessObjectDefinitionEntity.getNamespace().getCode(), businessObjectDefinitionEntity.getName(), businessObjectFormatUsage,
                fileTypeEntity.getCode(), businessObjectFormatVersion, businessObjectDataKey.getPartitionValue(),
                StringUtils.join(businessObjectDataKey.getSubPartitionValues(), ","), businessObjectDataKey.getBusinessObjectDataVersion(),
                StringUtils.join(storageNames, ",")));
        }
    }

    /**
     * Gets a first unmatched partition filter from the list of unmatched filters.
     *
     * @param unmatchedPartitionFilters the list of unmatchedPartitionFilters
     *
     * @return the first unmatched partition filter
     */
    private List<String> getFirstUnmatchedPartitionFilter(List<List<String>> unmatchedPartitionFilters)
    {
        // Get the first unmatched partition filter from the list.
        List<String> unmatchedPartitionFilter = unmatchedPartitionFilters.get(0);

        // Replace all null partition values with an empty string.
        for (int i = 0; i < unmatchedPartitionFilter.size(); i++)
        {
            if (unmatchedPartitionFilter.get(i) == null)
            {
                unmatchedPartitionFilter.set(i, "");
            }
        }

        return unmatchedPartitionFilter;
    }

    /**
     * Gets a storage entity per specified storage name. The method memorizes the responses for performance reasons.
     *
     * @param upperCaseStorageName the storage name in upper case
     * @param storageEntities the map of storage names in upper case to their relative storage entities
     *
     * @return the storage entity
     */
    private StorageEntity getStorageEntity(String upperCaseStorageName, Map<String, StorageEntity> storageEntities)
    {
        StorageEntity storageEntity;

        // If storage entity was already retrieved, use it. Otherwise, retrieve and store it in the map.
        if (storageEntities.containsKey(upperCaseStorageName))
        {
            storageEntity = storageEntities.get(upperCaseStorageName);
        }
        else
        {
            storageEntity = storageDaoHelper.getStorageEntity(upperCaseStorageName);
            storageEntities.put(upperCaseStorageName, storageEntity);
        }

        return storageEntity;
    }

    /**
     * Gets S3 key prefix velocity template for the specified storage entity. The method memorizes the responses for performance reasons.
     *
     * @param upperCaseStorageName the storage name in upper case
     * @param storageEntity the storage entity
     * @param s3KeyPrefixVelocityTemplates the map of storage names in upper case to their relative S3 key prefix velocity templates
     *
     * @return the S3 key prefix velocity template
     */
    private String getS3KeyPrefixVelocityTemplate(String upperCaseStorageName, StorageEntity storageEntity, Map<String, String> s3KeyPrefixVelocityTemplates)
    {
        String s3KeyPrefixVelocityTemplate;

        // If S3 key prefix velocity template was already retrieved for this storage, use it.
        if (s3KeyPrefixVelocityTemplates.containsKey(upperCaseStorageName))
        {
            s3KeyPrefixVelocityTemplate = s3KeyPrefixVelocityTemplates.get(upperCaseStorageName);
        }
        // Otherwise, retrieve the S3 3 key prefix velocity template attribute value and store it in memory.
        else
        {
            // Retrieve S3 key prefix velocity template storage attribute value and store it in memory.
            s3KeyPrefixVelocityTemplate = storageHelper
                .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    storageEntity, false);

            // Validate that S3 key prefix velocity template is configured.
            Assert.isTrue(StringUtils.isNotBlank(s3KeyPrefixVelocityTemplate),
                String.format("Storage \"%s\" has no S3 key prefix velocity template configured.", storageEntity.getName()));

            // Store the retrieved value in memory.
            s3KeyPrefixVelocityTemplates.put(upperCaseStorageName, s3KeyPrefixVelocityTemplate);
        }

        return s3KeyPrefixVelocityTemplate;
    }

    /**
     * Gets a business object format for the specified business  object format key. The method memorizes the responses for performance reasons.
     *
     * @param businessObjectFormatKey the business object format key
     * @param businessObjectFormats the map of business object keys to their relative business object format instances
     *
     * @return the business object format
     */
    private BusinessObjectFormat getBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey,
        Map<BusinessObjectFormatKey, BusinessObjectFormat> businessObjectFormats)
    {
        BusinessObjectFormat businessObjectFormat;

        // If business object format was already retrieved, use it. Otherwise, retrieve and store it in the map.
        if (businessObjectFormats.containsKey(businessObjectFormatKey))
        {
            businessObjectFormat = businessObjectFormats.get(businessObjectFormatKey);
        }
        else
        {
            BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);
            businessObjectFormat = businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
            businessObjectFormats.put(businessObjectFormatKey, businessObjectFormat);
        }

        return businessObjectFormat;
    }

    /**
     * Gets an S3 bucket name for the specified storage entity. The method memorizes the responses for performance reasons.
     *
     * @param upperCaseStorageName the storage name in upper case
     * @param storageEntity the storage entity
     * @param s3BucketNames the map of storage names in upper case to their relative S3 bucket names
     *
     * @return the S3 bucket name
     */
    private String getS3BucketName(String upperCaseStorageName, StorageEntity storageEntity, Map<String, String> s3BucketNames)
    {
        String s3BucketName;

        // If bucket name was already retrieved for this storage, use it.
        if (s3BucketNames.containsKey(upperCaseStorageName))
        {
            s3BucketName = s3BucketNames.get(upperCaseStorageName);
        }
        // Otherwise, retrieve the S3 bucket name attribute value and store it in memory. Please note that it is required, so we pass in a "true" flag.
        else
        {
            s3BucketName = storageHelper
                .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageEntity, true);
            s3BucketNames.put(upperCaseStorageName, s3BucketName);
        }

        return s3BucketName;
    }

    /**
     * Gets a list of Hive partitions. For single level partitioning, no auto-discovery of sub-partitions (sub-directories) is needed - the business object data
     * will be represented by a single Hive partition instance. For multiple level partitioning, this method performs an auto-discovery of all sub-partitions
     * (sub-directories) and creates a Hive partition object instance for each partition.
     *
     * @param businessObjectDataKey the business object data key
     * @param autoDiscoverableSubPartitionColumns the auto-discoverable sub-partition columns
     * @param s3KeyPrefix the S3 key prefix
     * @param storageFiles the storage files
     * @param storageName the storage name
     *
     * @return the list of Hive partitions
     */
    public List<HivePartitionDto> getHivePartitions(BusinessObjectDataKey businessObjectDataKey, List<SchemaColumn> autoDiscoverableSubPartitionColumns,
        String s3KeyPrefix, Collection<String> storageFiles, String storageName)
    {
        // We are using linked hash map to preserve the order of the discovered partitions.
        Map<List<String>, HivePartitionDto> linkedHashMap = new LinkedHashMap<>();

        Pattern pattern = getHivePathPattern(autoDiscoverableSubPartitionColumns);
        for (String storageFile : storageFiles)
        {
            // Remove S3 key prefix from the file path. Please note that the storage files are already validated to start with S3 key prefix.
            String relativeFilePath = storageFile.substring(s3KeyPrefix.length());

            // Try to match the relative file path to the expected subpartition folders.
            Matcher matcher = pattern.matcher(relativeFilePath);
            Assert.isTrue(matcher.matches(), String.format("Registered storage file or directory does not match the expected Hive sub-directory pattern. " +
                    "Storage: {%s}, file/directory: {%s}, business object data: {%s}, S3 key prefix: {%s}, pattern: {%s}", storageName, storageFile,
                businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), s3KeyPrefix, pattern.pattern()));

            // Create a list of partition values.
            List<String> partitionValues = new ArrayList<>();

            // Add partition values per business object data key.
            partitionValues.add(businessObjectDataKey.getPartitionValue());
            partitionValues.addAll(businessObjectDataKey.getSubPartitionValues());

            // Extract relative partition values.
            for (int i = 1; i <= matcher.groupCount() - 1; i++)
            {
                partitionValues.add(matcher.group(i));
            }

            // Get the matched trailing folder markers and an optional file name.
            String partitionPathTrailingPart = matcher.group(matcher.groupCount());

            // If we did not match all expected partition values along with the trailing folder markers and an optional file name, then this storage file
            // path is not part of a fully qualified partition (this is an intermediate folder marker) and we ignore it.
            if (!partitionValues.contains(null))
            {
                // Get path for this partition by removing trailing "/" followed by an optional file name or "_$folder$" which represents an empty folder in S3.
                String partitionPath = relativeFilePath.substring(0, relativeFilePath.length() - StringUtils.length(partitionPathTrailingPart));

                // Check if we already have that partition discovered - that would happen if partition contains multiple data files.
                HivePartitionDto hivePartition = linkedHashMap.get(partitionValues);

                if (hivePartition != null)
                {
                    // Partition is already discovered, so just validate that the relative paths match.
                    Assert.isTrue(hivePartition.getPath().equals(partitionPath), String.format(
                        "Found two different locations for the same Hive partition. Storage: {%s}, business object data: {%s}, " +
                            "S3 key prefix: {%s}, path[1]: {%s}, path[2]: {%s}", storageName,
                        businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey), s3KeyPrefix, hivePartition.getPath(), partitionPath));
                }
                else
                {
                    // Add this partition to the hash map of discovered partitions.
                    linkedHashMap.put(partitionValues, new HivePartitionDto(partitionPath, partitionValues));
                }
            }
        }

        List<HivePartitionDto> hivePartitions = new ArrayList<>();
        hivePartitions.addAll(linkedHashMap.values());

        return hivePartitions;
    }

    /**
     * Gets a pattern to match Hive partition sub-directories.
     *
     * @param partitionColumns the list of partition columns
     *
     * @return the newly created pattern to match Hive partition sub-directories.
     */
    private Pattern getHivePathPattern(List<SchemaColumn> partitionColumns)
    {
        return Pattern.compile(getHivePathRegex(partitionColumns));
    }

    /**
     * Gets a regex to match Hive partition sub-directories.
     *
     * @param partitionColumns the list of partition columns
     *
     * @return the newly created regex to match Hive partition sub-directories.
     */
    private String getHivePathRegex(List<SchemaColumn> partitionColumns)
    {
        StringBuilder sb = new StringBuilder(26);

        sb.append("^(?:");      // Start a non-capturing group for the entire regex.

        // For each partition column, add a regular expression to match "<COLUMN_NAME|COLUMN-NAME>=<VALUE>" sub-directory.
        for (SchemaColumn partitionColumn : partitionColumns)
        {
            sb.append("(?:");   // Start a non-capturing group for the remainder of the regex.
            sb.append("(?:");   // Start a non-capturing group for folder markers.
            sb.append("\\/");   // Add a trailing "/".
            sb.append('|');     // Ann an OR.
            sb.append(REGEX_S3_EMPTY_PARTITION);    // Add a trailing "_$folder$", which represents an empty partition in S3.
            sb.append(')');     // Close the non-capturing group for folder markers.
            sb.append('|');     // Add an OR.
            sb.append("(?:");   // Start a non-capturing group for "/<column name>=<column value>".
            sb.append("\\/");   // Add a "/".
            // We are using a non-capturing group for the partition column names here - this is done by adding "?:" to the beginning of a capture group.
            sb.append("(?:");   // Start a non-capturing group for column name.
            sb.append("(?i)");  // Match partition column names case insensitive.
            sb.append(Matcher.quoteReplacement(partitionColumn.getName()));
            sb.append('|');     // Add an OR.
            // For sub-partition folder, we do support partition column names having all underscores replaced with hyphens.
            sb.append(Matcher.quoteReplacement(partitionColumn.getName().replace("_", "-")));
            sb.append(')');     // Close the non-capturing group for column name.
            sb.append("=([^/]+)"); // Add a capturing group for a column value.
        }

        // Add additional regular expression for the trailing empty folder marker and/or "/" followed by an optional file name.
        sb.append("(");     // Start a capturing group for folder markers and an optional file name.
        sb.append("\\/");   // Add a trailing "/".
        sb.append("[^/]*"); // Add an optional file name.
        sb.append('|');     // Add an OR.
        sb.append(REGEX_S3_EMPTY_PARTITION);    // Add a trailing "_$folder$", which represents an empty partition in S3.
        sb.append(")");     // Close the capturing group for folder markers and an optional file name.

        // Close all non-capturing groups that are still open.
        for (int i = 0; i < 2 * partitionColumns.size(); i++)
        {
            sb.append(')');
        }

        sb.append(')');     // Close the non-capturing group for the entire regex.
        sb.append('$');

        return sb.toString();
    }

    static class GenerateDdlRequestWrapper
    {
        private Boolean isGeneratePartitionsRequest;

        private Boolean allowMissingData;

        private Integer businessObjectDataVersion;

        private BusinessObjectFormatEntity businessObjectFormatEntity;

        private Integer businessObjectFormatVersion;

        private CustomDdlEntity customDdlEntity;

        private Boolean includeAllRegisteredSubPartitions;

        private Boolean includeDropPartitions;

        private Boolean includeDropTableStatement;

        private Boolean includeIfNotExistsOption;

        private Boolean isPartitioned;

        private List<List<String>> partitionFilters;

        private Map<String, String> cachedS3BucketNames;

        private Map<String, StorageEntity> cachedStorageEntities;

        private List<String> storageNames;

        private List<StorageEntity> requestedStorageEntities;

        private Boolean suppressScanForUnregisteredSubPartitions;

        private Boolean combineMultiplePartitionsInSingleAlterTable;

        private String tableName;

        public Boolean getGeneratePartitionsRequest()
        {
            return isGeneratePartitionsRequest;
        }

        public void setGeneratePartitionsRequest(Boolean generatePartitionsRequest)
        {
            isGeneratePartitionsRequest = generatePartitionsRequest;
        }

        public Boolean getAllowMissingData()
        {
            return allowMissingData;
        }

        public void setAllowMissingData(Boolean allowMissingData)
        {
            this.allowMissingData = allowMissingData;
        }

        public Integer getBusinessObjectDataVersion()
        {
            return businessObjectDataVersion;
        }

        public void setBusinessObjectDataVersion(Integer businessObjectDataVersion)
        {
            this.businessObjectDataVersion = businessObjectDataVersion;
        }

        public BusinessObjectFormatEntity getBusinessObjectFormatEntity()
        {
            return businessObjectFormatEntity;
        }

        public void setBusinessObjectFormatEntity(BusinessObjectFormatEntity businessObjectFormatEntity)
        {
            this.businessObjectFormatEntity = businessObjectFormatEntity;
        }

        public Integer getBusinessObjectFormatVersion()
        {
            return businessObjectFormatVersion;
        }

        public void setBusinessObjectFormatVersion(Integer businessObjectFormatVersion)
        {
            this.businessObjectFormatVersion = businessObjectFormatVersion;
        }

        public CustomDdlEntity getCustomDdlEntity()
        {
            return customDdlEntity;
        }

        public void setCustomDdlEntity(CustomDdlEntity customDdlEntity)
        {
            this.customDdlEntity = customDdlEntity;
        }

        public Boolean getIncludeAllRegisteredSubPartitions()
        {
            return includeAllRegisteredSubPartitions;
        }

        public void setIncludeAllRegisteredSubPartitions(Boolean includeAllRegisteredSubPartitions)
        {
            this.includeAllRegisteredSubPartitions = includeAllRegisteredSubPartitions;
        }

        public Boolean getIncludeDropPartitions()
        {
            return includeDropPartitions;
        }

        public void setIncludeDropPartitions(Boolean includeDropPartitions)
        {
            this.includeDropPartitions = includeDropPartitions;
        }

        public Boolean getIncludeDropTableStatement()
        {
            return includeDropTableStatement;
        }

        public void setIncludeDropTableStatement(Boolean includeDropTableStatement)
        {
            this.includeDropTableStatement = includeDropTableStatement;
        }

        public Boolean getIncludeIfNotExistsOption()
        {
            return includeIfNotExistsOption;
        }

        public void setIncludeIfNotExistsOption(Boolean includeIfNotExistsOption)
        {
            this.includeIfNotExistsOption = includeIfNotExistsOption;
        }

        public Boolean getPartitioned()
        {
            return isPartitioned;
        }

        public void setPartitioned(Boolean partitioned)
        {
            isPartitioned = partitioned;
        }

        public List<List<String>> getPartitionFilters()
        {
            return partitionFilters;
        }

        public void setPartitionFilters(List<List<String>> partitionFilters)
        {
            this.partitionFilters = partitionFilters;
        }

        public Map<String, String> getCachedS3BucketNames()
        {
            return cachedS3BucketNames;
        }

        public void setCachedS3BucketNames(Map<String, String> cachedS3BucketNames)
        {
            this.cachedS3BucketNames = cachedS3BucketNames;
        }

        public Map<String, StorageEntity> getCachedStorageEntities()
        {
            return cachedStorageEntities;
        }

        public void setCachedStorageEntities(Map<String, StorageEntity> cachedStorageEntities)
        {
            this.cachedStorageEntities = cachedStorageEntities;
        }

        public List<String> getStorageNames()
        {
            return storageNames;
        }

        public void setStorageNames(List<String> storageNames)
        {
            this.storageNames = storageNames;
        }

        public List<StorageEntity> getRequestedStorageEntities()
        {
            return requestedStorageEntities;
        }

        public void setRequestedStorageEntities(List<StorageEntity> requestedStorageEntities)
        {
            this.requestedStorageEntities = requestedStorageEntities;
        }

        public Boolean getSuppressScanForUnregisteredSubPartitions()
        {
            return suppressScanForUnregisteredSubPartitions;
        }

        public void setSuppressScanForUnregisteredSubPartitions(Boolean suppressScanForUnregisteredSubPartitions)
        {
            this.suppressScanForUnregisteredSubPartitions = suppressScanForUnregisteredSubPartitions;
        }

        public Boolean getCombineMultiplePartitionsInSingleAlterTable()
        {
            return combineMultiplePartitionsInSingleAlterTable;
        }

        public void setCombineMultiplePartitionsInSingleAlterTable(Boolean combineMultiplePartitionsInSingleAlterTable)
        {
            this.combineMultiplePartitionsInSingleAlterTable = combineMultiplePartitionsInSingleAlterTable;
        }

        public String getTableName()
        {
            return tableName;
        }

        public void setTableName(String tableName)
        {
            this.tableName = tableName;
        }
    }

    GenerateDdlRequestWrapper getGenerateDdlRequestWrapperInstance()
    {
        return new GenerateDdlRequestWrapper();
    }
}
