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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDataAttributeDaoTestHelper;
import org.finra.herd.dao.BusinessObjectDataDaoTestHelper;
import org.finra.herd.dao.BusinessObjectDataStatusDaoTestHelper;
import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.BusinessObjectFormatDaoTestHelper;
import org.finra.herd.dao.CustomDdlDaoTestHelper;
import org.finra.herd.dao.S3DaoTestHelper;
import org.finra.herd.dao.S3Operations;
import org.finra.herd.dao.SchemaColumnDaoTestHelper;
import org.finra.herd.dao.StorageDao;
import org.finra.herd.dao.StorageDaoTestHelper;
import org.finra.herd.dao.StorageFileDaoTestHelper;
import org.finra.herd.dao.StoragePolicyDao;
import org.finra.herd.dao.StoragePolicyDaoTestHelper;
import org.finra.herd.dao.StorageUnitDaoTestHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailability;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdl;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStatus;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusInformation;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.LatestAfterPartitionValue;
import org.finra.herd.model.api.xml.LatestBeforePartitionValue;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferResultsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.Hive13DdlGenerator;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.impl.BusinessObjectDataServiceImpl;

@Component
public class BusinessObjectDataServiceTestHelper
{
    @Autowired
    private BusinessObjectDataAttributeDaoTestHelper businessObjectDataAttributeDaoTestHelper;

    @Autowired
    private BusinessObjectDataDaoTestHelper businessObjectDataDaoTestHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectDataStatusDaoTestHelper businessObjectDataStatusDaoTestHelper;

    @Autowired
    private BusinessObjectDefinitionServiceTestHelper businessObjectDefinitionServiceTestHelper;

    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private BusinessObjectFormatDaoTestHelper businessObjectFormatDaoTestHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private CustomDdlDaoTestHelper customDdlDaoTestHelper;

    @Autowired
    private CustomDdlServiceTestHelper customDdlServiceTestHelper;

    @Autowired
    private S3DaoTestHelper s3DaoTestHelper;

    @Autowired
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Autowired
    private S3Operations s3Operations;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private SchemaColumnDaoTestHelper schemaColumnDaoTestHelper;

    @Autowired
    private StorageDao storageDao;

    @Autowired
    private StorageDaoTestHelper storageDaoTestHelper;

    @Autowired
    private StorageFileDaoTestHelper storageFileDaoTestHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StoragePolicyDao storagePolicyDao;

    @Autowired
    private StoragePolicyDaoTestHelper storagePolicyDaoTestHelper;

    @Autowired
    private StorageUnitDaoTestHelper storageUnitDaoTestHelper;

    /**
     * Returns a newly created business object data create request.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionKey the partition key
     * @param partitionValue the partition value
     * @param storageName the storage name
     * @param storageDirectoryPath the storage directory path
     * @param storageFiles the list of storage files
     *
     * @return the business object create request
     */
    public BusinessObjectDataCreateRequest createBusinessObjectDataCreateRequest(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionKey, String partitionValue,
        String businessObjectDataStatusCode, String storageName, String storageDirectoryPath, List<StorageFile> storageFiles)
    {
        // Create a business object data create request.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = new BusinessObjectDataCreateRequest();
        businessObjectDataCreateRequest.setNamespace(namespaceCode);
        businessObjectDataCreateRequest.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        businessObjectDataCreateRequest.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        businessObjectDataCreateRequest.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        businessObjectDataCreateRequest.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectDataCreateRequest.setPartitionKey(partitionKey);
        businessObjectDataCreateRequest.setPartitionValue(partitionValue);
        businessObjectDataCreateRequest.setStatus(businessObjectDataStatusCode);

        List<StorageUnitCreateRequest> storageUnits = new ArrayList<>();
        businessObjectDataCreateRequest.setStorageUnits(storageUnits);

        StorageUnitCreateRequest storageUnit = new StorageUnitCreateRequest();
        storageUnits.add(storageUnit);
        storageUnit.setStorageName(storageName);
        if (storageDirectoryPath != null)
        {
            StorageDirectory storageDirectory = new StorageDirectory();
            storageUnit.setStorageDirectory(storageDirectory);
            storageDirectory.setDirectoryPath(storageDirectoryPath);
        }
        storageUnit.setStorageFiles(storageFiles);

        return businessObjectDataCreateRequest;
    }

    /**
     * Creates and persists database entities required for business object data availability collection testing.
     */
    public void createDatabaseEntitiesForBusinessObjectDataAvailabilityCollectionTesting()
    {
        // Create a storage unit entity.
        storageUnitDaoTestHelper.createStorageUnitEntity(AbstractServiceTest.STORAGE_NAME, AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME,
            AbstractServiceTest.FORMAT_USAGE_CODE, AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.FORMAT_VERSION,
            AbstractServiceTest.PARTITION_VALUE, AbstractServiceTest.SUBPARTITION_VALUES, AbstractServiceTest.DATA_VERSION,
            AbstractServiceTest.LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
            AbstractServiceTest.NO_STORAGE_DIRECTORY_PATH);
    }

    /**
     * Creates relative database entities required for the unit tests.
     */
    public void createDatabaseEntitiesForBusinessObjectDataDdlTesting()
    {
        createDatabaseEntitiesForBusinessObjectDataDdlTesting(FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME,
            AbstractServiceTest.PARTITION_KEY_GROUP, BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, AbstractServiceTest.UNSORTED_PARTITION_VALUES,
            AbstractServiceTest.SUBPARTITION_VALUES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE, AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
            AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            schemaColumnDaoTestHelper.getTestPartitionColumns(), false, AbstractServiceTest.CUSTOM_DDL_NAME, AbstractServiceTest.LATEST_VERSION_FLAG_SET,
            AbstractServiceTest.ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);
    }

    /**
     * Creates relative database entities required for the unit tests.
     */
    public void createDatabaseEntitiesForBusinessObjectDataDdlTesting(String businessObjectFormatFileType, String partitionKey, String partitionKeyGroupName,
        int partitionColumnPosition, List<String> partitionValues, List<String> subPartitionValues, String schemaDelimiterCharacter,
        String schemaEscapeCharacter, String schemaNullValue, List<SchemaColumn> schemaColumns, List<SchemaColumn> partitionColumns,
        boolean replaceUnderscoresWithHyphens, String customDdlName, boolean generateStorageFileEntities, boolean allowDuplicateBusinessObjectData)
    {
        // Create a business object format entity if it does not exist.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                businessObjectFormatFileType, AbstractServiceTest.FORMAT_VERSION));
        if (businessObjectFormatEntity == null)
        {
            businessObjectFormatEntity = businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                    businessObjectFormatFileType, AbstractServiceTest.FORMAT_VERSION, AbstractServiceTest.FORMAT_DESCRIPTION,
                    AbstractServiceTest.FORMAT_DOCUMENT_SCHEMA, AbstractServiceTest.LATEST_VERSION_FLAG_SET, partitionKey, partitionKeyGroupName,
                    AbstractServiceTest.NO_ATTRIBUTES, schemaDelimiterCharacter, schemaEscapeCharacter, schemaNullValue, schemaColumns, partitionColumns);
        }

        if (StringUtils.isNotBlank(customDdlName))
        {
            boolean partitioned = (partitionColumns != null);
            customDdlDaoTestHelper.createCustomDdlEntity(businessObjectFormatEntity, customDdlName, customDdlServiceTestHelper.getTestCustomDdl(partitioned));
        }

        // Create S3 storages with the relative "bucket.name" attribute configured.
        StorageEntity storageEntity1 = storageDao.getStorageByName(AbstractServiceTest.STORAGE_NAME);
        if (storageEntity1 == null)
        {
            storageEntity1 = storageDaoTestHelper.createStorageEntity(AbstractServiceTest.STORAGE_NAME, StoragePlatformEntity.S3, Arrays
                .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), AbstractServiceTest.S3_BUCKET_NAME),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                        AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE)));
        }
        StorageEntity storageEntity2 = storageDao.getStorageByName(AbstractServiceTest.STORAGE_NAME_2);
        if (storageEntity2 == null)
        {
            storageEntity2 = storageDaoTestHelper.createStorageEntity(AbstractServiceTest.STORAGE_NAME_2, StoragePlatformEntity.S3, Arrays
                .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), AbstractServiceTest.S3_BUCKET_NAME_2),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                        AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE)));
        }

        // Create business object data for each partition value.
        for (String partitionValue : partitionValues)
        {
            BusinessObjectDataEntity businessObjectDataEntity;

            // Create a business object data instance for the specified partition value.
            if (partitionColumnPosition == BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION)
            {
                businessObjectDataEntity = businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                        businessObjectFormatFileType, AbstractServiceTest.FORMAT_VERSION, partitionValue, subPartitionValues, AbstractServiceTest.DATA_VERSION,
                        AbstractServiceTest.LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
            }
            else
            {
                List<String> testSubPartitionValues = new ArrayList<>(subPartitionValues);
                // Please note that the second partition column is located at index 0.
                testSubPartitionValues.set(partitionColumnPosition - 2, partitionValue);
                businessObjectDataEntity = businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                        businessObjectFormatFileType, AbstractServiceTest.FORMAT_VERSION, AbstractServiceTest.PARTITION_VALUE, testSubPartitionValues,
                        AbstractServiceTest.DATA_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
            }

            // Get the expected S3 key prefix.
            String s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatEntity,
                businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity), AbstractServiceTest.STORAGE_NAME);

            // Check if we need to create the relative storage units.
            if (AbstractServiceTest.STORAGE_1_AVAILABLE_PARTITION_VALUES.contains(partitionValue) ||
                Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE.equals(partitionValue))
            {
                StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                    .createStorageUnitEntity(storageEntity1, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED,
                        AbstractServiceTest.NO_STORAGE_DIRECTORY_PATH);

                // If flag is set, create one storage file for each "auto-discoverable" partition.
                // Please note that is n! - thus we want to keep the number of partition levels small.
                if (generateStorageFileEntities)
                {
                    storageFileDaoTestHelper
                        .createStorageFileEntities(storageUnitEntity, s3KeyPrefix, partitionColumns, subPartitionValues, replaceUnderscoresWithHyphens);
                }
                // Add storage directory path value to the storage unit, since we have no storage files generated.
                else
                {
                    storageUnitEntity.setDirectoryPath(s3KeyPrefix);
                }
            }

            if (AbstractServiceTest.STORAGE_2_AVAILABLE_PARTITION_VALUES.contains(partitionValue) &&
                (allowDuplicateBusinessObjectData || !AbstractServiceTest.STORAGE_1_AVAILABLE_PARTITION_VALUES.contains(partitionValue)))
            {
                StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                    .createStorageUnitEntity(storageEntity2, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED,
                        AbstractServiceTest.NO_STORAGE_DIRECTORY_PATH);

                // If flag is set, create one storage file for each "auto-discoverable" partition.
                // Please note that is n! - thus we want to keep the number of partition levels small.
                if (generateStorageFileEntities)
                {
                    storageFileDaoTestHelper
                        .createStorageFileEntities(storageUnitEntity, s3KeyPrefix, partitionColumns, subPartitionValues, replaceUnderscoresWithHyphens);
                }
                // Add storage directory path value to the storage unit, since we have no storage files generated.
                else
                {
                    storageUnitEntity.setDirectoryPath(s3KeyPrefix);
                }
            }
        }
    }

    /**
     * Creates and persists database entities required for generate business object data and format ddl testing.
     */
    public StorageUnitEntity createDatabaseEntitiesForBusinessObjectDataDdlTesting(String partitionValue)
    {
        if (partitionValue != null)
        {
            // Build an S3 key prefix according to the herd S3 naming convention.
            String s3KeyPrefix = AbstractServiceTest
                .getExpectedS3KeyPrefix(AbstractServiceTest.NAMESPACE, AbstractServiceTest.DATA_PROVIDER_NAME, AbstractServiceTest.BDEF_NAME,
                    AbstractServiceTest.FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FORMAT_VERSION,
                    AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME, partitionValue, null, null, AbstractServiceTest.DATA_VERSION);

            // Creates and persists database entities required for generating business object data ddl testing.
            return createDatabaseEntitiesForBusinessObjectDataDdlTesting(partitionValue, s3KeyPrefix);
        }
        else
        {
            // Creates and persists database entities required for generating business object format ddl testing.
            return createDatabaseEntitiesForBusinessObjectDataDdlTesting(null, null);
        }
    }

    /**
     * Creates and persists database entities required for generate business object data ddl testing.
     */
    public StorageUnitEntity createDatabaseEntitiesForBusinessObjectDataDdlTesting(String partitionValue, String s3KeyPrefix)
    {
        // Build a list of schema columns.
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        schemaColumns.add(
            new SchemaColumn(AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME, "DATE", AbstractServiceTest.NO_COLUMN_SIZE, AbstractServiceTest.COLUMN_REQUIRED,
                AbstractServiceTest.NO_COLUMN_DEFAULT_VALUE, AbstractServiceTest.NO_COLUMN_DESCRIPTION));
        schemaColumns.add(new SchemaColumn(AbstractServiceTest.COLUMN_NAME, "NUMBER", AbstractServiceTest.COLUMN_SIZE, AbstractServiceTest.NO_COLUMN_REQUIRED,
            AbstractServiceTest.COLUMN_DEFAULT_VALUE, AbstractServiceTest.COLUMN_DESCRIPTION));

        // Use the first column as a partition column.
        List<SchemaColumn> partitionColumns = schemaColumns.subList(0, 1);

        // Create a business object format entity with the schema.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FORMAT_VERSION, AbstractServiceTest.FORMAT_DESCRIPTION,
                AbstractServiceTest.FORMAT_DOCUMENT_SCHEMA, AbstractServiceTest.LATEST_VERSION_FLAG_SET, AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME,
                AbstractServiceTest.NO_PARTITION_KEY_GROUP, AbstractServiceTest.NO_ATTRIBUTES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE,
                AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH, AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns);

        if (partitionValue != null)
        {
            // Create a business object data entity.
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(businessObjectFormatEntity, partitionValue, AbstractServiceTest.NO_SUBPARTITION_VALUES,
                    AbstractServiceTest.DATA_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

            // Create an S3 storage entity.
            StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(AbstractServiceTest.STORAGE_NAME, StoragePlatformEntity.S3, Arrays
                .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), AbstractServiceTest.S3_BUCKET_NAME),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                        AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

            // Create a storage unit with a storage directory path.
            return storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, s3KeyPrefix);
        }

        return null;
    }

    /**
     * Creates and persists VALID two-level partitioned business object data with "available" storage units in an S3 storage.
     *
     * @param partitions the list of partitions, where each is represented by a primary value and a sub-partition value
     *
     * @return the list of created storage unit entities
     */
    public List<StorageUnitEntity> createDatabaseEntitiesForBusinessObjectDataDdlTestingTwoPartitionLevels(List<List<String>> partitions)
    {
        // Create a list of storage unit entities to be returned.
        List<StorageUnitEntity> result = new ArrayList<>();

        // Build a list of schema columns.
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        schemaColumns.add(
            new SchemaColumn(AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME, "DATE", AbstractServiceTest.NO_COLUMN_SIZE, AbstractServiceTest.COLUMN_REQUIRED,
                AbstractServiceTest.NO_COLUMN_DEFAULT_VALUE, AbstractServiceTest.NO_COLUMN_DESCRIPTION));
        schemaColumns.add(new SchemaColumn(AbstractServiceTest.SECOND_PARTITION_COLUMN_NAME, "STRING", AbstractServiceTest.NO_COLUMN_SIZE,
            AbstractServiceTest.COLUMN_REQUIRED, AbstractServiceTest.NO_COLUMN_DEFAULT_VALUE, AbstractServiceTest.NO_COLUMN_DESCRIPTION));
        schemaColumns.add(new SchemaColumn(AbstractServiceTest.COLUMN_NAME, "NUMBER", AbstractServiceTest.COLUMN_SIZE, AbstractServiceTest.NO_COLUMN_REQUIRED,
            AbstractServiceTest.COLUMN_DEFAULT_VALUE, AbstractServiceTest.COLUMN_DESCRIPTION));

        // Use the first two columns as partition columns.
        List<SchemaColumn> partitionColumns = schemaColumns.subList(0, 2);

        // Create a business object format entity with the schema.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FORMAT_VERSION, AbstractServiceTest.FORMAT_DESCRIPTION,
                AbstractServiceTest.FORMAT_DOCUMENT_SCHEMA, AbstractServiceTest.LATEST_VERSION_FLAG_SET, AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME,
                AbstractServiceTest.NO_PARTITION_KEY_GROUP, AbstractServiceTest.NO_ATTRIBUTES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE,
                AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH, AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumns, partitionColumns);

        // Create an S3 storage entity.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(AbstractServiceTest.STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), AbstractServiceTest.S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        for (List<String> partition : partitions)
        {
            // Build an S3 key prefix according to the herd S3 naming convention.
            String s3KeyPrefix = AbstractServiceTest
                .getExpectedS3KeyPrefix(AbstractServiceTest.NAMESPACE, AbstractServiceTest.DATA_PROVIDER_NAME, AbstractServiceTest.BDEF_NAME,
                    AbstractServiceTest.FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FORMAT_VERSION,
                    AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME, partition.get(0), partitionColumns.subList(1, 2).toArray(new SchemaColumn[1]),
                    Arrays.asList(partition.get(1)).toArray(new String[1]), AbstractServiceTest.DATA_VERSION);

            // Create a business object data entity.
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(businessObjectFormatEntity, partition.get(0), Arrays.asList(partition.get(1)), AbstractServiceTest.DATA_VERSION,
                    AbstractServiceTest.LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

            // Create an "available" storage unit with a storage directory path.
            result.add(storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, s3KeyPrefix));
        }

        return result;
    }

    /**
     * Creates database entities for business object data search testing.
     */
    public void createDatabaseEntitiesForBusinessObjectDataSearchTesting()
    {
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.FORMAT_VERSION, AbstractServiceTest.PARTITION_VALUE,
                AbstractServiceTest.NO_SUBPARTITION_VALUES, AbstractServiceTest.DATA_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.VALID);

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE_2,
                AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.FORMAT_VERSION, AbstractServiceTest.PARTITION_VALUE,
                AbstractServiceTest.NO_SUBPARTITION_VALUES, AbstractServiceTest.DATA_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.INVALID);

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(AbstractServiceTest.NAMESPACE_2, AbstractServiceTest.BDEF_NAME_2, AbstractServiceTest.FORMAT_USAGE_CODE_2,
                AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.FORMAT_VERSION_2, AbstractServiceTest.PARTITION_VALUE,
                AbstractServiceTest.NO_SUBPARTITION_VALUES, AbstractServiceTest.DATA_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.INVALID);

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(AbstractServiceTest.NAMESPACE_2, AbstractServiceTest.BDEF_NAME_2, AbstractServiceTest.FORMAT_USAGE_CODE_2,
                AbstractServiceTest.FORMAT_FILE_TYPE_CODE_2, AbstractServiceTest.FORMAT_VERSION_2, AbstractServiceTest.PARTITION_VALUE,
                AbstractServiceTest.NO_SUBPARTITION_VALUES, AbstractServiceTest.DATA_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.VALID);
    }

    /**
     * Create and persist database entities required for the finalize restore testing.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the business object data entity
     */
    public BusinessObjectDataEntity createDatabaseEntitiesForFinalizeRestoreTesting(BusinessObjectDataKey businessObjectDataKey)
    {
        return createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey, AbstractServiceTest.STORAGE_NAME, AbstractServiceTest.S3_BUCKET_NAME,
            StorageUnitStatusEntity.RESTORING);
    }

    /**
     * Create and persist database entities required for the finalize restore testing.
     *
     * @param businessObjectDataKey the business object data key
     * @param storageName the storage name
     * @param s3BucketName the S3 bucket name
     * @param s3StorageUnitStatus the storage unit status
     *
     * @return the business object data entity
     */
    public BusinessObjectDataEntity createDatabaseEntitiesForFinalizeRestoreTesting(BusinessObjectDataKey businessObjectDataKey, String storageName,
        String s3BucketName, String s3StorageUnitStatus)
    {
        // Create

        // Create and persist a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectDataKey, AbstractServiceTest.LATEST_VERSION_FLAG_SET, AbstractServiceTest.BDATA_STATUS);

        // Create and persist an S3 storage entity.
        StorageEntity storageEntity;
        if (s3BucketName != null)
        {
            storageEntity = storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, Arrays
                .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), s3BucketName),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                        AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE)));
        }
        else
        {
            storageEntity = storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3);
        }

        // Create and persist a storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageEntity, businessObjectDataEntity, s3StorageUnitStatus, AbstractServiceTest.NO_STORAGE_DIRECTORY_PATH);

        // Get the expected S3 key prefix for the business object data key.
        String s3KeyPrefix = AbstractServiceTest
            .getExpectedS3KeyPrefix(businessObjectDataKey, AbstractServiceTest.DATA_PROVIDER_NAME, AbstractServiceTest.PARTITION_KEY,
                AbstractServiceTest.NO_SUB_PARTITION_KEYS);

        // Create and add storage file entities to the storage unit.
        for (String relativeFilePath : AbstractServiceTest.LOCAL_FILES)
        {
            storageFileDaoTestHelper
                .createStorageFileEntity(storageUnitEntity, String.format("%s/%s", s3KeyPrefix, relativeFilePath), AbstractServiceTest.FILE_SIZE_1_KB,
                    AbstractServiceTest.ROW_COUNT);
        }

        // Return the business object data entity.
        return businessObjectDataEntity;
    }

    /**
     * Create and persist database entities required for testing.
     *
     * @param createBusinessObjectDataEntity specifies if a business object data instance should be created or not
     */
    public void createDatabaseEntitiesForGetS3KeyPrefixTesting(boolean createBusinessObjectDataEntity)
    {
        // Get a list of test schema partition columns and use the first column name as the partition key.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();
        String partitionKey = partitionColumns.get(0).getName();

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.FORMAT_VERSION, AbstractServiceTest.FORMAT_DESCRIPTION,
                AbstractServiceTest.FORMAT_DOCUMENT_SCHEMA, AbstractServiceTest.LATEST_VERSION_FLAG_SET, partitionKey,
                AbstractServiceTest.NO_PARTITION_KEY_GROUP, AbstractServiceTest.NO_ATTRIBUTES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE,
                AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH, AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N,
                schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns);

        // Create and persist an S3 storage with the S3 key prefix velocity template attribute.
        storageDaoTestHelper.createStorageEntity(AbstractServiceTest.STORAGE_NAME, StoragePlatformEntity.S3,
            configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
            AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE);

        // If requested, create and persist a business object data entity.
        if (createBusinessObjectDataEntity)
        {
            businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(businessObjectFormatEntity, AbstractServiceTest.PARTITION_VALUE, AbstractServiceTest.SUBPARTITION_VALUES,
                    AbstractServiceTest.DATA_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET, AbstractServiceTest.BDATA_STATUS);
        }
    }

    /**
     * Create and persist database entities required for the initiate a restore request testing.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the business object data entity
     */
    public BusinessObjectDataEntity createDatabaseEntitiesForInitiateRestoreTesting(BusinessObjectDataKey businessObjectDataKey)
    {
        return createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, AbstractServiceTest.STORAGE_NAME, AbstractServiceTest.S3_BUCKET_NAME,
            StorageUnitStatusEntity.ARCHIVED, AbstractServiceTest.LOCAL_FILES);
    }

    /**
     * Create and persist database entities required for the initiate a restore request testing.
     *
     * @param businessObjectDataKey the business object data key
     * @param storageName the storage name
     * @param s3BucketName the S3 bucket name
     * @param storageUnitStatus the storage unit status
     * @param localFiles the list of local files to create relative storage files
     *
     * @return the business object data entity
     */
    public BusinessObjectDataEntity createDatabaseEntitiesForInitiateRestoreTesting(BusinessObjectDataKey businessObjectDataKey, String storageName,
        String s3BucketName, String storageUnitStatus, List<String> localFiles)
    {
        // Create and persist a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectDataKey, AbstractServiceTest.LATEST_VERSION_FLAG_SET, AbstractServiceTest.BDATA_STATUS);

        // Create and persist an S3 storage entity.
        StorageEntity storageEntity;
        if (s3BucketName != null)
        {
            storageEntity = storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, Arrays
                .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), s3BucketName),
                    new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                        AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE)));
        }
        else
        {
            storageEntity = storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3);
        }

        // Create and persist a storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatus, AbstractServiceTest.NO_STORAGE_DIRECTORY_PATH);

        // Get the expected S3 key prefix for the business object data key.
        String s3KeyPrefix = AbstractServiceTest
            .getExpectedS3KeyPrefix(businessObjectDataKey, AbstractServiceTest.DATA_PROVIDER_NAME, AbstractServiceTest.PARTITION_KEY,
                AbstractServiceTest.NO_SUB_PARTITION_KEYS);

        // Create and add storage file entities to the storage unit.
        for (String relativeFilePath : localFiles)
        {
            storageFileDaoTestHelper
                .createStorageFileEntity(storageUnitEntity, String.format("%s/%s", s3KeyPrefix, relativeFilePath), AbstractServiceTest.FILE_SIZE_1_KB,
                    AbstractServiceTest.ROW_COUNT);
        }

        // Return the business object data entity.
        return businessObjectDataEntity;
    }

    /**
     * Create and persist database entities required for the retry storage policy transition testing.
     *
     * @param businessObjectDataKey the business object data key
     * @param storagePolicyKey the storage policy key
     *
     * @return the business object data entity
     */
    public BusinessObjectDataEntity createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(BusinessObjectDataKey businessObjectDataKey,
        StoragePolicyKey storagePolicyKey)
    {
        return createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, AbstractServiceTest.STORAGE_NAME,
            AbstractServiceTest.S3_BUCKET_NAME, StorageUnitStatusEntity.ARCHIVING);
    }

    /**
     * Create and persist database entities required for the retry storage policy transition testing.
     *
     * @param businessObjectDataKey the business object data key
     * @param storagePolicyKey the storage policy key
     * @param storageName the storage name
     * @param s3BucketName the S3 bucket name
     * @param storageUnitStatus the storage unit status
     *
     * @return the business object data entity
     */
    public BusinessObjectDataEntity createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(BusinessObjectDataKey businessObjectDataKey,
        StoragePolicyKey storagePolicyKey, String storageName, String s3BucketName, String storageUnitStatus)
    {
        // Create a business object format entity with a schema.
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestSchemaColumns(AbstractServiceTest.RANDOM_SUFFIX);
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                businessObjectDataKey.getBusinessObjectFormatVersion(), AbstractServiceTest.FORMAT_DESCRIPTION, AbstractServiceTest.FORMAT_DOCUMENT_SCHEMA,
                AbstractServiceTest.LATEST_VERSION_FLAG_SET, partitionColumns.get(0).getName(), AbstractServiceTest.NO_PARTITION_KEY_GROUP,
                AbstractServiceTest.NO_ATTRIBUTES, AbstractServiceTest.SCHEMA_DELIMITER_PIPE, AbstractServiceTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                AbstractServiceTest.SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), partitionColumns);

        // Create and persist a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, businessObjectDataKey.getPartitionValue(),
                businessObjectDataKey.getSubPartitionValues(), businessObjectDataKey.getBusinessObjectDataVersion(),
                AbstractServiceTest.LATEST_VERSION_FLAG_SET, AbstractServiceTest.BDATA_STATUS);

        // If specified, create and persist an S3 storage entity along with a storage unit.
        if (StringUtils.isNotBlank(storageName))
        {
            StorageEntity storageEntity = storageDao.getStorageByName(storageName);
            if (storageEntity == null)
            {
                // Create and persist an S3 storage entity.
                List<Attribute> attributes = new ArrayList<>();
                if (StringUtils.isNotBlank(s3BucketName))
                {
                    attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), s3BucketName));
                }
                attributes
                    .add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()));
                attributes
                    .add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString()));
                attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE));
                storageEntity = storageDaoTestHelper.createStorageEntity(storageName, StoragePlatformEntity.S3, attributes);
            }

            // Create and persist an S3 storage unit entity.
            if (StringUtils.isNotBlank(storageUnitStatus))
            {
                StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                    .createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatus, AbstractServiceTest.NO_STORAGE_DIRECTORY_PATH);

                // Get S3 key prefix for this business object data.
                String s3KeyPrefix = s3KeyPrefixHelper
                    .buildS3KeyPrefix(storageEntity, storageUnitEntity.getBusinessObjectData().getBusinessObjectFormat(), businessObjectDataKey);

                // Create and add storage file entities to the storage unit.
                for (String relativeFilePath : AbstractServiceTest.LOCAL_FILES)
                {
                    storageFileDaoTestHelper
                        .createStorageFileEntity(storageUnitEntity, String.format("%s/%s", s3KeyPrefix, relativeFilePath), AbstractServiceTest.FILE_SIZE_1_KB,
                            AbstractServiceTest.ROW_COUNT);
                }
            }
        }

        // Create and persist a storage policy if needed.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDao.getStoragePolicyByAltKey(storagePolicyKey);
        if (storagePolicyEntity == null)
        {
            storagePolicyDaoTestHelper
                .createStoragePolicyEntity(storagePolicyKey, StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, AbstractServiceTest.BDATA_AGE_IN_DAYS,
                    businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                    businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(), storageName,
                    StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, AbstractServiceTest.INITIAL_VERSION,
                    AbstractServiceTest.LATEST_VERSION_FLAG_SET);
        }

        // Return the business object data entity.
        return businessObjectDataEntity;
    }

    /**
     * Creates an object in S3 with the prefix constructed from the given parameters. The object's full path will be {prefix}/{UUID}
     *
     * @param businessObjectFormatEntity business object format
     * @param request request with partition values and storage
     * @param businessObjectDataVersion business object data version to put
     */
    public void createS3Object(BusinessObjectFormatEntity businessObjectFormatEntity, BusinessObjectDataInvalidateUnregisteredRequest request,
        int businessObjectDataVersion)
    {
        StorageEntity storageEntity = storageDao.getStorageByName(request.getStorageName());
        String s3BucketName = storageHelper.getS3BucketAccessParams(storageEntity).getS3BucketName();

        BusinessObjectDataKey businessObjectDataKey = getBusinessObjectDataKey(request);
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataVersion);

        String s3KeyPrefix = s3KeyPrefixHelper
            .buildS3KeyPrefix(AbstractServiceTest.S3_KEY_PREFIX_VELOCITY_TEMPLATE, businessObjectFormatEntity, businessObjectDataKey, storageEntity.getName());
        String s3ObjectKey = s3KeyPrefix + "/test";
        PutObjectRequest putObjectRequest = new PutObjectRequest(s3BucketName, s3ObjectKey, new ByteArrayInputStream(new byte[1]), new ObjectMetadata());
        s3Operations.putObject(putObjectRequest, null);
    }

    /**
     * Creates a simple business object data search request.
     *
     * @param namespace the namespace to search for
     * @param bdefName the bdef name to search for
     *
     * @return the newly created business object data search request
     */
    public BusinessObjectDataSearchRequest createSimpleBusinessObjectDataSearchRequest(final String namespace, final String bdefName)
    {
        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(namespace);
        key.setBusinessObjectDefinitionName(bdefName);
        businessObjectDataSearchKeys.add(key);
        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);
        return request;
    }

    /**
     * Creates a test "valid" business object data entry with default sub-partition values.
     *
     * @return the newly created business object data.
     */
    public BusinessObjectDataEntity createTestValidBusinessObjectData()
    {
        return createTestValidBusinessObjectData(AbstractServiceTest.SUBPARTITION_VALUES, AbstractServiceTest.NO_ATTRIBUTE_DEFINITIONS,
            AbstractServiceTest.NO_ATTRIBUTES);
    }

    /**
     * Creates a test "valid" business object data entry.
     *
     * @param subPartitionValues the sub-partition values.
     *
     * @return the newly created business object data.
     */
    public BusinessObjectDataEntity createTestValidBusinessObjectData(List<String> subPartitionValues, List<AttributeDefinition> attributeDefinitions,
        List<Attribute> attributes)
    {
        // Create a persisted business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(AbstractServiceTest.BDEF_NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.FORMAT_VERSION, AbstractServiceTest.PARTITION_VALUE, subPartitionValues,
                AbstractServiceTest.DATA_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // If specified, add business object data attribute definitions to the business object format.
        if (!CollectionUtils.isEmpty(attributeDefinitions))
        {
            for (AttributeDefinition attributeDefinition : attributeDefinitions)
            {
                businessObjectFormatDaoTestHelper
                    .createBusinessObjectDataAttributeDefinitionEntity(businessObjectDataEntity.getBusinessObjectFormat(), attributeDefinition.getName(),
                        attributeDefinition.isPublish());
            }
        }

        // If specified, add business object data attributes to the business object data.
        if (!CollectionUtils.isEmpty(attributes))
        {
            for (Attribute attribute : attributes)
            {
                businessObjectDataAttributeDaoTestHelper
                    .createBusinessObjectDataAttributeEntity(businessObjectDataEntity, attribute.getName(), attribute.getValue());
            }
        }

        return businessObjectDataEntity;
    }

    /**
     * Gets the {@link BusinessObjectDataKey} from the given request.
     *
     * @param request {@link org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest}
     *
     * @return {@link BusinessObjectDataKey} minus the version
     */
    public BusinessObjectDataKey getBusinessObjectDataKey(BusinessObjectDataInvalidateUnregisteredRequest request)
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(request.getNamespace());
        businessObjectDataKey.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName());
        businessObjectDataKey.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage());
        businessObjectDataKey.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType());
        businessObjectDataKey.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());
        businessObjectDataKey.setPartitionValue(request.getPartitionValue());
        businessObjectDataKey.setSubPartitionValues(request.getSubPartitionValues());
        return businessObjectDataKey;
    }

    public BusinessObjectDataStorageUnitCreateRequest getBusinessObjectDataStorageUnitCreateRequest()
    {
        // Create business object data and storage entities.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(false);
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = businessObjectDataStatusDaoTestHelper
            .createBusinessObjectDataStatusEntity(AbstractServiceTest.BDATA_STATUS, AbstractServiceTest.DESCRIPTION,
                AbstractServiceTest.BDATA_STATUS_PRE_REGISTRATION_FLAG_SET);
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, AbstractServiceTest.PARTITION_VALUE, AbstractServiceTest.DATA_VERSION,
                AbstractServiceTest.LATEST_VERSION_FLAG_SET, businessObjectDataStatusEntity.getCode());
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity();

        // Create a business object data storage unit key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
                businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
                businessObjectFormatEntity.getFileType().getCode(), businessObjectFormatEntity.getBusinessObjectFormatVersion(),
                businessObjectDataEntity.getPartitionValue(), AbstractServiceTest.NO_SUBPARTITION_VALUES, businessObjectDataEntity.getVersion(),
                storageEntity.getName());

        // Create and return a business object data storage unit create request.
        return new BusinessObjectDataStorageUnitCreateRequest(businessObjectDataStorageUnitKey, AbstractServiceTest.NO_STORAGE_DIRECTORY, getStorageFiles(),
            AbstractServiceTest.NO_DISCOVER_STORAGE_FILES);
    }

    /**
     * Creates an expected business object data availability collection response using hard coded test values.
     *
     * @return the business object data availability collection response
     */
    public BusinessObjectDataAvailabilityCollectionResponse getExpectedBusinessObjectDataAvailabilityCollectionResponse()
    {
        // Prepare a check availability collection response using hard coded test values.
        BusinessObjectDataAvailabilityCollectionResponse businessObjectDataAvailabilityCollectionResponse =
            new BusinessObjectDataAvailabilityCollectionResponse();

        // Create a list of check business object data availability responses.
        List<BusinessObjectDataAvailability> businessObjectDataAvailabilityResponses = new ArrayList<>();
        businessObjectDataAvailabilityCollectionResponse.setBusinessObjectDataAvailabilityResponses(businessObjectDataAvailabilityResponses);

        // Create a business object data availability response.
        BusinessObjectDataAvailability businessObjectDataAvailability =
            new BusinessObjectDataAvailability(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, Arrays.asList(AbstractServiceTest.PARTITION_VALUE),
                    AbstractServiceTest.NO_PARTITION_VALUE_RANGE, AbstractServiceTest.NO_LATEST_BEFORE_PARTITION_VALUE,
                    AbstractServiceTest.NO_LATEST_AFTER_PARTITION_VALUE)), null, AbstractServiceTest.DATA_VERSION, AbstractServiceTest.NO_STORAGE_NAMES,
                AbstractServiceTest.STORAGE_NAME, Arrays.asList(
                new BusinessObjectDataStatus(AbstractServiceTest.FORMAT_VERSION, AbstractServiceTest.PARTITION_VALUE, AbstractServiceTest.SUBPARTITION_VALUES,
                    AbstractServiceTest.DATA_VERSION, BusinessObjectDataStatusEntity.VALID)), new ArrayList<>());
        businessObjectDataAvailabilityResponses.add(businessObjectDataAvailability);

        // Set the expected values for the flags.
        businessObjectDataAvailabilityCollectionResponse.setIsAllDataAvailable(true);
        businessObjectDataAvailabilityCollectionResponse.setIsAllDataNotAvailable(false);

        return businessObjectDataAvailabilityCollectionResponse;
    }

    /**
     * Returns Hive DDL that is expected to be produced by a unit test based on specified parameters and hard-coded test values.
     *
     * @return the Hive DDL
     */
    public String getExpectedBusinessObjectDataDdl()
    {
        return getExpectedBusinessObjectDataDdl(AbstractServiceTest.PARTITION_COLUMNS.length, AbstractServiceTest.FIRST_COLUMN_NAME,
            AbstractServiceTest.FIRST_COLUMN_DATA_TYPE, AbstractServiceTest.ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT, FileTypeEntity.TXT_FILE_TYPE,
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, AbstractServiceTest.STORAGE_1_AVAILABLE_PARTITION_VALUES,
            AbstractServiceTest.SUBPARTITION_VALUES, false, true, true);
    }

    /**
     * Returns Hive DDL that is expected to be produced by a unit test based on specified parameters and hard-coded test values.
     *
     * @return the Hive DDL
     */
    public String getExpectedBusinessObjectDataDdl(int partitionLevels, String firstColumnName, String firstColumnDataType, String hiveRowFormat,
        String hiveFileFormat, String businessObjectFormatFileType, int partitionColumnPosition, List<String> partitionValues, List<String> subPartitionValues,
        boolean replaceUnderscoresWithHyphens, boolean isDropStatementIncluded, boolean isIfNotExistsOptionIncluded)
    {
        return getExpectedBusinessObjectDataDdl(partitionLevels, firstColumnName, firstColumnDataType, hiveRowFormat, hiveFileFormat,
            businessObjectFormatFileType, partitionColumnPosition, partitionValues, subPartitionValues, replaceUnderscoresWithHyphens, isDropStatementIncluded,
            isIfNotExistsOptionIncluded, AbstractServiceTest.NO_INCLUDE_DROP_PARTITIONS);
    }

    /**
     * Returns Hive DDL that is expected to be produced by a unit test based on specified parameters and hard-coded test values.
     *
     * @param partitionLevels the number of partition levels
     * @param firstColumnName the name of the first schema column
     * @param firstColumnDataType the data type of the first schema column
     * @param hiveRowFormat the Hive row format
     * @param hiveFileFormat the Hive file format
     * @param businessObjectFormatFileType the business object format file type
     * @param partitionColumnPosition the position of the partition column
     * @param partitionValues the list of partition values
     * @param subPartitionValues the list of subpartition values
     * @param replaceUnderscoresWithHyphens specifies if we need to replace underscores with hyphens in subpartition key values when building subpartition
     * location path
     * @param isDropStatementIncluded specifies if expected DDL should include a drop table statement
     * @param isDropPartitionsStatementsIncluded specifies if expected DDL should include the relative drop partition statements
     *
     * @return the Hive DDL
     */
    public String getExpectedBusinessObjectDataDdl(int partitionLevels, String firstColumnName, String firstColumnDataType, String hiveRowFormat,
        String hiveFileFormat, String businessObjectFormatFileType, int partitionColumnPosition, List<String> partitionValues, List<String> subPartitionValues,
        boolean replaceUnderscoresWithHyphens, boolean isDropStatementIncluded, boolean isIfNotExistsOptionIncluded, boolean isDropPartitionsStatementsIncluded)
    {
        StringBuilder sb = new StringBuilder();

        if (isDropStatementIncluded)
        {
            sb.append("DROP TABLE IF EXISTS `[Table Name]`;\n\n");
        }
        sb.append("CREATE EXTERNAL TABLE [If Not Exists]`[Table Name]` (\n");
        sb.append(String.format("    `%s` %s,\n", firstColumnName, firstColumnDataType));
        sb.append("    `COLUMN002` SMALLINT COMMENT 'This is \\'COLUMN002\\' column. ");
        sb.append("Here are \\'single\\' and \"double\" quotes along with a backslash \\.',\n");
        sb.append("    `COLUMN003` INT,\n");
        sb.append("    `COLUMN004` BIGINT,\n");
        sb.append("    `COLUMN005` FLOAT,\n");
        sb.append("    `COLUMN006` DOUBLE,\n");
        sb.append("    `COLUMN007` DECIMAL,\n");
        sb.append("    `COLUMN008` DECIMAL(p,s),\n");
        sb.append("    `COLUMN009` DECIMAL,\n");
        sb.append("    `COLUMN010` DECIMAL(p),\n");
        sb.append("    `COLUMN011` DECIMAL(p,s),\n");
        sb.append("    `COLUMN012` TIMESTAMP,\n");
        sb.append("    `COLUMN013` DATE,\n");
        sb.append("    `COLUMN014` STRING,\n");
        sb.append("    `COLUMN015` VARCHAR(n),\n");
        sb.append("    `COLUMN016` VARCHAR(n),\n");
        sb.append("    `COLUMN017` CHAR(n),\n");
        sb.append("    `COLUMN018` BOOLEAN,\n");
        sb.append("    `COLUMN019` BINARY)\n");

        if (partitionLevels > 0)
        {
            if (partitionLevels > 1)
            {
                // Multiple level partitioning.
                sb.append("PARTITIONED BY (`PRTN_CLMN001` DATE, `PRTN_CLMN002` STRING, `PRTN_CLMN003` INT, `PRTN_CLMN004` DECIMAL, " +
                    "`PRTN_CLMN005` BOOLEAN, `PRTN_CLMN006` DECIMAL, `PRTN_CLMN007` DECIMAL)\n");
            }
            else
            {
                // Single level partitioning.
                sb.append("PARTITIONED BY (`PRTN_CLMN001` DATE)\n");
            }
        }

        sb.append("[Row Format]\n");
        sb.append(String.format("STORED AS [Hive File Format]%s\n", partitionLevels > 0 ? ";" : ""));

        if (partitionLevels > 0)
        {
            // Add partitions if we have a non-empty list of partition values.
            if (!CollectionUtils.isEmpty(partitionValues))
            {
                // Add drop partition statements.
                if (isDropPartitionsStatementsIncluded)
                {
                    sb.append("\n");

                    for (String partitionValue : partitionValues)
                    {
                        sb.append(String
                            .format("ALTER TABLE `[Table Name]` DROP IF EXISTS PARTITION (`PRTN_CLMN00%d`='%s');\n", partitionColumnPosition, partitionValue));
                    }
                }

                sb.append("\n");

                for (String partitionValue : partitionValues)
                {
                    if (partitionLevels > 1)
                    {
                        // Adjust expected partition values based on the partition column position.
                        String testPrimaryPartitionValue =
                            partitionColumnPosition == BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION ? partitionValue :
                                AbstractServiceTest.PARTITION_VALUE;
                        List<String> testSubPartitionValues = new ArrayList<>(subPartitionValues);
                        if (partitionColumnPosition > BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION)
                        {
                            testSubPartitionValues.set(partitionColumnPosition - 2, partitionValue);
                        }

                        // Multiple level partitioning.
                        if (partitionLevels == AbstractServiceTest.SUBPARTITION_VALUES.size() + 1)
                        {
                            // No auto-discovery.
                            sb.append(String.format("ALTER TABLE `[Table Name]` ADD [If Not Exists]PARTITION (`PRTN_CLMN001`='%s', `PRTN_CLMN002`='%s', " +
                                    "`PRTN_CLMN003`='%s', `PRTN_CLMN004`='%s', `PRTN_CLMN005`='%s') " +
                                    "LOCATION 's3n://%s/ut-namespace-1-[Random Suffix]/ut-dataprovider-1-[Random Suffix]/ut-usage[Random Suffix]" +
                                    "/[Format File Type]/ut-businessobjectdefinition-name-1-[Random Suffix]/schm-v[Format Version]" +
                                    "/data-v[Data Version]/prtn-clmn001=%s/prtn-clmn002=%s/prtn-clmn003=%s/prtn-clmn004=%s/prtn-clmn005=%s';\n",
                                testPrimaryPartitionValue, testSubPartitionValues.get(0), testSubPartitionValues.get(1), testSubPartitionValues.get(2),
                                testSubPartitionValues.get(3), getExpectedS3BucketName(partitionValue), testPrimaryPartitionValue,
                                testSubPartitionValues.get(0), testSubPartitionValues.get(1), testSubPartitionValues.get(2), testSubPartitionValues.get(3)));
                        }
                        else
                        {
                            // Auto-discovery test template.
                            for (String binaryString : Arrays.asList("00", "01", "10", "11"))
                            {
                                sb.append(String.format("ALTER TABLE `[Table Name]` ADD [If Not Exists]PARTITION (`PRTN_CLMN001`='%s', `PRTN_CLMN002`='%s', " +
                                        "`PRTN_CLMN003`='%s', `PRTN_CLMN004`='%s', `PRTN_CLMN005`='%s', `PRTN_CLMN006`='%s', `PRTN_CLMN007`='%s') " +
                                        "LOCATION 's3n://%s/ut-namespace-1-[Random Suffix]/ut-dataprovider-1-[Random Suffix]/ut-usage[Random Suffix]" +
                                        "/[Format File Type]/ut-businessobjectdefinition-name-1-[Random Suffix]/schm-v[Format Version]" +
                                        "/data-v[Data Version]/prtn-clmn001=%s/prtn-clmn002=%s/prtn-clmn003=%s/prtn-clmn004=%s/prtn-clmn005=%s/" +
                                        (replaceUnderscoresWithHyphens ? "prtn-clmn006" : "prtn_clmn006") + "=%s/" +
                                        (replaceUnderscoresWithHyphens ? "prtn-clmn007" : "prtn_clmn007") + "=%s';\n", testPrimaryPartitionValue,
                                    testSubPartitionValues.get(0), testSubPartitionValues.get(1), testSubPartitionValues.get(2), testSubPartitionValues.get(3),
                                    binaryString.substring(0, 1), binaryString.substring(1, 2), getExpectedS3BucketName(partitionValue),
                                    testPrimaryPartitionValue, testSubPartitionValues.get(0), testSubPartitionValues.get(1), testSubPartitionValues.get(2),
                                    testSubPartitionValues.get(3), binaryString.substring(0, 1), binaryString.substring(1, 2)));
                            }
                        }
                    }
                    else
                    {
                        // Single level partitioning.
                        sb.append(String.format("ALTER TABLE `[Table Name]` ADD [If Not Exists]PARTITION (`PRTN_CLMN001`='%s') " +
                            "LOCATION 's3n://%s/ut-namespace-1-[Random Suffix]/ut-dataprovider-1-[Random Suffix]/ut-usage[Random Suffix]" +
                            "/[Format File Type]/ut-businessobjectdefinition-name-1-[Random Suffix]/schm-v[Format Version]" +
                            "/data-v[Data Version]/prtn-clmn001=%s';\n", partitionValue, getExpectedS3BucketName(partitionValue), partitionValue));
                    }
                }
            }
        }
        else if (!CollectionUtils.isEmpty(partitionValues))
        {
            // Add a location statement since the table is not partitioned and we have a non-empty list of partition values.
            sb.append(String.format("LOCATION 's3n://%s/ut-namespace-1-[Random Suffix]/ut-dataprovider-1-[Random Suffix]/ut-usage[Random Suffix]" +
                    "/txt/ut-businessobjectdefinition-name-1-[Random Suffix]/schm-v[Format Version]/data-v[Data Version]/partition=none';",
                getExpectedS3BucketName(Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE)));
        }
        else
        {
            // Add a location statement for a non-partitioned table for the business object format dll unit tests.
            sb.append("LOCATION '${non-partitioned.table.location}';");
        }

        String ddlTemplate = sb.toString().trim();
        Pattern pattern = Pattern.compile("\\[(.+?)\\]");
        Matcher matcher = pattern.matcher(ddlTemplate);
        HashMap<String, String> replacements = new HashMap<>();

        // Populate the replacements map.
        replacements.put("Table Name", AbstractServiceTest.TABLE_NAME);
        replacements.put("Random Suffix", AbstractServiceTest.RANDOM_SUFFIX);
        replacements.put("Format Version", String.valueOf(AbstractServiceTest.FORMAT_VERSION));
        replacements.put("Data Version", String.valueOf(AbstractServiceTest.DATA_VERSION));
        replacements.put("Row Format", hiveRowFormat);
        replacements.put("Hive File Format", hiveFileFormat);
        replacements.put("Format File Type", businessObjectFormatFileType.toLowerCase());
        replacements.put("If Not Exists", isIfNotExistsOptionIncluded ? "IF NOT EXISTS " : "");

        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (matcher.find())
        {
            String replacement = replacements.get(matcher.group(1));
            builder.append(ddlTemplate.substring(i, matcher.start()));
            if (replacement == null)
            {
                builder.append(matcher.group(0));
            }
            else
            {
                builder.append(replacement);
            }
            i = matcher.end();
        }

        builder.append(ddlTemplate.substring(i, ddlTemplate.length()));

        return builder.toString();
    }

    /**
     * Returns the actual HIVE DDL expected to be generated.
     *
     * @param partitionValue the partition value
     *
     * @return the actual HIVE DDL expected to be generated
     */
    public String getExpectedBusinessObjectDataDdl(String partitionValue)
    {
        return getExpectedBusinessObjectDataDdl(partitionValue, partitionValue);
    }

    /**
     * Returns the actual HIVE DDL expected to be generated.
     *
     * @param partitionValueToDrop the partition value to drop
     * @param partitionValueToAdd the partition value to add
     *
     * @return the actual HIVE DDL expected to be generated
     */
    public String getExpectedBusinessObjectDataDdl(String partitionValueToDrop, String partitionValueToAdd)
    {
        // Build ddl expected to be generated.
        StringBuilder ddlBuilder = new StringBuilder();
        ddlBuilder.append("DROP TABLE IF EXISTS `" + AbstractServiceTest.TABLE_NAME + "`;\n");
        ddlBuilder.append("\n");
        ddlBuilder.append("CREATE EXTERNAL TABLE IF NOT EXISTS `" + AbstractServiceTest.TABLE_NAME + "` (\n");
        ddlBuilder.append("    `ORGNL_" + AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME + "` DATE,\n");
        ddlBuilder.append("    `" + AbstractServiceTest.COLUMN_NAME + "` DECIMAL(" + AbstractServiceTest.COLUMN_SIZE + ") COMMENT '" +
            AbstractServiceTest.COLUMN_DESCRIPTION + "')\n");
        ddlBuilder.append("PARTITIONED BY (`" + AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME + "` DATE)\n");
        ddlBuilder.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' NULL DEFINED AS '\\N'\n");
        ddlBuilder.append("STORED AS TEXTFILE;");

        if (partitionValueToDrop != null)
        {
            // Add the alter table drop partition statement.
            ddlBuilder.append("\n\n");
            ddlBuilder.append(
                "ALTER TABLE `" + AbstractServiceTest.TABLE_NAME + "` DROP IF EXISTS PARTITION (`" + AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME + "`='" +
                    partitionValueToDrop + "');");
        }

        if (partitionValueToAdd != null)
        {
            // Build an expected S3 key prefix.
            String expectedS3KeyPrefix = AbstractServiceTest
                .getExpectedS3KeyPrefix(AbstractServiceTest.NAMESPACE, AbstractServiceTest.DATA_PROVIDER_NAME, AbstractServiceTest.BDEF_NAME,
                    AbstractServiceTest.FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FORMAT_VERSION,
                    AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME, partitionValueToAdd, null, null, AbstractServiceTest.DATA_VERSION);

            // Add the alter table add partition statement.
            ddlBuilder.append("\n\n");
            ddlBuilder.append(
                "ALTER TABLE `" + AbstractServiceTest.TABLE_NAME + "` ADD IF NOT EXISTS PARTITION (`" + AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME +
                    "`='" + partitionValueToAdd + "') LOCATION 's3n://" + AbstractServiceTest.S3_BUCKET_NAME + "/" + expectedS3KeyPrefix + "';");
        }

        String expectedDdl = ddlBuilder.toString();

        return expectedDdl;
    }

    /**
     * Creates an expected generate business object data ddl collection response using hard coded test values.
     *
     * @return the business object data ddl collection response
     */
    public BusinessObjectDataDdlCollectionResponse getExpectedBusinessObjectDataDdlCollectionResponse()
    {
        // Prepare a generate business object data collection response using hard coded test values.
        BusinessObjectDataDdlCollectionResponse businessObjectDataDdlCollectionResponse = new BusinessObjectDataDdlCollectionResponse();

        // Create a list of business object data ddl responses.
        List<BusinessObjectDataDdl> businessObjectDataDdlResponses = new ArrayList<>();
        businessObjectDataDdlCollectionResponse.setBusinessObjectDataDdlResponses(businessObjectDataDdlResponses);

        // Get the actual HIVE DDL expected to be generated.
        String expectedDdl = getExpectedBusinessObjectDataDdl(AbstractServiceTest.PARTITION_VALUE);

        // Create a business object data ddl response.
        BusinessObjectDataDdl expectedBusinessObjectDataDdl =
            new BusinessObjectDataDdl(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME, Arrays.asList(AbstractServiceTest.PARTITION_VALUE),
                    AbstractServiceTest.NO_PARTITION_VALUE_RANGE, AbstractServiceTest.NO_LATEST_BEFORE_PARTITION_VALUE,
                    AbstractServiceTest.NO_LATEST_AFTER_PARTITION_VALUE)), AbstractServiceTest.NO_STANDALONE_PARTITION_VALUE_FILTER,
                AbstractServiceTest.DATA_VERSION, AbstractServiceTest.NO_STORAGE_NAMES, AbstractServiceTest.STORAGE_NAME,
                BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, AbstractServiceTest.TABLE_NAME, AbstractServiceTest.NO_CUSTOM_DDL_NAME, expectedDdl);

        // Add two business object ddl responses to the collection response.
        businessObjectDataDdlResponses.add(expectedBusinessObjectDataDdl);
        businessObjectDataDdlResponses.add(expectedBusinessObjectDataDdl);

        // Set the expected DDL collection value.
        businessObjectDataDdlCollectionResponse.setDdlCollection(String.format("%s\n\n%s", expectedDdl, expectedDdl));

        return businessObjectDataDdlCollectionResponse;
    }

    /**
     * Returns the actual HIVE DDL expected to be generated.
     *
     * @param partitions the list of partitions, where each is represented by a primary value and a sub-partition value
     *
     * @return the actual HIVE DDL expected to be generated
     */
    public String getExpectedBusinessObjectDataDdlTwoPartitionLevels(List<List<String>> partitions)
    {
        // Build ddl expected to be generated.
        StringBuilder ddlBuilder = new StringBuilder();
        ddlBuilder.append("DROP TABLE IF EXISTS `" + AbstractServiceTest.TABLE_NAME + "`;\n");
        ddlBuilder.append("\n");
        ddlBuilder.append("CREATE EXTERNAL TABLE IF NOT EXISTS `" + AbstractServiceTest.TABLE_NAME + "` (\n");
        ddlBuilder.append("    `ORGNL_" + AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME + "` DATE,\n");
        ddlBuilder.append("    `ORGNL_" + AbstractServiceTest.SECOND_PARTITION_COLUMN_NAME + "` STRING,\n");
        ddlBuilder.append("    `" + AbstractServiceTest.COLUMN_NAME + "` DECIMAL(" + AbstractServiceTest.COLUMN_SIZE + ") COMMENT '" +
            AbstractServiceTest.COLUMN_DESCRIPTION + "')\n");
        ddlBuilder.append(
            "PARTITIONED BY (`" + AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME + "` DATE, `" + AbstractServiceTest.SECOND_PARTITION_COLUMN_NAME +
                "` STRING)\n");
        ddlBuilder.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' NULL DEFINED AS '\\N'\n");
        ddlBuilder.append("STORED AS TEXTFILE;");

        // Add the alter table drop partition statement.
        ddlBuilder.append("\n\n");
        ddlBuilder.append(
            "ALTER TABLE `" + AbstractServiceTest.TABLE_NAME + "` DROP IF EXISTS PARTITION (`" + AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME + "`='" +
                partitions.get(0).get(0) + "');");
        ddlBuilder.append("\n");

        for (List<String> partition : partitions)
        {
            // Build an expected S3 key prefix.
            String expectedS3KeyPrefix = AbstractServiceTest
                .getExpectedS3KeyPrefix(AbstractServiceTest.NAMESPACE, AbstractServiceTest.DATA_PROVIDER_NAME, AbstractServiceTest.BDEF_NAME,
                    AbstractServiceTest.FORMAT_USAGE_CODE, FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FORMAT_VERSION,
                    AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME, partition.get(0), Arrays.asList(
                        new SchemaColumn(AbstractServiceTest.SECOND_PARTITION_COLUMN_NAME, "STRING", AbstractServiceTest.NO_COLUMN_SIZE,
                            AbstractServiceTest.COLUMN_REQUIRED, AbstractServiceTest.NO_COLUMN_DEFAULT_VALUE, AbstractServiceTest.NO_COLUMN_DESCRIPTION))
                        .toArray(new SchemaColumn[1]), Arrays.asList(partition.get(1)).toArray(new String[1]), AbstractServiceTest.DATA_VERSION);

            // Add the alter table add partition statement.
            ddlBuilder.append("\n");
            ddlBuilder.append(
                "ALTER TABLE `" + AbstractServiceTest.TABLE_NAME + "` ADD IF NOT EXISTS PARTITION (`" + AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME +
                    "`='" + partition.get(0) + "', `" + AbstractServiceTest.SECOND_PARTITION_COLUMN_NAME + "`='" + partition.get(1) + "') LOCATION 's3n://" +
                    AbstractServiceTest.S3_BUCKET_NAME + "/" + expectedS3KeyPrefix + "';");
        }

        String expectedDdl = ddlBuilder.toString();

        return expectedDdl;
    }

    /**
     * Returns an expected string representation of the specified business object data key.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartitionValues the list of subpartition values
     * @param businessObjectDataVersion the business object data version
     *
     * @return the string representation of the specified business object data key
     */
    public String getExpectedBusinessObjectDataKeyAsString(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue, List<String> subPartitionValues,
        Integer businessObjectDataVersion)
    {
        return String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", " +
                "businessObjectDataSubPartitionValues: \"%s\", businessObjectDataVersion: %d", namespaceCode, businessObjectDefinitionName,
            businessObjectFormatUsage, businessObjectFormatFileType, businessObjectFormatVersion, partitionValue,
            CollectionUtils.isEmpty(subPartitionValues) ? "" : org.apache.commons.lang3.StringUtils.join(subPartitionValues, ","), businessObjectDataVersion);
    }

    /**
     * Returns an expected string representation of the specified business object data key.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the string representation of the specified business object data key
     */
    public String getExpectedBusinessObjectDataKeyAsString(BusinessObjectDataKey businessObjectDataKey)
    {
        return getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
            businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
            businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(), businessObjectDataKey.getSubPartitionValues(),
            businessObjectDataKey.getBusinessObjectDataVersion());
    }

    /**
     * Returns the business object data not found error message per specified parameters.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param partitionValue the partition value
     * @param subPartitionValues the list of subpartition values
     * @param businessObjectDataVersion the business object data version
     * @param businessObjectDataStatus the business object data status
     *
     * @return the business object data not found error message
     */
    public String getExpectedBusinessObjectDataNotFoundErrorMessage(String namespaceCode, String businessObjectDefinitionName, String businessObjectFormatUsage,
        String businessObjectFormatFileType, Integer businessObjectFormatVersion, String partitionValue, List<String> subPartitionValues,
        Integer businessObjectDataVersion, String businessObjectDataStatus)
    {
        return getExpectedBusinessObjectDataNotFoundErrorMessage(
            new BusinessObjectDataKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, subPartitionValues, businessObjectDataVersion), businessObjectDataStatus);
    }

    /**
     * Returns the business object data not found error message per specified parameters.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataStatus the business object data status
     *
     * @return the business object data not found error message
     */
    public String getExpectedBusinessObjectDataNotFoundErrorMessage(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
    {
        return String.format("Business object data {%s, businessObjectDataStatus: \"%s\"} doesn't exist.",
            getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey), businessObjectDataStatus);
    }

    /**
     * Gets the expected S3 bucket name per specified partition value.
     *
     * @param partitionValue the partition value
     *
     * @return the expected S3 bucket name
     */
    public String getExpectedS3BucketName(String partitionValue)
    {
        if (AbstractServiceTest.STORAGE_1_AVAILABLE_PARTITION_VALUES.contains(partitionValue) ||
            Hive13DdlGenerator.NO_PARTITIONING_PARTITION_VALUE.equals(partitionValue))
        {
            return AbstractServiceTest.S3_BUCKET_NAME;
        }
        else
        {
            return AbstractServiceTest.S3_BUCKET_NAME_2;
        }
    }

    /**
     * Returns a list of all possible invalid partition filters based on the presence of partition value filter elements. This helper method is for all negative
     * test cases covering partition value filter having none or more than one partition filter option specified.
     */
    public List<PartitionValueFilter> getInvalidPartitionValueFilters()
    {
        return Arrays.asList(
            new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.NO_PARTITION_VALUES, AbstractServiceTest.NO_PARTITION_VALUE_RANGE,
                AbstractServiceTest.NO_LATEST_BEFORE_PARTITION_VALUE, AbstractServiceTest.NO_LATEST_AFTER_PARTITION_VALUE),
            new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.NO_PARTITION_VALUES, AbstractServiceTest.NO_PARTITION_VALUE_RANGE,
                new LatestBeforePartitionValue(), new LatestAfterPartitionValue()),
            new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.NO_PARTITION_VALUES,
                new PartitionValueRange(AbstractServiceTest.START_PARTITION_VALUE, AbstractServiceTest.END_PARTITION_VALUE),
                AbstractServiceTest.NO_LATEST_BEFORE_PARTITION_VALUE, new LatestAfterPartitionValue()),
            new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.NO_PARTITION_VALUES,
                new PartitionValueRange(AbstractServiceTest.START_PARTITION_VALUE, AbstractServiceTest.END_PARTITION_VALUE), new LatestBeforePartitionValue(),
                AbstractServiceTest.NO_LATEST_AFTER_PARTITION_VALUE),
            new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.NO_PARTITION_VALUES,
                new PartitionValueRange(AbstractServiceTest.START_PARTITION_VALUE, AbstractServiceTest.END_PARTITION_VALUE), new LatestBeforePartitionValue(),
                new LatestAfterPartitionValue()), new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.UNSORTED_PARTITION_VALUES,
                AbstractServiceTest.NO_PARTITION_VALUE_RANGE, AbstractServiceTest.NO_LATEST_BEFORE_PARTITION_VALUE, new LatestAfterPartitionValue()),
            new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.UNSORTED_PARTITION_VALUES,
                AbstractServiceTest.NO_PARTITION_VALUE_RANGE, new LatestBeforePartitionValue(), AbstractServiceTest.NO_LATEST_AFTER_PARTITION_VALUE),
            new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.UNSORTED_PARTITION_VALUES,
                AbstractServiceTest.NO_PARTITION_VALUE_RANGE, new LatestBeforePartitionValue(), new LatestAfterPartitionValue()),
            new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.UNSORTED_PARTITION_VALUES,
                new PartitionValueRange(AbstractServiceTest.START_PARTITION_VALUE, AbstractServiceTest.END_PARTITION_VALUE),
                AbstractServiceTest.NO_LATEST_BEFORE_PARTITION_VALUE, AbstractServiceTest.NO_LATEST_AFTER_PARTITION_VALUE),
            new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.UNSORTED_PARTITION_VALUES,
                new PartitionValueRange(AbstractServiceTest.START_PARTITION_VALUE, AbstractServiceTest.END_PARTITION_VALUE),
                AbstractServiceTest.NO_LATEST_BEFORE_PARTITION_VALUE, new LatestAfterPartitionValue()),
            new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.UNSORTED_PARTITION_VALUES,
                new PartitionValueRange(AbstractServiceTest.START_PARTITION_VALUE, AbstractServiceTest.END_PARTITION_VALUE), new LatestBeforePartitionValue(),
                AbstractServiceTest.NO_LATEST_AFTER_PARTITION_VALUE),
            new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, AbstractServiceTest.UNSORTED_PARTITION_VALUES,
                new PartitionValueRange(AbstractServiceTest.START_PARTITION_VALUE, AbstractServiceTest.END_PARTITION_VALUE), new LatestBeforePartitionValue(),
                new LatestAfterPartitionValue()));
    }

    /**
     * Gets a new business object data create request with attributes and attribute definitions.
     *
     * @return the business object create request.
     */
    public BusinessObjectDataCreateRequest getNewBusinessObjectDataCreateRequest()
    {
        return getNewBusinessObjectDataCreateRequest(true);
    }

    /**
     * Gets a newly created business object data create request.
     *
     * @param includeAttributes If true, attribute definitions and attributes will be included. Otherwise, not.
     *
     * @return the business object create request.
     */
    public BusinessObjectDataCreateRequest getNewBusinessObjectDataCreateRequest(boolean includeAttributes)
    {
        // Crete a test business object format (and associated data).
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(includeAttributes);

        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity();

        // Create a request to create business object data.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = new BusinessObjectDataCreateRequest();
        businessObjectDataCreateRequest.setNamespace(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
        businessObjectDataCreateRequest.setBusinessObjectDefinitionName(businessObjectFormatEntity.getBusinessObjectDefinition().getName());
        businessObjectDataCreateRequest.setBusinessObjectFormatUsage(businessObjectFormatEntity.getUsage());
        businessObjectDataCreateRequest.setBusinessObjectFormatFileType(businessObjectFormatEntity.getFileType().getCode());
        businessObjectDataCreateRequest.setBusinessObjectFormatVersion(businessObjectFormatEntity.getBusinessObjectFormatVersion());
        businessObjectDataCreateRequest.setPartitionKey(businessObjectFormatEntity.getPartitionKey());
        businessObjectDataCreateRequest.setPartitionValue(AbstractServiceTest.PARTITION_VALUE);
        businessObjectDataCreateRequest.setSubPartitionValues(AbstractServiceTest.SUBPARTITION_VALUES);

        List<StorageUnitCreateRequest> storageUnits = new ArrayList<>();
        businessObjectDataCreateRequest.setStorageUnits(storageUnits);

        StorageUnitCreateRequest storageUnit = new StorageUnitCreateRequest();
        storageUnits.add(storageUnit);
        storageUnit.setStorageName(storageEntity.getName());

        StorageDirectory storageDirectory = new StorageDirectory();
        storageUnit.setStorageDirectory(storageDirectory);
        storageDirectory.setDirectoryPath("Folder");

        List<StorageFile> storageFiles = new ArrayList<>();
        storageUnit.setStorageFiles(storageFiles);

        StorageFile storageFile1 = new StorageFile();
        storageFiles.add(storageFile1);
        storageFile1.setFilePath("Folder/file1.gz");
        storageFile1.setFileSizeBytes(0L);
        storageFile1.setRowCount(0L);

        StorageFile storageFile2 = new StorageFile();
        storageFiles.add(storageFile2);
        storageFile2.setFilePath("Folder/file2.gz");
        storageFile2.setFileSizeBytes(2999L);
        storageFile2.setRowCount(1000L);

        StorageFile storageFile3 = new StorageFile();
        storageFiles.add(storageFile3);
        storageFile3.setFilePath("Folder/file3.gz");
        storageFile3.setFileSizeBytes(Long.MAX_VALUE);
        storageFile3.setRowCount(Long.MAX_VALUE);

        if (includeAttributes)
        {
            businessObjectDataCreateRequest.setAttributes(businessObjectDefinitionServiceTestHelper.getNewAttributes());
        }

        List<BusinessObjectDataKey> businessObjectDataParents = new ArrayList<>();
        businessObjectDataCreateRequest.setBusinessObjectDataParents(businessObjectDataParents);

        // Create 2 parents.
        for (int i = 0; i < 2; i++)
        {
            BusinessObjectDataEntity parentBusinessObjectDataEntity = businessObjectDataDaoTestHelper.createBusinessObjectDataEntity();
            BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
            businessObjectDataKey.setNamespace(parentBusinessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode());
            businessObjectDataKey
                .setBusinessObjectDefinitionName(parentBusinessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName());
            businessObjectDataKey.setBusinessObjectFormatUsage(parentBusinessObjectDataEntity.getBusinessObjectFormat().getUsage());
            businessObjectDataKey.setBusinessObjectFormatFileType(parentBusinessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode());
            businessObjectDataKey.setBusinessObjectFormatVersion(parentBusinessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
            businessObjectDataKey.setPartitionValue(parentBusinessObjectDataEntity.getPartitionValue());
            businessObjectDataKey.setBusinessObjectDataVersion(parentBusinessObjectDataEntity.getVersion());
            businessObjectDataKey.setSubPartitionValues(businessObjectDataHelper.getSubPartitionValues(parentBusinessObjectDataEntity));

            businessObjectDataParents.add(businessObjectDataKey);
        }

        return businessObjectDataCreateRequest;
    }

    public BusinessObjectDataStorageFilesCreateRequest getNewBusinessObjectDataStorageFilesCreateRequest()
    {
        // Crete a test business object format (and associated data).
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(false);
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = businessObjectDataStatusDaoTestHelper
            .createBusinessObjectDataStatusEntity(AbstractServiceTest.BDATA_STATUS, AbstractServiceTest.DESCRIPTION,
                AbstractServiceTest.BDATA_STATUS_PRE_REGISTRATION_FLAG_SET);
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, AbstractServiceTest.PARTITION_VALUE, AbstractServiceTest.DATA_VERSION,
                AbstractServiceTest.LATEST_VERSION_FLAG_SET, businessObjectDataStatusEntity.getCode());
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity();
        storageUnitDaoTestHelper
            .createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, AbstractServiceTest.NO_STORAGE_DIRECTORY_PATH);

        // Create a business object data storage files create request.
        BusinessObjectDataStorageFilesCreateRequest businessObjectDataStorageFilesCreateRequest = new BusinessObjectDataStorageFilesCreateRequest();
        businessObjectDataStorageFilesCreateRequest.setNamespace(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
        businessObjectDataStorageFilesCreateRequest.setBusinessObjectDefinitionName(businessObjectFormatEntity.getBusinessObjectDefinition().getName());
        businessObjectDataStorageFilesCreateRequest.setBusinessObjectFormatUsage(businessObjectFormatEntity.getUsage());
        businessObjectDataStorageFilesCreateRequest.setBusinessObjectFormatFileType(businessObjectFormatEntity.getFileType().getCode());
        businessObjectDataStorageFilesCreateRequest.setBusinessObjectFormatVersion(businessObjectFormatEntity.getBusinessObjectFormatVersion());
        businessObjectDataStorageFilesCreateRequest.setPartitionValue(businessObjectDataEntity.getPartitionValue());
        businessObjectDataStorageFilesCreateRequest.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());
        businessObjectDataStorageFilesCreateRequest.setStorageName(storageEntity.getName());
        businessObjectDataStorageFilesCreateRequest.setStorageFiles(getStorageFiles());

        return businessObjectDataStorageFilesCreateRequest;
    }

    /**
     * Creates a check business object data availability collection request using hard coded test values.
     *
     * @return the business object data availability collection request
     */
    public BusinessObjectDataAvailabilityCollectionRequest getTestBusinessObjectDataAvailabilityCollectionRequest()
    {
        // Create a check business object data availability collection request.
        BusinessObjectDataAvailabilityCollectionRequest businessObjectDataAvailabilityCollectionRequest = new BusinessObjectDataAvailabilityCollectionRequest();

        // Create a list of check business object data availability requests.
        List<BusinessObjectDataAvailabilityRequest> businessObjectDataAvailabilityRequests = new ArrayList<>();
        businessObjectDataAvailabilityCollectionRequest.setBusinessObjectDataAvailabilityRequests(businessObjectDataAvailabilityRequests);

        // Create a business object data availability request.
        BusinessObjectDataAvailabilityRequest businessObjectDataAvailabilityRequest =
            new BusinessObjectDataAvailabilityRequest(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                AbstractServiceTest.FORMAT_FILE_TYPE_CODE, AbstractServiceTest.FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(AbstractServiceTest.PARTITION_KEY, Arrays.asList(AbstractServiceTest.PARTITION_VALUE),
                    AbstractServiceTest.NO_PARTITION_VALUE_RANGE, AbstractServiceTest.NO_LATEST_BEFORE_PARTITION_VALUE,
                    AbstractServiceTest.NO_LATEST_AFTER_PARTITION_VALUE)), null, AbstractServiceTest.DATA_VERSION, AbstractServiceTest.NO_STORAGE_NAMES,
                AbstractServiceTest.STORAGE_NAME, AbstractServiceTest.NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS);
        businessObjectDataAvailabilityRequests.add(businessObjectDataAvailabilityRequest);

        return businessObjectDataAvailabilityCollectionRequest;
    }

    /**
     * Creates and returns a business object data availability request using passed parameters along with some hard-coded test values.
     *
     * @param partitionKey the partition key
     * @param startPartitionValue the start partition value for the partition value range
     * @param endPartitionValue the end partition value for the partition value range
     * @param partitionValues the list of partition values
     *
     * @return the newly created business object data availability request
     */
    public BusinessObjectDataAvailabilityRequest getTestBusinessObjectDataAvailabilityRequest(String partitionKey, String startPartitionValue,
        String endPartitionValue, List<String> partitionValues)
    {
        BusinessObjectDataAvailabilityRequest request = new BusinessObjectDataAvailabilityRequest();

        request.setNamespace(AbstractServiceTest.NAMESPACE);
        request.setBusinessObjectDefinitionName(AbstractServiceTest.BDEF_NAME);
        request.setBusinessObjectFormatUsage(AbstractServiceTest.FORMAT_USAGE_CODE);
        request.setBusinessObjectFormatFileType(AbstractServiceTest.FORMAT_FILE_TYPE_CODE);
        request.setBusinessObjectFormatVersion(AbstractServiceTest.FORMAT_VERSION);

        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        request.setPartitionValueFilters(Arrays.asList(partitionValueFilter));
        partitionValueFilter.setPartitionKey(partitionKey);

        if (startPartitionValue != null || endPartitionValue != null)
        {
            PartitionValueRange partitionValueRange = new PartitionValueRange();
            partitionValueFilter.setPartitionValueRange(partitionValueRange);
            partitionValueRange.setStartPartitionValue(startPartitionValue);
            partitionValueRange.setEndPartitionValue(endPartitionValue);
        }

        if (partitionValues != null)
        {
            partitionValueFilter.setPartitionValues(new ArrayList<>(partitionValues));
        }

        request.setBusinessObjectDataVersion(AbstractServiceTest.DATA_VERSION);
        request.setStorageName(AbstractServiceTest.STORAGE_NAME);
        request.setIncludeAllRegisteredSubPartitions(AbstractServiceTest.NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS);

        return request;
    }

    /**
     * Creates and returns a business object data availability request using passed parameters along with some hard-coded test values.
     *
     * @param partitionKey the partition key
     * @param partitionValues the list of partition values
     *
     * @return the newly created business object data availability request
     */
    public BusinessObjectDataAvailabilityRequest getTestBusinessObjectDataAvailabilityRequest(String partitionKey, List<String> partitionValues)
    {
        return getTestBusinessObjectDataAvailabilityRequest(partitionKey, null, null, partitionValues);
    }

    /**
     * Creates and returns a business object data availability request using passed parameters along with some hard-coded test values.
     *
     * @param startPartitionValue the start partition value for the partition value range
     * @param endPartitionValue the end partition value for the partition value range
     *
     * @return the newly created business object data availability request
     */
    public BusinessObjectDataAvailabilityRequest getTestBusinessObjectDataAvailabilityRequest(String startPartitionValue, String endPartitionValue)
    {
        return getTestBusinessObjectDataAvailabilityRequest(AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME, startPartitionValue, endPartitionValue, null);
    }

    /**
     * Creates and returns a business object data availability request using passed parameters along with some hard-coded test values.
     *
     * @param partitionValues the list of partition values
     *
     * @return the newly created business object data availability request
     */
    public BusinessObjectDataAvailabilityRequest getTestBusinessObjectDataAvailabilityRequest(List<String> partitionValues)
    {
        return getTestBusinessObjectDataAvailabilityRequest(AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME, null, null, partitionValues);
    }

    /**
     * Creates a generate business object data ddl collection request using hard coded test values.
     *
     * @return the business object data ddl collection request
     */
    public BusinessObjectDataDdlCollectionRequest getTestBusinessObjectDataDdlCollectionRequest()
    {
        // Create a generate business object data ddl collection request.
        BusinessObjectDataDdlCollectionRequest businessObjectDataDdlCollectionRequest = new BusinessObjectDataDdlCollectionRequest();

        // Create a list of generate business object data ddl requests.
        List<BusinessObjectDataDdlRequest> businessObjectDataDdlRequests = new ArrayList<>();
        businessObjectDataDdlCollectionRequest.setBusinessObjectDataDdlRequests(businessObjectDataDdlRequests);

        // Create a generate business object data ddl request.
        BusinessObjectDataDdlRequest businessObjectDataDdlRequest =
            new BusinessObjectDataDdlRequest(AbstractServiceTest.NAMESPACE, AbstractServiceTest.BDEF_NAME, AbstractServiceTest.FORMAT_USAGE_CODE,
                FileTypeEntity.TXT_FILE_TYPE, AbstractServiceTest.FORMAT_VERSION, Arrays.asList(
                new PartitionValueFilter(AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME, Arrays.asList(AbstractServiceTest.PARTITION_VALUE),
                    AbstractServiceTest.NO_PARTITION_VALUE_RANGE, AbstractServiceTest.NO_LATEST_BEFORE_PARTITION_VALUE,
                    AbstractServiceTest.NO_LATEST_AFTER_PARTITION_VALUE)), AbstractServiceTest.NO_STANDALONE_PARTITION_VALUE_FILTER,
                AbstractServiceTest.DATA_VERSION, AbstractServiceTest.NO_STORAGE_NAMES, AbstractServiceTest.STORAGE_NAME,
                BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL, AbstractServiceTest.TABLE_NAME, AbstractServiceTest.NO_CUSTOM_DDL_NAME,
                AbstractServiceTest.INCLUDE_DROP_TABLE_STATEMENT, AbstractServiceTest.INCLUDE_IF_NOT_EXISTS_OPTION, AbstractServiceTest.INCLUDE_DROP_PARTITIONS,
                AbstractServiceTest.NO_ALLOW_MISSING_DATA, AbstractServiceTest.NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS,
                AbstractServiceTest.NO_SUPPRESS_SCAN_FOR_UNREGISTERED_SUBPARTITIONS);

        // Add two business object ddl requests to the collection request.
        businessObjectDataDdlRequests.add(businessObjectDataDdlRequest);
        businessObjectDataDdlRequests.add(businessObjectDataDdlRequest);

        return businessObjectDataDdlCollectionRequest;
    }

    /**
     * Creates and returns a business object data ddl request using passed parameters along with some hard-coded test values.
     *
     * @param startPartitionValue the start partition value for the partition value range
     * @param endPartitionValue the end partition value for the partition value range
     * @param partitionValues the list of partition values
     * @param customDdlName the custom DDL name
     *
     * @return the newly created business object data ddl request
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public BusinessObjectDataDdlRequest getTestBusinessObjectDataDdlRequest(String startPartitionValue, String endPartitionValue, List<String> partitionValues,
        String customDdlName)
    {
        BusinessObjectDataDdlRequest request = new BusinessObjectDataDdlRequest();

        request.setNamespace(AbstractServiceTest.NAMESPACE);
        request.setBusinessObjectDefinitionName(AbstractServiceTest.BDEF_NAME);
        request.setBusinessObjectFormatUsage(AbstractServiceTest.FORMAT_USAGE_CODE);
        request.setBusinessObjectFormatFileType(FileTypeEntity.TXT_FILE_TYPE);
        request.setBusinessObjectFormatVersion(AbstractServiceTest.FORMAT_VERSION);

        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        request.setPartitionValueFilters(Arrays.asList(partitionValueFilter));
        partitionValueFilter.setPartitionKey(AbstractServiceTest.FIRST_PARTITION_COLUMN_NAME);

        if (startPartitionValue != null || endPartitionValue != null)
        {
            PartitionValueRange partitionValueRange = new PartitionValueRange();
            partitionValueFilter.setPartitionValueRange(partitionValueRange);
            partitionValueRange.setStartPartitionValue(startPartitionValue);
            partitionValueRange.setEndPartitionValue(endPartitionValue);
        }

        if (partitionValues != null)
        {
            partitionValueFilter.setPartitionValues(new ArrayList(partitionValues));
        }

        request.setBusinessObjectDataVersion(AbstractServiceTest.DATA_VERSION);
        request.setStorageName(AbstractServiceTest.STORAGE_NAME);
        request.setOutputFormat(BusinessObjectDataDdlOutputFormatEnum.HIVE_13_DDL);
        request.setTableName(AbstractServiceTest.TABLE_NAME);
        request.setCustomDdlName(customDdlName);
        request.setIncludeDropTableStatement(true);
        request.setIncludeIfNotExistsOption(true);
        request.setAllowMissingData(true);
        request.setIncludeAllRegisteredSubPartitions(AbstractServiceTest.NO_INCLUDE_ALL_REGISTERED_SUBPARTITIONS);

        return request;
    }

    /**
     * Creates and returns a business object data ddl request using passed parameters along with some hard-coded test values.
     *
     * @param partitionValues the list of partition values
     *
     * @return the newly created business object data ddl request
     */
    public BusinessObjectDataDdlRequest getTestBusinessObjectDataDdlRequest(List<String> partitionValues)
    {
        return getTestBusinessObjectDataDdlRequest(null, null, partitionValues, AbstractServiceTest.NO_CUSTOM_DDL_NAME);
    }

    /**
     * Creates and returns a business object data ddl request using passed parameters along with some hard-coded test values.
     *
     * @param startPartitionValue the start partition value for the partition value range
     * @param endPartitionValue the end partition value for the partition value range
     *
     * @return the newly created business object data ddl request
     */
    public BusinessObjectDataDdlRequest getTestBusinessObjectDataDdlRequest(String startPartitionValue, String endPartitionValue)
    {
        return getTestBusinessObjectDataDdlRequest(startPartitionValue, endPartitionValue, null, AbstractServiceTest.NO_CUSTOM_DDL_NAME);
    }

    /**
     * Creates and returns a business object data ddl request using passed parameters along with some hard-coded test values.
     *
     * @param partitionValues the list of partition values
     * @param customDdlName the custom DDL name
     *
     * @return the newly created business object data ddl request
     */
    public BusinessObjectDataDdlRequest getTestBusinessObjectDataDdlRequest(List<String> partitionValues, String customDdlName)
    {
        return getTestBusinessObjectDataDdlRequest(null, null, partitionValues, customDdlName);
    }

    /**
     * Creates and returns a business object data ddl request using passed parameters along with some hard-coded test values.
     *
     * @param startPartitionValue the start partition value for the partition value range
     * @param endPartitionValue the end partition value for the partition value range * @param customDdlName the custom DDL name
     * @param customDdlName the custom DDL name
     *
     * @return the newly created business object data ddl request
     */
    public BusinessObjectDataDdlRequest getTestBusinessObjectDataDdlRequest(String startPartitionValue, String endPartitionValue, String customDdlName)
    {
        return getTestBusinessObjectDataDdlRequest(startPartitionValue, endPartitionValue, null, customDdlName);
    }

    /**
     * Creates and returns a list of business object data status elements initialised per provided parameters.
     *
     * @param businessObjectFormatVersion the business object format version
     * @param partitionColumnPosition the position of the partition column (one-based numbering)
     * @param partitionValues the list of partition values
     * @param subPartitionValues the list of subpartition values
     * @param businessObjectDataVersion the business object data version
     * @param reason the reason for the not available business object data
     * @param useSinglePartitionValue specifies if not available statuses should be generated using single partition value logic
     *
     * @return the newly created list of business object data status elements
     */
    public List<BusinessObjectDataStatus> getTestBusinessObjectDataStatuses(Integer businessObjectFormatVersion, int partitionColumnPosition,
        List<String> partitionValues, List<String> subPartitionValues, Integer businessObjectDataVersion, String reason, boolean useSinglePartitionValue)
    {
        List<BusinessObjectDataStatus> businessObjectDataStatuses = new ArrayList<>();

        if (partitionValues != null)
        {
            for (String partitionValue : partitionValues)
            {
                BusinessObjectDataStatus businessObjectDataStatus = new BusinessObjectDataStatus();
                businessObjectDataStatuses.add(businessObjectDataStatus);
                businessObjectDataStatus.setBusinessObjectFormatVersion(businessObjectFormatVersion);
                businessObjectDataStatus.setBusinessObjectDataVersion(businessObjectDataVersion);
                businessObjectDataStatus.setReason(reason);

                if (BusinessObjectDataServiceImpl.REASON_NOT_REGISTERED.equals(reason))
                {
                    // We are generating business object data status for a not registered business object data.
                    if (partitionColumnPosition == BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION)
                    {
                        // This is a not-available not-registered business object data searched on a primary partition.
                        businessObjectDataStatus.setPartitionValue(partitionValue);
                        businessObjectDataStatus.setSubPartitionValues(useSinglePartitionValue ? null : Arrays.asList("", "", "", ""));
                    }
                    else
                    {
                        // This is a not-available not-registered business object data searched on a sub-partition value.
                        if (useSinglePartitionValue)
                        {
                            businessObjectDataStatus.setPartitionValue(partitionValue);
                        }
                        else
                        {
                            businessObjectDataStatus.setPartitionValue("");
                            businessObjectDataStatus.setSubPartitionValues(Arrays.asList("", "", "", ""));
                            businessObjectDataStatus.getSubPartitionValues().set(partitionColumnPosition - 2, partitionValue);
                        }
                    }
                }
                else if (partitionColumnPosition == BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION)
                {
                    // This is a found business object data selected on primary partition value.
                    businessObjectDataStatus.setPartitionValue(partitionValue);
                    businessObjectDataStatus.setSubPartitionValues(subPartitionValues);
                }
                else
                {
                    // This is a found business object data selected on a subpartition column.
                    businessObjectDataStatus.setPartitionValue(AbstractServiceTest.PARTITION_VALUE);
                    List<String> testSubPartitionValues = new ArrayList<>(subPartitionValues);
                    // Please note that the value of the second partition column is located at index 0.
                    testSubPartitionValues.set(partitionColumnPosition - 2, partitionValue);
                    businessObjectDataStatus.setSubPartitionValues(testSubPartitionValues);
                }
            }
        }

        return businessObjectDataStatuses;
    }

    /**
     * Builds and returns a list of test storage file object instances.
     *
     * @param s3KeyPrefix the S3 key prefix
     * @param relativeFilePaths the list of relative file paths that might include sub-directories
     *
     * @return the newly created list of storage files
     */
    public List<StorageFile> getTestStorageFiles(String s3KeyPrefix, List<String> relativeFilePaths)
    {
        return getTestStorageFiles(s3KeyPrefix, relativeFilePaths, true);
    }

    /**
     * Builds and returns a list of test storage file object instances.
     *
     * @param s3KeyPrefix the S3 key prefix
     * @param relativeFilePaths the list of relative file paths that might include sub-directories,
     * @param setRowCount specifies if some storage files should get row count attribute set to a hard coded test value
     *
     * @return the newly created list of storage files
     */
    public List<StorageFile> getTestStorageFiles(String s3KeyPrefix, List<String> relativeFilePaths, boolean setRowCount)
    {
        // Build a list of storage files.
        List<StorageFile> storageFiles = new ArrayList<>();

        for (String file : relativeFilePaths)
        {
            StorageFile storageFile = new StorageFile();
            storageFiles.add(storageFile);
            storageFile.setFilePath(s3KeyPrefix + "/" + file.replaceAll("\\\\", "/"));
            storageFile.setFileSizeBytes(AbstractServiceTest.FILE_SIZE_1_KB);

            if (setRowCount)
            {
                // Row count is an optional field, so let's not set it for one of the storage files - this is required for code coverage.
                storageFile.setRowCount(file.equals(AbstractServiceTest.LOCAL_FILES.get(0)) ? null : AbstractServiceTest.ROW_COUNT_1000);
            }
        }

        return storageFiles;
    }

    /**
     * Creates specified list of files in the local temporary directory and uploads them to the test S3 bucket.
     *
     * @param s3keyPrefix the destination S3 key prefix
     * @param localTempPath the local temporary directory
     * @param localFilePaths the list of local files that might include sub-directories
     *
     * @throws Exception
     */
    public void prepareTestS3Files(String s3keyPrefix, Path localTempPath, List<String> localFilePaths) throws Exception
    {
        prepareTestS3Files(s3keyPrefix, localTempPath, localFilePaths, new ArrayList<>());
    }

    /**
     * Creates specified list of files in the local temporary directory and uploads them to the test S3 bucket. This method also creates 0 byte S3 directory
     * markers relative to the s3 key prefix.
     *
     * @param s3KeyPrefix the destination S3 key prefix
     * @param localTempPath the local temporary directory
     * @param localFilePaths the list of local files that might include sub-directories
     * @param directoryPaths the list of directory paths to be created in S3 relative to the S3 key prefix
     *
     * @throws Exception
     */
    public void prepareTestS3Files(String s3KeyPrefix, Path localTempPath, List<String> localFilePaths, List<String> directoryPaths) throws Exception
    {
        prepareTestS3Files(null, s3KeyPrefix, localTempPath, localFilePaths, directoryPaths);
    }

    /**
     * Creates specified list of files in the local temporary directory and uploads them to the test S3 bucket. This method also creates 0 byte S3 directory
     * markers relative to the s3 key prefix.
     *
     * @param bucketName the bucket name in S3 to place the files.
     * @param s3KeyPrefix the destination S3 key prefix
     * @param localTempPath the local temporary directory
     * @param localFilePaths the list of local files that might include sub-directories
     * @param directoryPaths the list of directory paths to be created in S3 relative to the S3 key prefix
     *
     * @throws Exception
     */
    public void prepareTestS3Files(String bucketName, String s3KeyPrefix, Path localTempPath, List<String> localFilePaths, List<String> directoryPaths)
        throws Exception
    {
        // Create local test files.
        for (String file : localFilePaths)
        {
            AbstractServiceTest.createLocalFile(localTempPath.toString(), file, AbstractServiceTest.FILE_SIZE_1_KB);
        }

        // Upload test file to S3.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        if (bucketName != null)
        {
            s3FileTransferRequestParamsDto.setS3BucketName(bucketName);
        }
        s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
        s3FileTransferRequestParamsDto.setLocalPath(localTempPath.toString());
        s3FileTransferRequestParamsDto.setRecursive(true);
        S3FileTransferResultsDto results = s3Service.uploadDirectory(s3FileTransferRequestParamsDto);

        // Validate the transfer result.
        assertEquals(Long.valueOf(localFilePaths.size()), results.getTotalFilesTransferred());

        // Create 0 byte S3 directory markers.
        for (String directoryPath : directoryPaths)
        {
            // Create 0 byte directory marker.
            s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix + "/" + directoryPath);
            s3Service.createDirectory(s3FileTransferRequestParamsDto);
        }

        // Validate the uploaded S3 files and created directory markers, if any.
        s3FileTransferRequestParamsDto.setS3KeyPrefix(s3KeyPrefix);
        assertEquals(localFilePaths.size() + directoryPaths.size(), s3Service.listDirectory(s3FileTransferRequestParamsDto).size());
    }

    /**
     * Validates business object data against specified arguments and expected (hard coded) test values.
     *
     * @param request the business object data create request
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedLatestVersion the expected business
     * @param actualBusinessObjectData the business object data availability object instance to be validated
     */
    public void validateBusinessObjectData(BusinessObjectDataCreateRequest request, Integer expectedBusinessObjectDataVersion, Boolean expectedLatestVersion,
        BusinessObjectData actualBusinessObjectData)
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(new BusinessObjectFormatKey(
            org.apache.commons.lang3.StringUtils.isNotBlank(request.getNamespace()) ? request.getNamespace() : AbstractServiceTest.NAMESPACE,
            request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(), request.getBusinessObjectFormatFileType(),
            request.getBusinessObjectFormatVersion()));

        List<String> expectedSubPartitionValues =
            CollectionUtils.isEmpty(request.getSubPartitionValues()) ? new ArrayList<>() : request.getSubPartitionValues();

        String expectedStatusCode =
            org.apache.commons.lang3.StringUtils.isNotBlank(request.getStatus()) ? request.getStatus() : BusinessObjectDataStatusEntity.VALID;

        StorageUnitCreateRequest storageUnitCreateRequest = request.getStorageUnits().get(0);

        StorageEntity storageEntity = storageDao.getStorageByName(storageUnitCreateRequest.getStorageName());

        String expectedStorageDirectoryPath =
            storageUnitCreateRequest.getStorageDirectory() != null ? storageUnitCreateRequest.getStorageDirectory().getDirectoryPath() : null;

        List<StorageFile> expectedStorageFiles =
            CollectionUtils.isEmpty(storageUnitCreateRequest.getStorageFiles()) ? null : storageUnitCreateRequest.getStorageFiles();

        List<Attribute> expectedAttributes = CollectionUtils.isEmpty(request.getAttributes()) ? new ArrayList<>() : request.getAttributes();

        validateBusinessObjectData(businessObjectFormatEntity, request.getPartitionValue(), expectedSubPartitionValues, expectedBusinessObjectDataVersion,
            expectedLatestVersion, expectedStatusCode, storageEntity.getName(), expectedStorageDirectoryPath, expectedStorageFiles, expectedAttributes,
            actualBusinessObjectData);
    }

    /**
     * Validates business object data against specified arguments and expected (hard coded) test values.
     *
     * @param businessObjectFormatEntity the business object format entity that this business object data belongs to
     * @param expectedBusinessObjectDataPartitionValue the expected partition value for this business object data
     * @param expectedBusinessObjectDataSubPartitionValues the expected subpartition values for this business object data
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedLatestVersion the expected business
     * @param expectedStatusCode the expected business object data status code
     * @param expectedStorageName the expected storage name
     * @param expectedStorageDirectoryPath the expected storage directory path
     * @param expectedStorageFiles the expected storage files
     * @param expectedAttributes the expected attributes
     * @param actualBusinessObjectData the business object data availability object instance to be validated
     */
    public void validateBusinessObjectData(BusinessObjectFormatEntity businessObjectFormatEntity, String expectedBusinessObjectDataPartitionValue,
        List<String> expectedBusinessObjectDataSubPartitionValues, Integer expectedBusinessObjectDataVersion, Boolean expectedLatestVersion,
        String expectedStatusCode, String expectedStorageName, String expectedStorageDirectoryPath, List<StorageFile> expectedStorageFiles,
        List<Attribute> expectedAttributes, BusinessObjectData actualBusinessObjectData)
    {
        validateBusinessObjectData(null, businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
            businessObjectFormatEntity.getFileType().getCode(), businessObjectFormatEntity.getBusinessObjectFormatVersion(),
            expectedBusinessObjectDataPartitionValue, expectedBusinessObjectDataSubPartitionValues, expectedBusinessObjectDataVersion, expectedLatestVersion,
            expectedStatusCode, expectedStorageName, expectedStorageDirectoryPath, expectedStorageFiles, expectedAttributes, actualBusinessObjectData);
    }

    /**
     * Validates business object data against specified arguments and expected (hard coded) test values.
     *
     * @param expectedBusinessObjectDataId the expected business object data ID
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedBusinessObjectDataPartitionValue the expected partition value for this business object data
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedLatestVersion the expected business
     * @param expectedStatusCode the expected business object data status code
     * @param expectedStorageName the expected storage name
     * @param expectedStorageDirectoryPath the expected storage directory path
     * @param expectedStorageFiles the expected storage files
     * @param expectedAttributes the expected attributes
     * @param actualBusinessObjectData the business object data availability object instance to be validated
     */
    public void validateBusinessObjectData(Integer expectedBusinessObjectDataId, String expectedNamespace, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        String expectedBusinessObjectDataPartitionValue, List<String> expectedBusinessObjectDataSubPartitionValues, Integer expectedBusinessObjectDataVersion,
        Boolean expectedLatestVersion, String expectedStatusCode, String expectedStorageName, String expectedStorageDirectoryPath,
        List<StorageFile> expectedStorageFiles, List<Attribute> expectedAttributes, BusinessObjectData actualBusinessObjectData)
    {
        validateBusinessObjectData(expectedBusinessObjectDataId, expectedNamespace, expectedBusinessObjectDefinitionName, expectedBusinessObjectFormatUsage,
            expectedBusinessObjectFormatFileType, expectedBusinessObjectFormatVersion, expectedBusinessObjectDataPartitionValue,
            expectedBusinessObjectDataSubPartitionValues, expectedBusinessObjectDataVersion, expectedLatestVersion, expectedStatusCode,
            actualBusinessObjectData);

        // We expected test business object data to contain a single storage unit.
        assertEquals(1, actualBusinessObjectData.getStorageUnits().size());
        StorageUnit actualStorageUnit = actualBusinessObjectData.getStorageUnits().get(0);

        assertEquals(expectedStorageName, actualStorageUnit.getStorage().getName());
        assertEquals(expectedStorageDirectoryPath,
            actualStorageUnit.getStorageDirectory() != null ? actualStorageUnit.getStorageDirectory().getDirectoryPath() : null);
        AbstractServiceTest.assertEqualsIgnoreOrder("storage files", expectedStorageFiles, actualStorageUnit.getStorageFiles());

        assertEquals(expectedAttributes, actualBusinessObjectData.getAttributes());
    }

    /**
     * Validates business object data against specified arguments.
     *
     * @param expectedBusinessObjectDataId the expected business object data ID
     * @param expectedBusinessObjectDataKey the expected business object data key
     * @param expectedLatestVersion the expected business
     * @param expectedStatusCode the expected business object data status code
     * @param actualBusinessObjectData the business object data availability object instance to be validated
     */
    public void validateBusinessObjectData(Integer expectedBusinessObjectDataId, BusinessObjectDataKey expectedBusinessObjectDataKey,
        Boolean expectedLatestVersion, String expectedStatusCode, BusinessObjectData actualBusinessObjectData)
    {
        validateBusinessObjectData(expectedBusinessObjectDataId, expectedBusinessObjectDataKey.getNamespace(),
            expectedBusinessObjectDataKey.getBusinessObjectDefinitionName(), expectedBusinessObjectDataKey.getBusinessObjectFormatUsage(),
            expectedBusinessObjectDataKey.getBusinessObjectFormatFileType(), expectedBusinessObjectDataKey.getBusinessObjectFormatVersion(),
            expectedBusinessObjectDataKey.getPartitionValue(), expectedBusinessObjectDataKey.getSubPartitionValues(),
            expectedBusinessObjectDataKey.getBusinessObjectDataVersion(), expectedLatestVersion, expectedStatusCode, actualBusinessObjectData);
    }

    /**
     * Validates business object data against specified arguments.
     *
     * @param expectedBusinessObjectDataId the expected business object data ID
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedBusinessObjectDataPartitionValue the expected partition value for this business object data
     * @param expectedBusinessObjectDataSubPartitionValues the expected subpartition values for this business object data
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedLatestVersion the expected business
     * @param expectedStatusCode the expected business object data status code
     * @param actualBusinessObjectData the business object data availability object instance to be validated
     */
    public void validateBusinessObjectData(Integer expectedBusinessObjectDataId, String expectedNamespace, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        String expectedBusinessObjectDataPartitionValue, List<String> expectedBusinessObjectDataSubPartitionValues, Integer expectedBusinessObjectDataVersion,
        Boolean expectedLatestVersion, String expectedStatusCode, BusinessObjectData actualBusinessObjectData)
    {
        assertNotNull(actualBusinessObjectData);

        if (expectedBusinessObjectDataId != null)
        {
            assertEquals(expectedBusinessObjectDataId, Integer.valueOf(actualBusinessObjectData.getId()));
        }

        assertEquals(expectedNamespace, actualBusinessObjectData.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualBusinessObjectData.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualBusinessObjectData.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualBusinessObjectData.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, Integer.valueOf(actualBusinessObjectData.getBusinessObjectFormatVersion()));
        assertEquals(expectedBusinessObjectDataPartitionValue, actualBusinessObjectData.getPartitionValue());
        assertEquals(expectedBusinessObjectDataSubPartitionValues, actualBusinessObjectData.getSubPartitionValues());
        assertEquals(expectedBusinessObjectDataVersion, Integer.valueOf(actualBusinessObjectData.getVersion()));
        assertEquals(expectedLatestVersion, actualBusinessObjectData.isLatestVersion());
        assertEquals(expectedStatusCode, actualBusinessObjectData.getStatus());
    }

    public void validateBusinessObjectData(String expectedNamespaceCode, String expectedBusinessObjectDefinitionName, String expectedBusinessObjectFormatUsage,
        String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion, String expectedBusinessObjectDataStatus,
        List<Attribute> expectedAttributes, String expectedStorageName, String expectedFileName, Long expectedFileSizeBytes,
        BusinessObjectData businessObjectData)
    {
        assertNotNull(businessObjectData);

        // Validate business object data alternate key values.
        assertEquals(expectedNamespaceCode, businessObjectData.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, businessObjectData.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, businessObjectData.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, businessObjectData.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, Integer.valueOf(businessObjectData.getBusinessObjectFormatVersion()));
        // The business object data partition value must contain an UUID value.
        assertNotNull(businessObjectData.getPartitionValue());
        assertEquals(AbstractServiceTest.EXPECTED_UUID_SIZE, businessObjectData.getPartitionValue().length());
        assertEquals(AbstractServiceTest.NO_SUBPARTITION_VALUES, businessObjectData.getSubPartitionValues());
        assertEquals(AbstractServiceTest.INITIAL_DATA_VERSION, Integer.valueOf(businessObjectData.getVersion()));

        // Validate business object data status.
        assertTrue(businessObjectData.isLatestVersion());
        assertEquals(expectedBusinessObjectDataStatus, businessObjectData.getStatus());

        // Validate business object data attributes.
        businessObjectDefinitionServiceTestHelper.validateAttributes(expectedAttributes, businessObjectData.getAttributes());

        // Validate storage unit contents.
        assertEquals(1, businessObjectData.getStorageUnits().size());
        StorageUnit storageUnit = businessObjectData.getStorageUnits().get(0);
        assertEquals(expectedStorageName, storageUnit.getStorage().getName());
        String expectedStorageDirectoryPath = String.format("%s/%s/%s", AbstractServiceTest.ENVIRONMENT_NAME.trim().toLowerCase().replace('_', '-'),
            expectedNamespaceCode.trim().toLowerCase().replace('_', '-'), businessObjectData.getPartitionValue());
        assertEquals(expectedStorageDirectoryPath, storageUnit.getStorageDirectory().getDirectoryPath());
        assertEquals(1, storageUnit.getStorageFiles().size());
        StorageFile storageFile = storageUnit.getStorageFiles().get(0);
        String expectedStorageFilePath = String.format("%s/%s", expectedStorageDirectoryPath, expectedFileName);
        assertEquals(expectedStorageFilePath, storageFile.getFilePath());
        assertEquals(expectedFileSizeBytes, storageFile.getFileSizeBytes());
        assertEquals(null, storageFile.getRowCount());
    }

    /**
     * Validates business object data availability against specified arguments and expected (hard coded) test values.
     *
     * @param request the business object data availability request
     * @param actualBusinessObjectDataAvailability the business object data availability object instance to be validated
     */
    public void validateBusinessObjectDataAvailability(BusinessObjectDataAvailabilityRequest request, List<BusinessObjectDataStatus> expectedAvailableStatuses,
        List<BusinessObjectDataStatus> expectedNotAvailableStatuses, BusinessObjectDataAvailability actualBusinessObjectDataAvailability)
    {
        assertNotNull(actualBusinessObjectDataAvailability);
        assertEquals(request.getNamespace(), actualBusinessObjectDataAvailability.getNamespace());
        assertEquals(request.getBusinessObjectDefinitionName(), actualBusinessObjectDataAvailability.getBusinessObjectDefinitionName());
        assertEquals(request.getBusinessObjectFormatUsage(), actualBusinessObjectDataAvailability.getBusinessObjectFormatUsage());
        assertEquals(request.getBusinessObjectFormatFileType(), actualBusinessObjectDataAvailability.getBusinessObjectFormatFileType());
        assertEquals(request.getBusinessObjectFormatVersion(), actualBusinessObjectDataAvailability.getBusinessObjectFormatVersion());
        assertEquals(request.getPartitionValueFilter(), actualBusinessObjectDataAvailability.getPartitionValueFilter());
        assertEquals(request.getBusinessObjectDataVersion(), actualBusinessObjectDataAvailability.getBusinessObjectDataVersion());
        assertEquals(request.getStorageName(), actualBusinessObjectDataAvailability.getStorageName());
        assertEquals(expectedAvailableStatuses, actualBusinessObjectDataAvailability.getAvailableStatuses());
        assertEquals(expectedNotAvailableStatuses, actualBusinessObjectDataAvailability.getNotAvailableStatuses());
    }

    /**
     * Validates business object data ddl object instance against specified arguments and expected (hard coded) test values.
     *
     * @param request the business object ddl request
     * @param actualBusinessObjectDataDdl the business object data ddl object instance to be validated
     */
    public void validateBusinessObjectDataDdl(BusinessObjectDataDdlRequest request, String expectedDdl, BusinessObjectDataDdl actualBusinessObjectDataDdl)
    {
        assertNotNull(actualBusinessObjectDataDdl);
        assertEquals(request.getNamespace(), actualBusinessObjectDataDdl.getNamespace());
        assertEquals(request.getBusinessObjectDefinitionName(), actualBusinessObjectDataDdl.getBusinessObjectDefinitionName());
        assertEquals(request.getBusinessObjectFormatUsage(), actualBusinessObjectDataDdl.getBusinessObjectFormatUsage());
        assertEquals(request.getBusinessObjectFormatFileType(), actualBusinessObjectDataDdl.getBusinessObjectFormatFileType());
        assertEquals(request.getBusinessObjectFormatVersion(), actualBusinessObjectDataDdl.getBusinessObjectFormatVersion());
        assertEquals(request.getPartitionValueFilter(), actualBusinessObjectDataDdl.getPartitionValueFilter());
        assertEquals(request.getBusinessObjectDataVersion(), actualBusinessObjectDataDdl.getBusinessObjectDataVersion());
        assertEquals(request.getStorageName(), actualBusinessObjectDataDdl.getStorageName());
        assertEquals(request.getOutputFormat(), actualBusinessObjectDataDdl.getOutputFormat());
        assertEquals(request.getTableName(), actualBusinessObjectDataDdl.getTableName());
        assertEquals(expectedDdl, actualBusinessObjectDataDdl.getDdl());
    }

    /**
     * Validates business object data key against specified arguments.
     *
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedBusinessObjectDataPartitionValue the expected partition value for this business object data
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param actualBusinessObjectDataKey the business object data availability object instance to be validated
     */
    public void validateBusinessObjectDataKey(String expectedNamespace, String expectedBusinessObjectDefinitionName, String expectedBusinessObjectFormatUsage,
        String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion, String expectedBusinessObjectDataPartitionValue,
        List<String> expectedBusinessObjectDataSubPartitionValues, Integer expectedBusinessObjectDataVersion, BusinessObjectDataKey actualBusinessObjectDataKey)
    {
        assertNotNull(actualBusinessObjectDataKey);
        assertEquals(expectedNamespace, actualBusinessObjectDataKey.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualBusinessObjectDataKey.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualBusinessObjectDataKey.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualBusinessObjectDataKey.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, actualBusinessObjectDataKey.getBusinessObjectFormatVersion());
        assertEquals(expectedBusinessObjectDataPartitionValue, actualBusinessObjectDataKey.getPartitionValue());
        assertEquals(expectedBusinessObjectDataSubPartitionValues, actualBusinessObjectDataKey.getSubPartitionValues());
        assertEquals(expectedBusinessObjectDataVersion, actualBusinessObjectDataKey.getBusinessObjectDataVersion());
    }

    /**
     * Validates a business object data status change notification message.
     *
     * @param expectedMessageType the expected message type
     * @param expectedMessageDestination the expected message destination
     * @param expectedBusinessObjectDataKey the expected business object data key
     * @param expectedBusinessObjectDataId the expected business object data id
     * @param expectedUsername the expected username
     * @param expectedNewBusinessObjectDataStatus the expected new business object data status
     * @param expectedOldBusinessObjectDataStatus the expected old business object data status
     * @param expectedBusinessObjectDataAttributes the expected list of business object data attributes
     * @param expectedMessageHeaders the list of expected message headers
     * @param notificationMessage the notification message to be validated
     */
    public void validateBusinessObjectDataStatusChangeMessageWithXmlPayload(String expectedMessageType, String expectedMessageDestination,
        BusinessObjectDataKey expectedBusinessObjectDataKey, Integer expectedBusinessObjectDataId, String expectedUsername,
        String expectedNewBusinessObjectDataStatus, String expectedOldBusinessObjectDataStatus, List<Attribute> expectedBusinessObjectDataAttributes,
        List<MessageHeader> expectedMessageHeaders, NotificationMessage notificationMessage)
    {
        assertNotNull(notificationMessage);

        assertEquals(expectedMessageType, notificationMessage.getMessageType());
        assertEquals(expectedMessageDestination, notificationMessage.getMessageDestination());

        String messageText = notificationMessage.getMessageText();

        validateXmlFieldPresent(messageText, "correlation-id", "BusinessObjectData_" + expectedBusinessObjectDataId);
        validateXmlFieldPresent(messageText, "triggered-by-username", expectedUsername);
        validateXmlFieldPresent(messageText, "context-message-type", "testDomain/testApplication/BusinessObjectDataStatusChanged");
        validateXmlFieldPresent(messageText, "newBusinessObjectDataStatus", expectedNewBusinessObjectDataStatus);

        if (expectedOldBusinessObjectDataStatus == null)
        {
            validateXmlFieldNotPresent(messageText, "oldBusinessObjectDataStatus");
        }
        else
        {
            validateXmlFieldPresent(messageText, "oldBusinessObjectDataStatus", expectedOldBusinessObjectDataStatus);
        }

        validateXmlFieldPresent(messageText, "namespace", expectedBusinessObjectDataKey.getNamespace());
        validateXmlFieldPresent(messageText, "businessObjectDefinitionName", expectedBusinessObjectDataKey.getBusinessObjectDefinitionName());
        validateXmlFieldPresent(messageText, "businessObjectFormatUsage", expectedBusinessObjectDataKey.getBusinessObjectFormatUsage());
        validateXmlFieldPresent(messageText, "businessObjectFormatFileType", expectedBusinessObjectDataKey.getBusinessObjectFormatFileType());
        validateXmlFieldPresent(messageText, "businessObjectFormatVersion", expectedBusinessObjectDataKey.getBusinessObjectFormatVersion());
        validateXmlFieldPresent(messageText, "partitionValue", expectedBusinessObjectDataKey.getPartitionValue());

        if (CollectionUtils.isEmpty(expectedBusinessObjectDataKey.getSubPartitionValues()))
        {
            validateXmlFieldNotPresent(messageText, "subPartitionValues");
        }
        else
        {
            validateXmlFieldPresent(messageText, "subPartitionValues");
        }

        for (String subPartitionValue : expectedBusinessObjectDataKey.getSubPartitionValues())
        {
            validateXmlFieldPresent(messageText, "partitionValue", subPartitionValue);
        }

        if (CollectionUtils.isEmpty(expectedBusinessObjectDataAttributes))
        {
            validateXmlFieldNotPresent(messageText, "attributes");
        }
        else
        {
            validateXmlFieldPresent(messageText, "attributes");
        }

        for (Attribute attribute : expectedBusinessObjectDataAttributes)
        {
            // Validate each expected "<attribute>" XML tag. Please note that null attribute value is expected to be published as an empty string.
            validateXmlFieldPresent(messageText, "attribute", "name", attribute.getName(),
                attribute.getValue() == null ? AbstractServiceTest.EMPTY_STRING : attribute.getValue());
        }

        validateXmlFieldPresent(messageText, "businessObjectDataVersion", expectedBusinessObjectDataKey.getBusinessObjectDataVersion());

        assertEquals(expectedMessageHeaders, notificationMessage.getMessageHeaders());
    }

    /**
     * Validates the contents of a business object data status information against the specified parameters.
     *
     * @param expectedBusinessObjectDataKey the expected business object data key
     * @param expectedBusinessObjectDataStatus the expected business object data status
     * @param businessObjectDataStatusInformation the actual business object data status information
     */
    public void validateBusinessObjectDataStatusInformation(BusinessObjectDataKey expectedBusinessObjectDataKey, String expectedBusinessObjectDataStatus,
        BusinessObjectDataStatusInformation businessObjectDataStatusInformation)
    {
        assertNotNull(businessObjectDataStatusInformation);
        assertEquals(expectedBusinessObjectDataKey, businessObjectDataStatusInformation.getBusinessObjectDataKey());
        assertEquals(expectedBusinessObjectDataStatus, businessObjectDataStatusInformation.getStatus());
    }

    /**
     * Validates the contents of a business object data status update response against the specified parameters.
     *
     * @param expectedBusinessObjectDataKey the expected business object data key
     * @param expectedBusinessObjectDataStatus the expected business object data status
     * @param expectedPreviousBusinessObjectDataStatus the expected previous business object data status
     * @param actualResponse the actual business object data status update response
     */
    public void validateBusinessObjectDataStatusUpdateResponse(BusinessObjectDataKey expectedBusinessObjectDataKey, String expectedBusinessObjectDataStatus,
        String expectedPreviousBusinessObjectDataStatus, BusinessObjectDataStatusUpdateResponse actualResponse)
    {
        assertNotNull(actualResponse);
        assertEquals(expectedBusinessObjectDataKey, actualResponse.getBusinessObjectDataKey());
        assertEquals(expectedBusinessObjectDataStatus, actualResponse.getStatus());
        assertEquals(expectedPreviousBusinessObjectDataStatus, actualResponse.getPreviousStatus());
    }

    /**
     * Validates business object data storage files create response contents against specified parameters.
     *
     * @param expectedNamespace the expected namespace
     * @param expectedBusinessObjectDefinitionName the expected business object definition name
     * @param expectedBusinessObjectFormatUsage the expected business object format usage
     * @param expectedBusinessObjectFormatFileType the expected business object format file type
     * @param expectedBusinessObjectFormatVersion the expected business object format version
     * @param expectedPartitionValue the expected partition value
     * @param expectedSubPartitionValues the expected subpartition values
     * @param expectedBusinessObjectDataVersion the expected business object data version
     * @param expectedStorageName the expected storage name
     * @param expectedStorageFiles the list of expected storage files
     * @param actualResponse the business object data storage files create response to be validated
     */
    public void validateBusinessObjectDataStorageFilesCreateResponse(String expectedNamespace, String expectedBusinessObjectDefinitionName,
        String expectedBusinessObjectFormatUsage, String expectedBusinessObjectFormatFileType, Integer expectedBusinessObjectFormatVersion,
        String expectedPartitionValue, List<String> expectedSubPartitionValues, Integer expectedBusinessObjectDataVersion, String expectedStorageName,
        List<StorageFile> expectedStorageFiles, BusinessObjectDataStorageFilesCreateResponse actualResponse)
    {
        assertNotNull(actualResponse);
        assertEquals(expectedNamespace, actualResponse.getNamespace());
        assertEquals(expectedBusinessObjectDefinitionName, actualResponse.getBusinessObjectDefinitionName());
        assertEquals(expectedBusinessObjectFormatUsage, actualResponse.getBusinessObjectFormatUsage());
        assertEquals(expectedBusinessObjectFormatFileType, actualResponse.getBusinessObjectFormatFileType());
        assertEquals(expectedBusinessObjectFormatVersion, actualResponse.getBusinessObjectFormatVersion());
        assertEquals(expectedPartitionValue, actualResponse.getPartitionValue());
        assertEquals(expectedSubPartitionValues, actualResponse.getSubPartitionValues());
        assertEquals(expectedBusinessObjectDataVersion, actualResponse.getBusinessObjectDataVersion());
        assertEquals(expectedStorageName, actualResponse.getStorageName());
        assertEquals(expectedStorageFiles, actualResponse.getStorageFiles());
    }

    /**
     * Validates a list of StorageFiles against the expected values.
     *
     * @param expectedStorageFiles the list of expected StorageFiles
     * @param actualStorageFiles the list of actual StorageFiles to be validated
     */
    public void validateStorageFiles(List<StorageFile> expectedStorageFiles, List<StorageFile> actualStorageFiles)
    {
        assertEquals(expectedStorageFiles.size(), actualStorageFiles.size());
        for (int i = 0; i < expectedStorageFiles.size(); i++)
        {
            StorageFile expectedStorageFile = expectedStorageFiles.get(i);
            StorageFile actualStorageFile = actualStorageFiles.get(i);
            assertEquals(expectedStorageFile.getFilePath(), actualStorageFile.getFilePath());
            assertEquals(expectedStorageFile.getFileSizeBytes(), actualStorageFile.getFileSizeBytes());
            assertEquals(expectedStorageFile.getRowCount(), actualStorageFile.getRowCount());
        }
    }

    /**
     * Creates and returns a list of test storage files.
     *
     * @return the list of storage files
     */
    private List<StorageFile> getStorageFiles()
    {
        List<StorageFile> storageFiles = new ArrayList<>();

        StorageFile storageFile1 = new StorageFile();
        storageFiles.add(storageFile1);
        storageFile1.setFilePath("Folder/file1.gz");
        storageFile1.setFileSizeBytes(0L);
        storageFile1.setRowCount(0L);

        StorageFile storageFile2 = new StorageFile();
        storageFiles.add(storageFile2);
        storageFile2.setFilePath("Folder/file2.gz");
        storageFile2.setFileSizeBytes(2999L);
        storageFile2.setRowCount(1000L);

        StorageFile storageFile3 = new StorageFile();
        storageFiles.add(storageFile3);
        storageFile3.setFilePath("Folder/file3.gz");
        storageFile3.setFileSizeBytes(Long.MAX_VALUE);
        storageFile3.setRowCount(Long.MAX_VALUE);

        return storageFiles;
    }

    /**
     * Validates that a specified XML opening and closing set of tags are not present in the message.
     *
     * @param message the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     */
    private void validateXmlFieldNotPresent(String message, String xmlTagName)
    {
        for (String xmlTag : Arrays.asList(String.format("<%s>", xmlTagName), String.format("</%s>", xmlTagName)))
        {
            assertTrue(String.format("%s tag not expected, but found.", xmlTag), !message.contains(xmlTag));
        }
    }

    /**
     * Validates that a specified XML opening and closing set of tags are present in the message.
     *
     * @param message the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     */
    private void validateXmlFieldPresent(String message, String xmlTagName)
    {
        for (String xmlTag : Arrays.asList(String.format("<%s>", xmlTagName), String.format("</%s>", xmlTagName)))
        {
            assertTrue(String.format("%s expected, but not found.", xmlTag), message.contains(xmlTag));
        }
    }

    /**
     * Validates that a specified XML tag and value are present in the message.
     *
     * @param message the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     * @param value the value of the data for the tag.
     */
    private void validateXmlFieldPresent(String message, String xmlTagName, Object value)
    {
        assertTrue(xmlTagName + " \"" + value + "\" expected, but not found.",
            message.contains("<" + xmlTagName + ">" + (value == null ? null : value.toString()) + "</" + xmlTagName + ">"));
    }

    /**
     * Validates that the specified XML tag with the specified tag attribute and tag value is present in the message.
     *
     * @param message the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     * @param xmlTagAttributeName the tag attribute name.
     * @param xmlTagAttributeValue the tag attribute value.
     * @param xmlTagValue the value of the data for the tag.
     */
    private void validateXmlFieldPresent(String message, String xmlTagName, String xmlTagAttributeName, String xmlTagAttributeValue, Object xmlTagValue)
    {
        assertTrue(String.format("<%s> is expected, but not found or does not match expected attribute and/or value.", xmlTagName), message.contains(String
            .format("<%s %s=\"%s\">%s</%s>", xmlTagName, xmlTagAttributeName, xmlTagAttributeValue, xmlTagValue == null ? null : xmlTagValue.toString(),
                xmlTagName)));
    }
}
