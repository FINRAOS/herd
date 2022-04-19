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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.util.Assert;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.api.xml.RelationalTableRegistrationDeleteResponse;
import org.finra.herd.model.api.xml.Schema;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.StorageUnitCreateRequest;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.impl.BusinessObjectDataServiceImpl;

public class RelationalTableRegistrationServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "relationalTableRegistrationServiceImpl")
    private RelationalTableRegistrationService relationalTableRegistrationServiceImpl;

    @Test
    public void testCreateRelationalTableRegistration()
    {
        // Create database entities required for relational table registration testing.
        relationalTableRegistrationServiceTestHelper
            .createDatabaseEntitiesForRelationalTableRegistrationTesting(BDEF_NAMESPACE, DATA_PROVIDER_NAME, STORAGE_NAME);

        // Pick one of the in-memory database tables to be registered as a relational table.
        String relationalSchemaName = "PUBLIC";
        String relationalTableName = BusinessObjectDefinitionEntity.TABLE_NAME.toUpperCase();

        // Create a relational table registration create request for a table that is part of the in-memory database setup as part of DAO mocks.
        RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest =
            new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                relationalSchemaName, relationalTableName, STORAGE_NAME);

        // Create a relational table registration.
        BusinessObjectData resultBusinessObjectData = relationalTableRegistrationService
            .createRelationalTableRegistration(relationalTableRegistrationCreateRequest, APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);

        // Create an expected storage unit.
        StorageUnit expectedStorageUnit = new StorageUnit();
        expectedStorageUnit
            .setStorage(new Storage(STORAGE_NAME, StoragePlatformEntity.RELATIONAL, relationalTableRegistrationServiceTestHelper.getStorageAttributes()));
        expectedStorageUnit.setStorageUnitStatus(StorageUnitStatusEntity.ENABLED);

        // Create an expected business object data.
        BusinessObjectData expectedBusinessObjectData = new BusinessObjectData();
        expectedBusinessObjectData.setId(resultBusinessObjectData.getId());
        expectedBusinessObjectData.setNamespace(BDEF_NAMESPACE);
        expectedBusinessObjectData.setBusinessObjectDefinitionName(BDEF_NAME);
        expectedBusinessObjectData.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        expectedBusinessObjectData.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        expectedBusinessObjectData.setBusinessObjectFormatVersion(INITIAL_FORMAT_VERSION);
        expectedBusinessObjectData.setPartitionValue(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE);
        expectedBusinessObjectData.setSubPartitionValues(new ArrayList<>());
        expectedBusinessObjectData.setVersion(INITIAL_DATA_VERSION);
        expectedBusinessObjectData.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);
        expectedBusinessObjectData.setLatestVersion(LATEST_VERSION_FLAG_SET);
        expectedBusinessObjectData.setStatus(BusinessObjectDataStatusEntity.VALID);
        expectedBusinessObjectData.setStorageUnits(Collections.singletonList(expectedStorageUnit));
        expectedBusinessObjectData.setAttributes(new ArrayList<>());
        expectedBusinessObjectData.setBusinessObjectDataParents(new ArrayList<>());
        expectedBusinessObjectData.setBusinessObjectDataChildren(new ArrayList<>());
        expectedBusinessObjectData.setCreatedOn(resultBusinessObjectData.getCreatedOn());
        expectedBusinessObjectData.setCreatedByUserId(HerdDaoSecurityHelper.SYSTEM_USER);

        // Validate the response.
        assertEquals(expectedBusinessObjectData, resultBusinessObjectData);

        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, INITIAL_FORMAT_VERSION);

        // Retrieve business object format that was created as part of the relational table registration.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKey);

        // Create an expected schema.
        Schema expectedSchema = new Schema();
        expectedSchema.setColumns(relationalTableRegistrationServiceTestHelper.getExpectedSchemaColumns());
        expectedSchema.setNullValue(EMPTY_STRING);

        // Build an expected business object format.
        BusinessObjectFormat expectedBusinessObjectFormat = new BusinessObjectFormat();
        expectedBusinessObjectFormat.setId(resultBusinessObjectFormat.getId());
        expectedBusinessObjectFormat.setNamespace(BDEF_NAMESPACE);
        expectedBusinessObjectFormat.setBusinessObjectDefinitionName(BDEF_NAME);
        expectedBusinessObjectFormat.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        expectedBusinessObjectFormat.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        expectedBusinessObjectFormat.setBusinessObjectFormatVersion(INITIAL_FORMAT_VERSION);
        expectedBusinessObjectFormat.setLatestVersion(LATEST_VERSION_FLAG_SET);
        expectedBusinessObjectFormat.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);
        expectedBusinessObjectFormat.setBusinessObjectFormatParents(new ArrayList<>());
        expectedBusinessObjectFormat.setBusinessObjectFormatChildren(new ArrayList<>());
        expectedBusinessObjectFormat.setBusinessObjectFormatExternalInterfaces(new ArrayList<>());
        expectedBusinessObjectFormat.setAttributes(new ArrayList<>());
        expectedBusinessObjectFormat.setAttributeDefinitions(new ArrayList<>());
        expectedBusinessObjectFormat.setRelationalSchemaName(relationalSchemaName);
        expectedBusinessObjectFormat.setRelationalTableName(relationalTableName);
        expectedBusinessObjectFormat.setSchema(expectedSchema);
        expectedBusinessObjectFormat.setAllowNonBackwardsCompatibleChanges(true);

        // Validate the newly created business object format.
        assertEquals(expectedBusinessObjectFormat, resultBusinessObjectFormat);

        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Retrieve business object definition that was created as part of the relational table registration.
        BusinessObjectDefinition resultBusinessObjectDefinition =
            businessObjectDefinitionService.getBusinessObjectDefinition(businessObjectDefinitionKey, true);

        // Create an expected business object definition.
        BusinessObjectDefinition expectedBusinessObjectDefinition = new BusinessObjectDefinition();
        expectedBusinessObjectDefinition.setId(resultBusinessObjectDefinition.getId());
        expectedBusinessObjectDefinition.setNamespace(BDEF_NAMESPACE);
        expectedBusinessObjectDefinition.setBusinessObjectDefinitionName(BDEF_NAME);
        expectedBusinessObjectDefinition.setDataProviderName(DATA_PROVIDER_NAME);
        expectedBusinessObjectDefinition.setDisplayName(BDEF_DISPLAY_NAME);
        expectedBusinessObjectDefinition.setAttributes(new ArrayList<>());
        expectedBusinessObjectDefinition.setSampleDataFiles(new ArrayList<>());
        expectedBusinessObjectDefinition.setCreatedByUserId(resultBusinessObjectDefinition.getCreatedByUserId());
        expectedBusinessObjectDefinition.setLastUpdatedByUserId(resultBusinessObjectDefinition.getLastUpdatedByUserId());
        expectedBusinessObjectDefinition.setLastUpdatedOn(resultBusinessObjectDefinition.getLastUpdatedOn());
        expectedBusinessObjectDefinition.setBusinessObjectDefinitionChangeEvents(resultBusinessObjectDefinition.getBusinessObjectDefinitionChangeEvents());

        // Validate the newly created business object definition.
        assertEquals(expectedBusinessObjectDefinition, resultBusinessObjectDefinition);
    }

    @Test
    public void testCreateRelationalTableRegistrationWithAppendToExistingBusinessObjectDefinitionSetToTrue()
    {
        // Create an existing business object definition.
        BusinessObjectDefinitionEntity existingBusinessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, DESCRIPTION, BDEF_DISPLAY_NAME, new ArrayList<>());

        // Create database entities required for relational table registration testing.
        relationalTableRegistrationServiceTestHelper.createDatabaseEntitiesForRelationalTableRegistrationTesting(STORAGE_NAME);

        // Pick one of the in-memory database tables to be registered as a relational table.
        String relationalSchemaName = "PUBLIC";
        String relationalTableName = BusinessObjectDefinitionEntity.TABLE_NAME.toUpperCase();

        // Create a relational table registration create request for a table that is part of the in-memory database setup as part of DAO mocks.
        RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest =
            new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                relationalSchemaName, relationalTableName, STORAGE_NAME);

        // Create a relational table registration.
        BusinessObjectData businessObjectData = relationalTableRegistrationService
            .createRelationalTableRegistration(relationalTableRegistrationCreateRequest, APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_TRUE);

        // Create an expected storage unit.
        StorageUnit expectedStorageUnit = new StorageUnit();
        expectedStorageUnit
            .setStorage(new Storage(STORAGE_NAME, StoragePlatformEntity.RELATIONAL, relationalTableRegistrationServiceTestHelper.getStorageAttributes()));
        expectedStorageUnit.setStorageUnitStatus(StorageUnitStatusEntity.ENABLED);

        // Create an expected business object data.
        BusinessObjectData expectedBusinessObjectData = new BusinessObjectData();
        expectedBusinessObjectData.setId(businessObjectData.getId());
        expectedBusinessObjectData.setNamespace(BDEF_NAMESPACE);
        expectedBusinessObjectData.setBusinessObjectDefinitionName(BDEF_NAME);
        expectedBusinessObjectData.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        expectedBusinessObjectData.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        expectedBusinessObjectData.setBusinessObjectFormatVersion(INITIAL_FORMAT_VERSION);
        expectedBusinessObjectData.setPartitionValue(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE);
        expectedBusinessObjectData.setSubPartitionValues(new ArrayList<>());
        expectedBusinessObjectData.setVersion(INITIAL_DATA_VERSION);
        expectedBusinessObjectData.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);
        expectedBusinessObjectData.setLatestVersion(LATEST_VERSION_FLAG_SET);
        expectedBusinessObjectData.setStatus(BusinessObjectDataStatusEntity.VALID);
        expectedBusinessObjectData.setStorageUnits(Collections.singletonList(expectedStorageUnit));
        expectedBusinessObjectData.setAttributes(new ArrayList<>());
        expectedBusinessObjectData.setBusinessObjectDataParents(new ArrayList<>());
        expectedBusinessObjectData.setBusinessObjectDataChildren(new ArrayList<>());
        expectedBusinessObjectData.setCreatedOn(businessObjectData.getCreatedOn());
        expectedBusinessObjectData.setCreatedByUserId(HerdDaoSecurityHelper.SYSTEM_USER);

        // Validate the response.
        assertEquals(expectedBusinessObjectData, businessObjectData);

        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, INITIAL_FORMAT_VERSION);

        // Retrieve business object format that was created as part of the relational table registration.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKey);

        // Create an expected schema.
        Schema expectedSchema = new Schema();
        expectedSchema.setColumns(relationalTableRegistrationServiceTestHelper.getExpectedSchemaColumns());
        expectedSchema.setNullValue(EMPTY_STRING);

        // Build an expected business object format.
        BusinessObjectFormat expectedBusinessObjectFormat = new BusinessObjectFormat();
        expectedBusinessObjectFormat.setId(businessObjectFormat.getId());
        expectedBusinessObjectFormat.setNamespace(BDEF_NAMESPACE);
        expectedBusinessObjectFormat.setBusinessObjectDefinitionName(BDEF_NAME);
        expectedBusinessObjectFormat.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        expectedBusinessObjectFormat.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        expectedBusinessObjectFormat.setBusinessObjectFormatVersion(INITIAL_FORMAT_VERSION);
        expectedBusinessObjectFormat.setLatestVersion(LATEST_VERSION_FLAG_SET);
        expectedBusinessObjectFormat.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);
        expectedBusinessObjectFormat.setBusinessObjectFormatParents(new ArrayList<>());
        expectedBusinessObjectFormat.setBusinessObjectFormatChildren(new ArrayList<>());
        expectedBusinessObjectFormat.setBusinessObjectFormatExternalInterfaces(new ArrayList<>());
        expectedBusinessObjectFormat.setAttributes(new ArrayList<>());
        expectedBusinessObjectFormat.setAttributeDefinitions(new ArrayList<>());
        expectedBusinessObjectFormat.setSchema(expectedSchema);
        expectedBusinessObjectFormat.setAllowNonBackwardsCompatibleChanges(true);
        expectedBusinessObjectFormat.setRelationalTableName(relationalTableName);
        expectedBusinessObjectFormat.setRelationalSchemaName(relationalSchemaName);

        // Validate the newly created business object format.
        assertEquals(expectedBusinessObjectFormat, businessObjectFormat);

        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME);

        // Retrieve business object definition that was created as part of the relational table registration.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);

        // Validate the newly created business object definition.
        assertEquals(existingBusinessObjectDefinitionEntity, businessObjectDefinitionEntity);
    }

    @Test
    public void testDeleteRelationalTableRegistration()
    {
        // Setup the objects needed for the test.
        BusinessObjectData resultBusinessObjectData = prepareToTestDeleteRelationalTableRegistration();

        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, null);

        // Call the method being tested.
        RelationalTableRegistrationDeleteResponse relationalTableRegistrationDeleteResponse =
            relationalTableRegistrationService.deleteRelationalTableRegistration(businessObjectFormatKey);

        // Get the business object data list from the response.
        List<BusinessObjectData> businessObjectDataList = relationalTableRegistrationDeleteResponse.getBusinessObjectDataElements();

        // Validate the result.
        assertEquals(businessObjectDataList.size(), 1);
        assertEquals(businessObjectDataList.get(0), resultBusinessObjectData);
        assertNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME)));
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey));

        try
        {
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.createBusinessObjectDataKey(resultBusinessObjectData));
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            Assert.isTrue(objectNotFoundException.toString().contains("org.finra.herd.model.ObjectNotFoundException: Business object data"),
                "Incorrect error message.");
        }
    }

    @Test
    public void testDeleteRelationalTableRegistrationNoBusinessObjectFormats()
    {
        // Setup the objects needed for the test.
        BusinessObjectData resultBusinessObjectData = prepareToTestDeleteRelationalTableRegistration();

        BusinessObjectFormatKey bogusBusinessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, null);

        try
        {
            // Call the method being tested.
            relationalTableRegistrationService.deleteRelationalTableRegistration(bogusBusinessObjectFormatKey);
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            Assert.isTrue(objectNotFoundException.toString().contains("org.finra.herd.model.ObjectNotFoundException: Business object format with namespace"),
                "Incorrect error message.");
        }
    }

    @Test
    public void testDeleteRelationalTableRegistrationTrimParameters()
    {
        // Setup the objects needed for the test.
        BusinessObjectData resultBusinessObjectData = prepareToTestDeleteRelationalTableRegistration();

        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, null);

        // Call the method being tested.
        RelationalTableRegistrationDeleteResponse relationalTableRegistrationDeleteResponse =
            relationalTableRegistrationService.deleteRelationalTableRegistration(businessObjectFormatKey);

        // Get the business object data list from the response.
        List<BusinessObjectData> businessObjectDataList = relationalTableRegistrationDeleteResponse.getBusinessObjectDataElements();

        // Validate the result.
        assertEquals(businessObjectDataList.size(), 1);
        assertEquals(businessObjectDataList.get(0), resultBusinessObjectData);
        assertNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME)));
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey));

        try
        {
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.createBusinessObjectDataKey(resultBusinessObjectData));
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            Assert.isTrue(objectNotFoundException.toString().contains("org.finra.herd.model.ObjectNotFoundException: Business object data"),
                "Incorrect error message.");
        }
    }

    @Test
    public void testDeleteRelationalTableRegistrationNoBusinessObjectData()
    {
        // Create a business object definition.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, DESCRIPTION, BDEF_DISPLAY_NAME, new ArrayList<>());

        // Create a file type entity.
        FileTypeEntity fileTypeEntity = fileTypeDaoTestHelper.createFileTypeEntity(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);

        List<BusinessObjectFormatKey> businessObjectFormatKeyList = new ArrayList<>();

        // Create and persist database entities required for testing.
        for (Integer formatVersion : Arrays.asList(INITIAL_FORMAT_VERSION, SECOND_FORMAT_VERSION))
        {
            BusinessObjectFormatEntity businessObjectFormatEntity =
                businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity,
                    formatVersion, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY, null,
                    NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH,
                    SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, NO_SCHEMA_CUSTOM_TBL_PROPERTIES,
                    SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), NO_PARTITION_COLUMNS);

            businessObjectFormatKeyList.add(businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormatEntity));
        }

        // Call the method being tested.
        RelationalTableRegistrationDeleteResponse relationalTableRegistrationDeleteResponse = relationalTableRegistrationService
            .deleteRelationalTableRegistration(
                new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, null));

        // Get the business object data list from the response.
        List<BusinessObjectData> businessObjectDataList = relationalTableRegistrationDeleteResponse.getBusinessObjectDataElements();

        // Validate the result.
        assertEquals(businessObjectDataList.size(), 0);
        assertNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME)));
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKeyList.get(0)));
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKeyList.get(1)));
    }

    @Test
    public void testDeleteRelationalTableRegistrationMultipleBusinessObjectData()
    {
        // Create a business object definition.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, DESCRIPTION, BDEF_DISPLAY_NAME, new ArrayList<>());

        // Create a file type entity.
        FileTypeEntity fileTypeEntity = fileTypeDaoTestHelper.createFileTypeEntity(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);

        List<BusinessObjectData> resultBusinessObjectDataList = new ArrayList<>();
        List<BusinessObjectFormatKey> businessObjectFormatKeyList = new ArrayList<>();

        // Create and persist database entities required for testing.
        for (Integer formatVersion : Arrays.asList(INITIAL_FORMAT_VERSION, SECOND_FORMAT_VERSION))
        {
            BusinessObjectFormatEntity businessObjectFormatEntity =
                businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity,
                    formatVersion, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, FORMAT_DOCUMENT_SCHEMA_URL, LATEST_VERSION_FLAG_SET, PARTITION_KEY, null,
                    NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE, SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH,
                    SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, NO_SCHEMA_CUSTOM_TBL_PROPERTIES,
                    SCHEMA_NULL_VALUE_BACKSLASH_N, schemaColumnDaoTestHelper.getTestSchemaColumns(), NO_PARTITION_COLUMNS);

            businessObjectFormatKeyList.add(businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormatEntity));

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

            StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity();

            List<StorageUnitCreateRequest> storageUnits = new ArrayList<>();
            businessObjectDataCreateRequest.setStorageUnits(storageUnits);

            StorageUnitCreateRequest storageUnit = new StorageUnitCreateRequest();
            storageUnits.add(storageUnit);
            storageUnit.setStorageName(storageEntity.getName());

            List<StorageFile> storageFiles = new ArrayList<>();
            storageUnit.setStorageFiles(storageFiles);

            StorageFile storageFile1 = new StorageFile();
            storageFiles.add(storageFile1);
            storageFile1.setFilePath("Folder/file1.gz");
            storageFile1.setFileSizeBytes(0L);
            storageFile1.setRowCount(0L);

            // Create business object data.
            BusinessObjectData businessObjectData = businessObjectDataDaoHelper.createBusinessObjectData(businessObjectDataCreateRequest);

            resultBusinessObjectDataList.add(businessObjectData);
        }

        // Call the method being tested.
        RelationalTableRegistrationDeleteResponse relationalTableRegistrationDeleteResponse = relationalTableRegistrationService
            .deleteRelationalTableRegistration(
                new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, null));

        // Get the business object data list from the response.
        List<BusinessObjectData> businessObjectDataList = relationalTableRegistrationDeleteResponse.getBusinessObjectDataElements();

        // Validate the result.
        assertEquals(businessObjectDataList.size(), 2);
        assertEquals(businessObjectDataList, resultBusinessObjectDataList);
        assertNull(businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME)));
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKeyList.get(0)));
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKeyList.get(1)));

        try
        {
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.createBusinessObjectDataKey(businessObjectDataList.get(0)));
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            Assert.isTrue(objectNotFoundException.toString().contains("org.finra.herd.model.ObjectNotFoundException: Business object data"),
                "Incorrect error message.");
        }

        try
        {
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.createBusinessObjectDataKey(businessObjectDataList.get(1)));
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            Assert.isTrue(objectNotFoundException.toString().contains("org.finra.herd.model.ObjectNotFoundException: Business object data"),
                "Incorrect error message.");
        }
    }


    @Test
    public void testGetRelationalTableRegistrationsForSchemaUpdate()
    {
        // Create a storage unit entity for a relational table registration.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.RELATIONAL,
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, INITIAL_FORMAT_VERSION,
                BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION),
            AbstractDaoTest.LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
            AbstractDaoTest.NO_STORAGE_DIRECTORY_PATH);

        // Get a list of relational table registrations for the schema update..
        List<BusinessObjectDataStorageUnitKey> result = relationalTableRegistrationService.getRelationalTableRegistrationsForSchemaUpdate();

        // Validate the results.
        assertEquals(Collections.singletonList(storageUnitHelper.createStorageUnitKeyFromEntity(storageUnitEntity)), result);
    }

    @Test
    public void testProcessRelationalTableRegistrationForSchemaUpdate()
    {
        // Pick one of the in-memory database tables to be used as a relational table.
        String relationalSchemaName = "PUBLIC";
        String relationalTableName = BusinessObjectDefinitionEntity.TABLE_NAME.toUpperCase();

        // Create a RELATIONAL storage with attributes required for relational table registration testing.
        storageDaoTestHelper
            .createStorageEntity(STORAGE_NAME, StoragePlatformEntity.RELATIONAL, relationalTableRegistrationServiceTestHelper.getStorageAttributes());

        // Create a business object definition.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, DESCRIPTION, BDEF_DISPLAY_NAME, new ArrayList<>());

        // Create RELATIONAL_TABLE file type entity.
        fileTypeDaoTestHelper.createFileTypeEntity(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, AbstractServiceTest.FORMAT_FILE_TYPE_DESCRIPTION);

        // Create a schema that matches the relational table schema.
        Schema expectedSchema = new Schema();
        expectedSchema.setColumns(relationalTableRegistrationServiceTestHelper.getExpectedSchemaColumns());
        expectedSchema.setNullValue(EMPTY_STRING);

        // Create a business object format with the schema that has less columns than the relational table.
        BusinessObjectFormat initialBusinessObjectFormat = businessObjectFormatService.createBusinessObjectFormat(
            new BusinessObjectFormatCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE,
                BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY, NO_FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL,
                null, NO_ATTRIBUTE_DEFINITIONS,
                new Schema(expectedSchema.getColumns().subList(0, expectedSchema.getColumns().size() - 1), NO_PARTITION_COLUMNS, EMPTY_STRING, null, null, null,
                    null, null, null, null, NO_PARTITION_KEY_GROUP), relationalSchemaName, relationalTableName));

        // Create a business object data key for the initial version of the relation table registration.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, INITIAL_FORMAT_VERSION,
                BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION);

        // Create a storage unit entity for the initial version of the relational table registration.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.RELATIONAL, businessObjectDataKey, AbstractDaoTest.LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED, AbstractDaoTest.NO_STORAGE_DIRECTORY_PATH);

        // Get the storage unit key for the initial version of the relational table registration.
        BusinessObjectDataStorageUnitKey storageUnitKey = storageUnitHelper.createStorageUnitKeyFromEntity(storageUnitEntity);

        // Process relational table registration for schema update, when schema is expected not to be updated.
        BusinessObjectData resultBusinessObjectData = relationalTableRegistrationService.processRelationalTableRegistrationForSchemaUpdate(storageUnitKey);

        // Create an expected storage unit.
        StorageUnit expectedStorageUnit = new StorageUnit();
        expectedStorageUnit
            .setStorage(new Storage(STORAGE_NAME, StoragePlatformEntity.RELATIONAL, relationalTableRegistrationServiceTestHelper.getStorageAttributes()));
        expectedStorageUnit.setStorageUnitStatus(StorageUnitStatusEntity.ENABLED);

        // Create an expected business object data.
        BusinessObjectData expectedBusinessObjectData = new BusinessObjectData();
        expectedBusinessObjectData.setId(resultBusinessObjectData.getId());
        expectedBusinessObjectData.setNamespace(BDEF_NAMESPACE);
        expectedBusinessObjectData.setBusinessObjectDefinitionName(BDEF_NAME);
        expectedBusinessObjectData.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        expectedBusinessObjectData.setBusinessObjectFormatFileType(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE);
        expectedBusinessObjectData.setBusinessObjectFormatVersion(SECOND_FORMAT_VERSION);
        expectedBusinessObjectData.setPartitionValue(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE);
        expectedBusinessObjectData.setSubPartitionValues(new ArrayList<>());
        expectedBusinessObjectData.setVersion(INITIAL_DATA_VERSION);
        expectedBusinessObjectData.setPartitionKey(BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY);
        expectedBusinessObjectData.setLatestVersion(LATEST_VERSION_FLAG_SET);
        expectedBusinessObjectData.setStatus(BusinessObjectDataStatusEntity.VALID);
        expectedBusinessObjectData.setStorageUnits(Collections.singletonList(expectedStorageUnit));
        expectedBusinessObjectData.setAttributes(new ArrayList<>());
        expectedBusinessObjectData.setBusinessObjectDataParents(new ArrayList<>());
        expectedBusinessObjectData.setBusinessObjectDataChildren(new ArrayList<>());
        expectedBusinessObjectData.setCreatedOn(resultBusinessObjectData.getCreatedOn());
        expectedBusinessObjectData.setCreatedByUserId(HerdDaoSecurityHelper.SYSTEM_USER);

        // Validate the response.
        assertEquals(expectedBusinessObjectData, resultBusinessObjectData);

        // Create a business object format key for the business object format version with the updated schema.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, SECOND_FORMAT_VERSION);

        // Retrieve business object format that was created as part of the relational table schema update.
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKey);

        // Build an expected business object format.
        BusinessObjectFormat expectedBusinessObjectFormat = (BusinessObjectFormat) initialBusinessObjectFormat.clone();
        expectedBusinessObjectFormat.setId(resultBusinessObjectFormat.getId());
        expectedBusinessObjectFormat.setBusinessObjectFormatVersion(SECOND_FORMAT_VERSION);
        expectedBusinessObjectFormat.setSchema(expectedSchema);
        expectedBusinessObjectFormat.setRelationalSchemaName(relationalSchemaName);
        expectedBusinessObjectFormat.setRelationalTableName(relationalTableName);

        // Validate the newly created business object format version.
        assertEquals(expectedBusinessObjectFormat, resultBusinessObjectFormat);
    }

    @Test
    public void testProcessRelationalTableRegistrationForSchemaUpdateNoSchemaChanges()
    {
        // Pick one of the in-memory database tables to be used as a relational table.
        String relationalSchemaName = "PUBLIC";
        String relationalTableName = BusinessObjectDefinitionEntity.TABLE_NAME.toUpperCase();

        // Create a RELATIONAL storage with attributes required for relational table registration testing.
        storageDaoTestHelper
            .createStorageEntity(STORAGE_NAME, StoragePlatformEntity.RELATIONAL, relationalTableRegistrationServiceTestHelper.getStorageAttributes());

        // Create a business object definition.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, DESCRIPTION, BDEF_DISPLAY_NAME, new ArrayList<>());

        // Create RELATIONAL_TABLE file type entity.
        fileTypeDaoTestHelper.createFileTypeEntity(FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, AbstractServiceTest.FORMAT_FILE_TYPE_DESCRIPTION);

        // Create a business object format with the schema that matches the relational table schema.
        businessObjectFormatService.createBusinessObjectFormat(
            new BusinessObjectFormatCreateRequest(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE,
                BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_KEY, DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, null,
                NO_ATTRIBUTE_DEFINITIONS,
                new Schema(relationalTableRegistrationServiceTestHelper.getExpectedSchemaColumns(), NO_PARTITION_COLUMNS, EMPTY_STRING, null, null, null, null,
                    null, null, null, NO_PARTITION_KEY_GROUP), relationalSchemaName, relationalTableName));

        // Create a business object data key for the initial version of the relation table registration.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, INITIAL_FORMAT_VERSION,
                BusinessObjectDataServiceImpl.NO_PARTITIONING_PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION);

        // Create a storage unit entity for the initial version of the relational table registration.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.RELATIONAL, businessObjectDataKey, AbstractDaoTest.LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED, AbstractDaoTest.NO_STORAGE_DIRECTORY_PATH);

        // Get the storage unit key for the initial version of the relational table registration.
        BusinessObjectDataStorageUnitKey storageUnitKey = storageUnitHelper.createStorageUnitKeyFromEntity(storageUnitEntity);

        // Process relational table registration for schema update, when schema is expected not to be updated.
        BusinessObjectData resultBusinessObjectData = relationalTableRegistrationService.processRelationalTableRegistrationForSchemaUpdate(storageUnitKey);

        // Validate the results. No relational table schema update is expected to occur.
        assertNull(resultBusinessObjectData);
    }

    /**
     * This unit test is to get coverage for the methods that have an explicit annotation for transaction propagation.
     */
    @Test
    public void testRelationalTableRegistrationServiceMethodsNewTransactionPropagation()
    {
        try
        {
            relationalTableRegistrationServiceImpl
                .createRelationalTableRegistration(new RelationalTableRegistrationCreateRequest(), APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        relationalTableRegistrationServiceImpl.getRelationalTableRegistrationsForSchemaUpdate();

        try
        {
            relationalTableRegistrationServiceImpl.processRelationalTableRegistrationForSchemaUpdate(
                new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data storage unit {%s, storageName: \"%s\"} doesn't exist.", businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataKeyAsString(
                    new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        SUBPARTITION_VALUES, DATA_VERSION)), STORAGE_NAME), e.getMessage());
        }
    }


    private BusinessObjectData prepareToTestDeleteRelationalTableRegistration()
    {
        // Create database entities required for relational table registration testing.
        relationalTableRegistrationServiceTestHelper
            .createDatabaseEntitiesForRelationalTableRegistrationTesting(BDEF_NAMESPACE, DATA_PROVIDER_NAME, STORAGE_NAME);

        // Pick one of the in-memory database tables to be registered as a relational table.
        String relationalSchemaName = "PUBLIC";
        String relationalTableName = BusinessObjectDefinitionEntity.TABLE_NAME.toUpperCase();

        // Create a relational table registration create request for a table that is part of the in-memory database setup as part of DAO mocks.
        RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest =
            new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                relationalSchemaName, relationalTableName, STORAGE_NAME);

        // Create a relational table registration.
        return relationalTableRegistrationService
            .createRelationalTableRegistration(relationalTableRegistrationCreateRequest, APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
    }
}
