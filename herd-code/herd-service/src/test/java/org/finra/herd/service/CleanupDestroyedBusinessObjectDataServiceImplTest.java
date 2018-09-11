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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusHistoryEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * This class tests functionality within the cleanup destroyed business object data service implementation.
 */
public class CleanupDestroyedBusinessObjectDataServiceImplTest extends AbstractServiceTest
{
    @Test
    public void testCleanupS3StorageUnit()
    {
        // Create some data for testing the cleanup
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey = createDatabaseEntitiesForCleanupDestroyedBusinessObjectDataTesting();

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.createBusinessObjectDataKeyFromStorageUnitKey(businessObjectDataStorageUnitKey);

        // Confirm the business object data entity exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey);

        assertThat("The business object data entity is null.", businessObjectDataEntity, is(notNullValue()));

        // Confirm the business object data attribute exists.
        BusinessObjectDataAttributeKey businessObjectDataAttributeKey =
            businessObjectDataAttributeHelper.getBusinessObjectDataAttributeKey(businessObjectDataKey, ATTRIBUTE_NAME);

        assertThat("The business object data attribute key is null.", businessObjectDataAttributeKey, is(notNullValue()));

        assertThat("The business object data entity attributes size is not correct.", businessObjectDataEntity.getAttributes().size(), is(equalTo(1)));

        // Confirm the business object data attribute exists.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(businessObjectDataAttributeKey);

        assertThat("The business object data attribute entity is null.", businessObjectDataAttributeEntity, is(notNullValue()));

        // Confirm the storage unity exists.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntityByKey(businessObjectDataStorageUnitKey);

        assertThat("The storage unit entity is null.", storageUnitEntity, is(notNullValue()));

        // Confirm the storage file exists.
        StorageFileEntity storageFileEntity = storageFileDao.getStorageFileByStorageNameAndFilePath(STORAGE_NAME, STORAGE_DIRECTORY_PATH);

        assertThat("The storage file entity is null.", storageFileEntity, is(notNullValue()));

        // Confirm the business object data status history exists.
        Collection<BusinessObjectDataStatusHistoryEntity> businessObjectDataStatusHistoryEntities = businessObjectDataEntity.getHistoricalStatuses();

        assertThat("The business object data status history entities size is not correct.", businessObjectDataStatusHistoryEntities.size(), is(equalTo(1)));

        // Confirm the business object data children
        assertThat("The business object data children size is not correct.", businessObjectDataEntity.getBusinessObjectDataChildren().size(), is(equalTo(1)));

        BusinessObjectDataEntity businessObjectDataEntityChild = businessObjectDataEntity.getBusinessObjectDataChildren().get(0);

        assertThat("The business object data children is not correct.", businessObjectDataEntityChild.getBusinessObjectDataParents().get(0),
            is(businessObjectDataEntity));

        // Confirm the business object data parents
        assertThat("The business object data parents size is not correct.", businessObjectDataEntity.getBusinessObjectDataParents().size(), is(equalTo(1)));

        assertThat(businessObjectDataEntityChild.getBusinessObjectDataParents().get(0), is(businessObjectDataEntity));

        BusinessObjectDataEntity businessObjectDataEntityParent = businessObjectDataEntity.getBusinessObjectDataParents().get(0);

        assertThat("The business object data parent is not correct.", businessObjectDataEntityParent.getBusinessObjectDataChildren().get(0),
            is(businessObjectDataEntity));

        // All traces of BData are removed: BData record, Attributes, related Storage Units and Storage Files, Status history, parent-child relationships
        cleanupDestroyedBusinessObjectDataService.cleanupS3StorageUnit(businessObjectDataStorageUnitKey);

        // Verify the business object data has been removed.
        businessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey);

        assertThat("The business object data entity is not null.", businessObjectDataEntity, is(nullValue()));

        // Verify the business object data attribute has been removed.
        businessObjectDataAttributeEntity = businessObjectDataAttributeDao.getBusinessObjectDataAttributeByKey(businessObjectDataAttributeKey);

        assertThat("The business object data attribute entity is not null.", businessObjectDataAttributeEntity, is(nullValue()));

        // Verify the storage unit has been removed.
        storageUnitEntity = storageUnitDao.getStorageUnitByKey(businessObjectDataStorageUnitKey);

        assertThat("The storage unit entity is not null.", storageUnitEntity, is(nullValue()));

        // Verify the storage file has been removed.
        storageFileEntity = storageFileDao.getStorageFileByStorageNameAndFilePath(STORAGE_NAME, STORAGE_DIRECTORY_PATH);

        assertThat("The storage file entity is not null.", storageFileEntity, is(nullValue()));

        // Verify the business object data children parent relationship has been removed
        businessObjectDataEntityChild = businessObjectDataDaoHelper
            .getBusinessObjectDataEntity(businessObjectDataHelper.createBusinessObjectDataKeyFromEntity(businessObjectDataEntityChild));
        assertThat("The business object data children size is not correct.", businessObjectDataEntityChild.getBusinessObjectDataParents().size(),
            is(equalTo(0)));

        // // Verify the business object data parent child relationship has been removed
        businessObjectDataEntityParent = businessObjectDataDaoHelper
            .getBusinessObjectDataEntity(businessObjectDataHelper.createBusinessObjectDataKeyFromEntity(businessObjectDataEntityParent));
        assertThat("The business object data parents size is not correct.", businessObjectDataEntityParent.getBusinessObjectDataChildren().size(),
            is(equalTo(0)));
    }

    @Test
    public void testGetS3StorageUnitsToCleanup()
    {
        // Create a list of business object data keys.
        List<BusinessObjectDataKey> businessObjectDataKeys = new ArrayList<>();
        for (int i = 0; i < 7; i++)
        {
            businessObjectDataKeys.add(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Integer.toString(i),
                    SUBPARTITION_VALUES, DATA_VERSION));
        }

        // Create database entities required for testing.
        List<StorageUnitEntity> storageUnitEntities = Lists.newArrayList();

        // Add 4 valid storage unit entries
        storageUnitEntities.add(storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.S3, businessObjectDataKeys.get(0), LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.DELETED, StorageUnitStatusEntity.DISABLED, NO_STORAGE_DIRECTORY_PATH));

        storageUnitEntities.add(storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.S3, businessObjectDataKeys.get(1), LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.DELETED, StorageUnitStatusEntity.DISABLED, NO_STORAGE_DIRECTORY_PATH));

        storageUnitEntities.add(storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.S3, businessObjectDataKeys.get(2), LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.DELETED, StorageUnitStatusEntity.DISABLED, NO_STORAGE_DIRECTORY_PATH));

        storageUnitEntities.add(storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME_2, StoragePlatformEntity.S3, businessObjectDataKeys.get(3), LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.DELETED, StorageUnitStatusEntity.DISABLED, NO_STORAGE_DIRECTORY_PATH));

        // Not a valid business object data status
        storageUnitEntities.add(storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.S3, businessObjectDataKeys.get(4), LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.ARCHIVED, StorageUnitStatusEntity.DISABLED, NO_STORAGE_DIRECTORY_PATH));

        // Not a valid storage unit status
        storageUnitEntities.add(storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.S3, businessObjectDataKeys.get(5), LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.DELETED, StorageUnitStatusEntity.ARCHIVED, NO_STORAGE_DIRECTORY_PATH));

        // Not a valid storage platform
        storageUnitEntities.add(storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME_3, StoragePlatformEntity.TABLE_NAME, businessObjectDataKeys.get(6), LATEST_VERSION_FLAG_SET,
                BusinessObjectDataStatusEntity.DELETED, StorageUnitStatusEntity.DISABLED, NO_STORAGE_DIRECTORY_PATH));

        // Set restore expiration time values.
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());

        // Not a valid final destroy on date
        storageUnitEntities.get(0).setFinalDestroyOn(null);

        // Final destroy on date is not less than current timestamp
        storageUnitEntities.get(1).setFinalDestroyOn(HerdDateUtils.addDays(currentTime, 1));

        // Valid final destroy on dates
        storageUnitEntities.get(2).setFinalDestroyOn(HerdDateUtils.addDays(currentTime, -1));
        storageUnitEntities.get(3).setFinalDestroyOn(HerdDateUtils.addDays(currentTime, -2));
        storageUnitEntities.get(4).setFinalDestroyOn(HerdDateUtils.addDays(currentTime, -2));
        storageUnitEntities.get(5).setFinalDestroyOn(HerdDateUtils.addDays(currentTime, -2));
        storageUnitEntities.get(6).setFinalDestroyOn(HerdDateUtils.addDays(currentTime, -2));

        // Retrieve the business object data storage unit keys and validate the results. Only two entities are expected to match all select criteria.
        List<BusinessObjectDataStorageUnitKey> result = cleanupDestroyedBusinessObjectDataService.getS3StorageUnitsToCleanup(MAX_RESULT);
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey3 = storageUnitHelper.createStorageUnitKeyFromEntity(storageUnitEntities.get(3));
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey2 = storageUnitHelper.createStorageUnitKeyFromEntity(storageUnitEntities.get(2));
        assertEquals(Arrays.asList(businessObjectDataStorageUnitKey3, businessObjectDataStorageUnitKey2), result);

        // Try to retrieve the business object data storage unit keys with max result limit set to 1. Only a single storage unit entity should get selected.
        result = cleanupDestroyedBusinessObjectDataService.getS3StorageUnitsToCleanup(1);
        assertEquals(Collections.singletonList(businessObjectDataStorageUnitKey3), result);
    }

    /**
     * Creates business object data and associated entities required for the cleanup destroyed business object data unit tests.
     */
    public BusinessObjectDataStorageUnitKey createDatabaseEntitiesForCleanupDestroyedBusinessObjectDataTesting()
    {
        List<SchemaColumn> columns = schemaColumnDaoTestHelper.getTestSchemaColumns();
        List<SchemaColumn> partitionColumns = schemaColumnDaoTestHelper.getTestPartitionColumns();

        // Create a business object format entity.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, LATEST_VERSION_FLAG_SET, PARTITION_KEY, PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_COMMA,
                SCHEMA_ESCAPE_CHARACTER_BACKSLASH, SCHEMA_NULL_VALUE_BACKSLASH_N, columns, partitionColumns);

        // Create S3 storage entity.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, Arrays
            .asList(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME),
                new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE),
                    S3_KEY_PREFIX_VELOCITY_TEMPLATE)));

        // Create a business object data entity parent.
        BusinessObjectDataEntity businessObjectDataEntityParent = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME_3, FORMAT_USAGE_CODE_3, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_3,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.DELETED,
                Lists.newArrayList(businessObjectDataEntityParent));

        // Create a business object data entity child.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, PARTITION_VALUE_2,
                SUBPARTITION_VALUES_2, DATA_VERSION_2, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                Lists.newArrayList(businessObjectDataEntity));

        // Create and persist a business object data attribute entity.
        businessObjectDataAttributeDaoTestHelper.createBusinessObjectDataAttributeEntity(businessObjectDataEntity, ATTRIBUTE_NAME, ATTRIBUTE_VALUE);

        // Create the storage units.
        StorageUnitEntity storageUnitEntity =
            storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.DISABLED, STORAGE_DIRECTORY_PATH);

        storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, STORAGE_DIRECTORY_PATH, FILE_SIZE, ROW_COUNT);

        return storageUnitHelper.createStorageUnitKeyFromEntity(storageUnitEntity);
    }
}
