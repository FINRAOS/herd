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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.dao.impl.AbstractHerdDao;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

public class StorageUnitDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetGlacierStorageUnitsToRestore()
    {
        // Create a list of business object data keys.
        List<BusinessObjectDataKey> businessObjectDataKeys = Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
            SUBPARTITION_VALUES, DATA_VERSION),
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_3,
                SUBPARTITION_VALUES, DATA_VERSION),
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_4,
                SUBPARTITION_VALUES, DATA_VERSION),
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_5,
                SUBPARTITION_VALUES, DATA_VERSION));

        // Create database entities required for testing. Only the first two Glacier storage unit entities are expected to be selected.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(storageUnitDaoTestHelper
            .createBusinessObjectDataEntityInRestoringState(businessObjectDataKeys.get(0), STORAGE_NAME_ORIGIN, StorageUnitStatusEntity.RESTORING,
                STORAGE_NAME_GLACIER, StorageUnitStatusEntity.ENABLED), storageUnitDaoTestHelper
            .createBusinessObjectDataEntityInRestoringState(businessObjectDataKeys.get(1), STORAGE_NAME_ORIGIN, StorageUnitStatusEntity.RESTORING,
                STORAGE_NAME_GLACIER, StorageUnitStatusEntity.ENABLED), storageUnitDaoTestHelper
            .createBusinessObjectDataEntityInRestoringState(businessObjectDataKeys.get(2), STORAGE_NAME_ORIGIN, StorageUnitStatusEntity.RESTORING,
                STORAGE_NAME_GLACIER, STORAGE_UNIT_STATUS), storageUnitDaoTestHelper
            .createBusinessObjectDataEntityInRestoringState(businessObjectDataKeys.get(3), STORAGE_NAME_ORIGIN, STORAGE_UNIT_STATUS, STORAGE_NAME_GLACIER,
                StorageUnitStatusEntity.ENABLED), storageUnitDaoTestHelper
            .createBusinessObjectDataEntityInRestoringState(businessObjectDataKeys.get(4), STORAGE_NAME_ORIGIN, NO_STORAGE_UNIT_STATUS, STORAGE_NAME_GLACIER,
                StorageUnitStatusEntity.ENABLED));

        // Get the list of expected Glacier storage unit entities.
        List<StorageUnitEntity> expectedGlacierStorageUnitEntities = Arrays
            .asList(storageUnitDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntities.get(0), STORAGE_NAME_GLACIER),
                storageUnitDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntities.get(1), STORAGE_NAME_GLACIER));

        // Retrieve the storage units and validate the results.
        List<StorageUnitEntity> resultStorageUnitEntities = storageUnitDao.getGlacierStorageUnitsToRestore(MAX_RESULT);
        assertEquals(expectedGlacierStorageUnitEntities.size(), resultStorageUnitEntities.size());
        assertTrue(resultStorageUnitEntities.contains(expectedGlacierStorageUnitEntities.get(0)));
        assertTrue(resultStorageUnitEntities.contains(expectedGlacierStorageUnitEntities.get(1)));
        assertTrue(resultStorageUnitEntities.get(0).getUpdatedOn().getTime() <= resultStorageUnitEntities.get(1).getUpdatedOn().getTime());

        // Try to retrieve the storage units with max result limit set to 1. Only the oldest updated storage unit entity should get selected.
        assertEquals(1, storageUnitDao.getGlacierStorageUnitsToRestore(1).size());
    }

    @Test
    public void testGetStorageUnitByStorageNameAndDirectoryPath()
    {
        // Create database entities required for testing.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, STORAGE_DIRECTORY_PATH);

        // Retrieve the relative storage file entities and validate the results.
        assertEquals(storageUnitEntity, storageUnitDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME, STORAGE_DIRECTORY_PATH));

        // Test case insensitivity for the storage name.
        assertEquals(storageUnitEntity, storageUnitDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME.toUpperCase(), STORAGE_DIRECTORY_PATH));
        assertEquals(storageUnitEntity, storageUnitDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME.toLowerCase(), STORAGE_DIRECTORY_PATH));

        // Test case sensitivity of the storage directory path.
        assertNull(storageUnitDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME, STORAGE_DIRECTORY_PATH.toUpperCase()));
        assertNull(storageUnitDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME, STORAGE_DIRECTORY_PATH.toLowerCase()));

        // Confirm negative results when using wrong input parameters.
        assertNull(storageUnitDao.getStorageUnitByStorageNameAndDirectoryPath("I_DO_NOT_EXIST", TEST_S3_KEY_PREFIX));
        assertNull(storageUnitDao.getStorageUnitByStorageNameAndDirectoryPath(STORAGE_NAME, "I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetStorageUnitByBusinessObjectDataAndStorageName()
    {
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, STORAGE_DIRECTORY_PATH);
        BusinessObjectDataEntity businessObjectDataEntity = storageUnitEntity.getBusinessObjectData();

        // test retrieval by name
        assertEquals(storageUnitEntity, storageUnitDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME));

        // test retrieval by name, case insensitive
        assertEquals(storageUnitEntity, storageUnitDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME.toUpperCase()));
        assertEquals(storageUnitEntity, storageUnitDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, STORAGE_NAME.toLowerCase()));

        // test retrieval failure
        assertNull(storageUnitDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, "I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetStorageUnitsByStorageAndBusinessObjectData()
    {
        // Create database entities required for testing.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, STORAGE_UNIT_STATUS, TEST_S3_KEY_PREFIX);

        // Retrieve storage unit entities by storage and business object data.
        List<StorageUnitEntity> resultStorageUnitEntities = storageUnitDao
            .getStorageUnitsByStorageAndBusinessObjectData(storageUnitEntity.getStorage(), Arrays.asList(storageUnitEntity.getBusinessObjectData()));

        // Validate the results.
        assertNotNull(resultStorageUnitEntities);
        assertEquals(1, resultStorageUnitEntities.size());
        assertEquals(TEST_S3_KEY_PREFIX, resultStorageUnitEntities.get(0).getDirectoryPath());
    }

    @Test
    public void testGetStorageUnitsByStoragePlatformAndBusinessObjectData()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create database entities required for testing.
        List<StorageUnitEntity> storageUnitEntities = Arrays.asList(storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME_2, STORAGE_PLATFORM_CODE, businessObjectDataEntity, STORAGE_UNIT_STATUS, STORAGE_DIRECTORY_PATH),
            storageUnitDaoTestHelper
                .createStorageUnitEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE, businessObjectDataEntity, STORAGE_UNIT_STATUS, STORAGE_DIRECTORY_PATH),
            storageUnitDaoTestHelper
                .createStorageUnitEntity(STORAGE_NAME_3, STORAGE_PLATFORM_CODE_2, businessObjectDataEntity, STORAGE_UNIT_STATUS, STORAGE_DIRECTORY_PATH));

        // Retrieve storage unit entities by storage platform and business object data.
        assertEquals(Arrays.asList(storageUnitEntities.get(1), storageUnitEntities.get(0)),
            storageUnitDao.getStorageUnitsByStoragePlatformAndBusinessObjectData(STORAGE_PLATFORM_CODE, businessObjectDataEntity));

        // Test case insensitivity of storage platform.
        assertEquals(Arrays.asList(storageUnitEntities.get(1), storageUnitEntities.get(0)),
            storageUnitDao.getStorageUnitsByStoragePlatformAndBusinessObjectData(STORAGE_PLATFORM_CODE.toUpperCase(), businessObjectDataEntity));
        assertEquals(Arrays.asList(storageUnitEntities.get(1), storageUnitEntities.get(0)),
            storageUnitDao.getStorageUnitsByStoragePlatformAndBusinessObjectData(STORAGE_PLATFORM_CODE.toLowerCase(), businessObjectDataEntity));

        // Try to retrieve storage unit entities using invalid input parameters.
        assertEquals(0, storageUnitDao.getStorageUnitsByStoragePlatformAndBusinessObjectData("I_DO_NOT_EXIST", businessObjectDataEntity).size());
    }

    @Test
    public void testGetStorageUnitsByPartitionFiltersAndStorages()
    {
        // Create database entities required for testing.
        List<StorageUnitEntity> expectedMultiStorageAvailableStorageUnits = businessObjectDataAvailabilityTestHelper
            .createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, new ArrayList<>(), new ArrayList<>(),
                BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, SUBPARTITION_VALUES, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA, STORAGE_NAMES);

        // Build a list of partition values, large enough to cause executing the select queries in chunks.
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < AbstractHerdDao.MAX_PARTITION_FILTERS_PER_REQUEST; i++)
        {
            partitionValues.add(String.format("%s-%s", PARTITION_VALUE, i));
        }
        partitionValues.addAll(UNSORTED_PARTITION_VALUES);

        // Build a list of partition filters to select the "available" business object data.
        // We add a second level partition value to partition filters here just for conditional coverage.
        List<List<String>> partitionFilters = new ArrayList<>();
        for (String partitionValue : partitionValues)
        {
            partitionFilters.add(Arrays.asList(partitionValue, SUBPARTITION_VALUES.get(0), null, null, null));
        }

        // Retrieve "available" storage units per specified parameters.
        List<StorageUnitEntity> resultStorageUnitEntities1 = storageUnitDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION, null,
            STORAGE_NAMES, null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertNotNull(resultStorageUnitEntities1);
        assertEquals(expectedMultiStorageAvailableStorageUnits, resultStorageUnitEntities1);

        // Retrieve "available" storage units without specifying
        // a business object format version, which is an optional parameter.
        List<StorageUnitEntity> resultStorageUnitEntities2 = storageUnitDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, DATA_VERSION, null, STORAGE_NAMES, null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(resultStorageUnitEntities1, resultStorageUnitEntities2);

        // Retrieve "available" storage units without specifying
        // both business object format version and business object data version.
        List<StorageUnitEntity> resultStorageUnitEntities3 = storageUnitDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, null, STORAGE_NAMES, null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(resultStorageUnitEntities1, resultStorageUnitEntities3);

        // Retrieve the "available" storage units and with VALID business object data
        // status without specifying both business object format version and business object data version.
        List<StorageUnitEntity> resultStorageUnitEntities4 = storageUnitDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, BusinessObjectDataStatusEntity.VALID, STORAGE_NAMES, null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(resultStorageUnitEntities1, resultStorageUnitEntities4);

        // Try to retrieve "available" storage units, with wrong business object data
        // status and without specifying both business object format version and business object data version.
        List<StorageUnitEntity> resultStorageUnitEntities5 = storageUnitDao
            .getStorageUnitsByPartitionFiltersAndStorages(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null),
                partitionFilters, null, BDATA_STATUS, STORAGE_NAMES, null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertTrue(resultStorageUnitEntities5.isEmpty());

        // Retrieve "available" storage units and with VALID business
        // object data status without specifying any of the storages or storage platform type.
        List<StorageUnitEntity> resultStorageUnitEntities6 = storageUnitDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION,
            BusinessObjectDataStatusEntity.VALID, NO_STORAGE_NAMES, null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(resultStorageUnitEntities1, resultStorageUnitEntities6);

        // Try to retrieve "available" storage units without
        // specifying any of the storages and providing a non-existing storage platform type.
        List<StorageUnitEntity> resultStorageUnitEntities7 = storageUnitDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION,
            BusinessObjectDataStatusEntity.VALID, NO_STORAGE_NAMES, "I_DO_NOT_EXIST", null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertTrue(resultStorageUnitEntities7.isEmpty());

        // Try to retrieve "available" storage units when excluding the storage platform type that are test storage belongs to.
        List<StorageUnitEntity> resultStorageUnitEntities8 = storageUnitDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION,
            BusinessObjectDataStatusEntity.VALID, NO_STORAGE_NAMES, null, StoragePlatformEntity.S3, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertTrue(resultStorageUnitEntities8.isEmpty());
    }

    @Test
    public void testGetStorageUnitsByPartitionFiltersAndStoragesNotEnabledStorageUnitStatus()
    {
        // Create enabled and disabled storage units for different partition values.
        StorageUnitEntity enabledStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        StorageUnitEntity disabledStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, StorageUnitStatusEntity.DISABLED, NO_STORAGE_DIRECTORY_PATH);

        // Build a list of partition filters to select business object data.
        List<List<String>> partitionFilters = new ArrayList<>();
        for (String partitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            partitionFilters.add(Arrays.asList(partitionValue, null, null, null, null));
        }

        // Retrieve "available" storage units per specified parameters.
        List<StorageUnitEntity> resultStorageUnitEntities = storageUnitDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION,
            null, Arrays.asList(STORAGE_NAME), null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(Arrays.asList(enabledStorageUnitEntity), resultStorageUnitEntities);

        // Retrieve "available" storage units without specifying
        // a business object format version, which is an optional parameter.
        resultStorageUnitEntities = storageUnitDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), partitionFilters, DATA_VERSION, null,
            Arrays.asList(STORAGE_NAME), null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(Arrays.asList(enabledStorageUnitEntity), resultStorageUnitEntities);

        // Retrieve storage units regardless of storage unit status per specified parameters.
        resultStorageUnitEntities = storageUnitDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION,
            null, Arrays.asList(STORAGE_NAME), null, null, NO_SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(Arrays.asList(enabledStorageUnitEntity, disabledStorageUnitEntity), resultStorageUnitEntities);

        // Retrieve storage units regardless of storage unit status without specifying
        // a business object format version, which is an optional parameter.
        resultStorageUnitEntities = storageUnitDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), partitionFilters, DATA_VERSION, null,
            Arrays.asList(STORAGE_NAME), null, null, NO_SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(Arrays.asList(enabledStorageUnitEntity, disabledStorageUnitEntity), resultStorageUnitEntities);
    }
}
