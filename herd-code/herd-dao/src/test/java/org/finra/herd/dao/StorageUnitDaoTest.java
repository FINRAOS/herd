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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.impl.AbstractHerdDao;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

public class StorageUnitDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetLatestVersionStorageUnitsByStoragePlatformAndFileType()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit entity that belong to the latest relative versions of business object format and business object data.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE, businessObjectDataKey, AbstractDaoTest.LATEST_VERSION_FLAG_SET,
                AbstractDaoTest.BDATA_STATUS, STORAGE_UNIT_STATUS, AbstractDaoTest.NO_STORAGE_DIRECTORY_PATH);

        // Retrieve latest version storage units by storage platform and business object format file type.
        assertEquals(Collections.singletonList(storageUnitEntity),
            storageUnitDao.getLatestVersionStorageUnitsByStoragePlatformAndFileType(STORAGE_PLATFORM_CODE, FORMAT_FILE_TYPE_CODE));

        // Test case insensitivity of the input parameters.
        assertEquals(Collections.singletonList(storageUnitEntity),
            storageUnitDao.getLatestVersionStorageUnitsByStoragePlatformAndFileType(STORAGE_PLATFORM_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase()));
        assertEquals(Collections.singletonList(storageUnitEntity),
            storageUnitDao.getLatestVersionStorageUnitsByStoragePlatformAndFileType(STORAGE_PLATFORM_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase()));

        // Try to retrieve storage units using invalid input parameters.
        assertEquals(0, storageUnitDao.getLatestVersionStorageUnitsByStoragePlatformAndFileType(I_DO_NOT_EXIST, FORMAT_FILE_TYPE_CODE).size());
        assertEquals(0, storageUnitDao.getLatestVersionStorageUnitsByStoragePlatformAndFileType(STORAGE_PLATFORM_CODE, I_DO_NOT_EXIST).size());

        // Update the business object format entity not to have its latest version flag set.
        storageUnitEntity.getBusinessObjectData().getBusinessObjectFormat().setLatestVersion(false);

        // Validate that no storage units get selected now.
        assertEquals(0, storageUnitDao.getLatestVersionStorageUnitsByStoragePlatformAndFileType(STORAGE_PLATFORM_CODE, FORMAT_FILE_TYPE_CODE).size());

        // Restore the business object format latest version flag.
        storageUnitEntity.getBusinessObjectData().getBusinessObjectFormat().setLatestVersion(true);

        // Retrieve latest version storage units by storage platform and business object format file type.
        assertEquals(Collections.singletonList(storageUnitEntity),
            storageUnitDao.getLatestVersionStorageUnitsByStoragePlatformAndFileType(STORAGE_PLATFORM_CODE, FORMAT_FILE_TYPE_CODE));

        // Update the business object data entity not to have its latest version flag set.
        storageUnitEntity.getBusinessObjectData().setLatestVersion(false);

        // Validate that no storage units get selected now.
        assertEquals(0, storageUnitDao.getLatestVersionStorageUnitsByStoragePlatformAndFileType(STORAGE_PLATFORM_CODE, FORMAT_FILE_TYPE_CODE).size());

        // Restore the business object data latest version flag.
        storageUnitEntity.getBusinessObjectData().setLatestVersion(true);

        // Retrieve latest version storage units by storage platform and business object format file type.
        assertEquals(Collections.singletonList(storageUnitEntity),
            storageUnitDao.getLatestVersionStorageUnitsByStoragePlatformAndFileType(STORAGE_PLATFORM_CODE, FORMAT_FILE_TYPE_CODE));
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

        // Retrieve the storage units and validate the results. Only two entities are expected to match all select criteria.
        List<StorageUnitEntity> result = storageUnitDao.getS3StorageUnitsToCleanup(MAX_RESULT);
        assertEquals(Arrays.asList(storageUnitEntities.get(3), storageUnitEntities.get(2)), result);

        // Try to retrieve the storage units with max result limit set to 1. Only a single storage unit entity should get selected.
        result = storageUnitDao.getS3StorageUnitsToCleanup(1);
        assertEquals(Collections.singletonList(storageUnitEntities.get(3)), result);
    }

    @Test
    public void testGetS3StorageUnitsToExpire()
    {
        // Create a list of business object data keys.
        List<BusinessObjectDataKey> businessObjectDataKeys = new ArrayList<>();
        for (int i = 0; i < 6; i++)
        {
            businessObjectDataKeys.add(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Integer.toString(i),
                    SUBPARTITION_VALUES, DATA_VERSION));
        }

        // Create database entities required for testing.
        List<StorageUnitEntity> storageUnitEntities = Arrays
            .asList(storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, businessObjectDataKeys.get(0), StorageUnitStatusEntity.RESTORED),
                storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, businessObjectDataKeys.get(1), StorageUnitStatusEntity.RESTORED),
                storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, businessObjectDataKeys.get(2), StorageUnitStatusEntity.RESTORED),
                storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, businessObjectDataKeys.get(3), StorageUnitStatusEntity.RESTORED),
                storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, businessObjectDataKeys.get(4), STORAGE_UNIT_STATUS), storageUnitDaoTestHelper
                    .createStorageUnitEntity(STORAGE_NAME_2, STORAGE_PLATFORM_CODE, businessObjectDataKeys.get(5), LATEST_VERSION_FLAG_SET, BDATA_STATUS,
                        StorageUnitStatusEntity.RESTORED, NO_STORAGE_DIRECTORY_PATH));

        // Set restore expiration time values.
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        storageUnitEntities.get(0).setRestoreExpirationOn(null);
        storageUnitEntities.get(1).setRestoreExpirationOn(HerdDateUtils.addDays(currentTime, 1));
        storageUnitEntities.get(2).setRestoreExpirationOn(HerdDateUtils.addDays(currentTime, -1));
        storageUnitEntities.get(3).setRestoreExpirationOn(HerdDateUtils.addDays(currentTime, -2));
        storageUnitEntities.get(4).setRestoreExpirationOn(HerdDateUtils.addDays(currentTime, -2));
        storageUnitEntities.get(5).setRestoreExpirationOn(HerdDateUtils.addDays(currentTime, -2));

        // Retrieve the storage units and validate the results. Only two entities are expected to match all select criteria.
        List<StorageUnitEntity> result = storageUnitDao.getS3StorageUnitsToExpire(MAX_RESULT);
        assertEquals(Arrays.asList(storageUnitEntities.get(3), storageUnitEntities.get(2)), result);

        // Try to retrieve the storage units with max result limit set to 1. Only a single storage unit entity should get selected.
        assertEquals(Collections.singletonList(storageUnitEntities.get(3)), storageUnitDao.getS3StorageUnitsToExpire(1));
    }

    @Test
    public void testGetS3StorageUnitsToRestore()
    {
        // Create a list of business object data keys.
        List<BusinessObjectDataKey> businessObjectDataKeys = new ArrayList<>();
        for (int i = 0; i < 4; i++)
        {
            businessObjectDataKeys.add(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Integer.toString(i),
                    SUBPARTITION_VALUES, DATA_VERSION));
        }

        // Create database entities required for testing.
        List<StorageUnitEntity> storageUnitEntities = Arrays
            .asList(storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, businessObjectDataKeys.get(0), StorageUnitStatusEntity.RESTORING),
                storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, businessObjectDataKeys.get(1), StorageUnitStatusEntity.RESTORING),
                storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, businessObjectDataKeys.get(2), STORAGE_UNIT_STATUS), storageUnitDaoTestHelper
                    .createStorageUnitEntity(STORAGE_NAME_2, STORAGE_PLATFORM_CODE, businessObjectDataKeys.get(3), LATEST_VERSION_FLAG_SET, BDATA_STATUS,
                        StorageUnitStatusEntity.RESTORING, NO_STORAGE_DIRECTORY_PATH));

        // Retrieve the storage units and validate the results. Only the first two storage unit entities are expected to be selected.
        List<StorageUnitEntity> result = storageUnitDao.getS3StorageUnitsToRestore(MAX_RESULT);
        assertEquals(2, result.size());
        assertTrue(result.contains(storageUnitEntities.get(0)));
        assertTrue(result.contains(storageUnitEntities.get(1)));
        assertTrue(result.get(0).getUpdatedOn().getTime() <= result.get(1).getUpdatedOn().getTime());

        // Try to retrieve the storage units with max result limit set to 1. Only the oldest updated storage unit entity should get selected.
        assertEquals(1, storageUnitDao.getS3StorageUnitsToRestore(1).size());
    }

    @Test
    public void testGetStorageUnitByBusinessObjectDataAndStorage()
    {
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, STORAGE_DIRECTORY_PATH);
        BusinessObjectDataEntity businessObjectDataEntity = storageUnitEntity.getBusinessObjectData();
        StorageEntity storageEntity = storageUnitEntity.getStorage();

        // Test retrieval by entities.
        assertEquals(storageUnitEntity, storageUnitDao.getStorageUnitByBusinessObjectDataAndStorage(businessObjectDataEntity, storageEntity));

        // Test retrieval failures.
        assertNull(
            storageUnitDao.getStorageUnitByBusinessObjectDataAndStorage(businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(), storageEntity));
        assertNull(storageUnitDao.getStorageUnitByBusinessObjectDataAndStorage(businessObjectDataEntity, storageDaoTestHelper.createStorageEntity()));
    }

    @Test
    public void testGetStorageUnitByBusinessObjectDataAndStorageName()
    {
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, STORAGE_DIRECTORY_PATH);
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
    public void testGetStorageUnitByKey()
    {
        // Create and persist the relative database entities.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, STORAGE_DIRECTORY_PATH);

        // Get a storage unit.
        assertEquals(storageUnitEntity, storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME)));

        // Test case insensitivity.
        assertEquals(storageUnitEntity, storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME.toUpperCase())));
        assertEquals(storageUnitEntity, storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME.toLowerCase())));

        // Try to retrieve storage unit using invalid input parameters.
        assertNull(storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(I_DO_NOT_EXIST, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME)));
        assertNull(storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, I_DO_NOT_EXIST, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME)));
        assertNull(storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, I_DO_NOT_EXIST, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME)));
        assertNull(storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME)));
        assertNull(storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME)));
        assertNull(storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, I_DO_NOT_EXIST,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME)));
        assertNull(storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES_2, DATA_VERSION, STORAGE_NAME)));
        assertNull(storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION_2, STORAGE_NAME)));
        assertNull(storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, I_DO_NOT_EXIST)));
    }

    @Test
    public void testGetStorageUnitByKeyNoSubPartitionValues()
    {
        // Create and persist the relative database entities.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, STORAGE_DIRECTORY_PATH);

        // Get a storage unit.
        assertEquals(storageUnitEntity, storageUnitDao.getStorageUnitByKey(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME)));
    }

    @Test
    public void testGetStorageUnitByStorageAndDirectoryPath()
    {
        // Create two storage entities.
        List<StorageEntity> storageEntities =
            Arrays.asList(storageDaoTestHelper.createStorageEntity(STORAGE_NAME), storageDaoTestHelper.createStorageEntity(STORAGE_NAME_2));

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, STORAGE_DIRECTORY_PATH);

        // Retrieve the relative storage unit entity.
        assertEquals(storageUnitEntity, storageUnitDao.getStorageUnitByStorageAndDirectoryPath(storageEntities.get(0), STORAGE_DIRECTORY_PATH));

        // Test case sensitivity of the storage directory path.
        assertNull(storageUnitDao.getStorageUnitByStorageAndDirectoryPath(storageEntities.get(0), STORAGE_DIRECTORY_PATH.toUpperCase()));
        assertNull(storageUnitDao.getStorageUnitByStorageAndDirectoryPath(storageEntities.get(0), STORAGE_DIRECTORY_PATH.toLowerCase()));

        // Confirm that no storage unit get selected when using wrong input parameters.
        assertNull(storageUnitDao.getStorageUnitByStorageAndDirectoryPath(storageEntities.get(1), STORAGE_DIRECTORY_PATH));
        assertNull(storageUnitDao.getStorageUnitByStorageAndDirectoryPath(storageEntities.get(0), I_DO_NOT_EXIST));
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
                NO_SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        StorageUnitEntity disabledStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                NO_SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, StorageUnitStatusEntity.DISABLED, NO_STORAGE_DIRECTORY_PATH);

        // Build a list of partition filters to select business object data.
        List<List<String>> partitionFilters = new ArrayList<>();
        for (String partitionValue : Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2))
        {
            partitionFilters.add(Arrays.asList(partitionValue, null, null, null, null));
        }

        // Retrieve "available" storage units per specified parameters.
        List<StorageUnitEntity> resultStorageUnitEntities = storageUnitDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION,
            null, Collections.singletonList(STORAGE_NAME), null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(Collections.singletonList(enabledStorageUnitEntity), resultStorageUnitEntities);

        // Retrieve "available" storage units without specifying
        // a business object format version, which is an optional parameter.
        resultStorageUnitEntities = storageUnitDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), partitionFilters, DATA_VERSION, null,
            Collections.singletonList(STORAGE_NAME), null, null, SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(Collections.singletonList(enabledStorageUnitEntity), resultStorageUnitEntities);

        // Retrieve storage units regardless of storage unit status per specified parameters.
        resultStorageUnitEntities = storageUnitDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), partitionFilters, DATA_VERSION,
            null, Collections.singletonList(STORAGE_NAME), null, null, NO_SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(Arrays.asList(enabledStorageUnitEntity, disabledStorageUnitEntity), resultStorageUnitEntities);

        // Retrieve storage units regardless of storage unit status without specifying
        // a business object format version, which is an optional parameter.
        resultStorageUnitEntities = storageUnitDao.getStorageUnitsByPartitionFiltersAndStorages(
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), partitionFilters, DATA_VERSION, null,
            Collections.singletonList(STORAGE_NAME), null, null, NO_SELECT_ONLY_AVAILABLE_STORAGE_UNITS);

        // Validate the results.
        assertEquals(Arrays.asList(enabledStorageUnitEntity, disabledStorageUnitEntity), resultStorageUnitEntities);
    }

    @Test
    public void testGetStorageUnitsByStorageAndBusinessObjectData()
    {
        // Create database entities required for testing.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, TEST_S3_KEY_PREFIX);

        // Retrieve storage unit entities by storage and business object data.
        List<StorageUnitEntity> resultStorageUnitEntities = storageUnitDao.getStorageUnitsByStorageAndBusinessObjectData(storageUnitEntity.getStorage(),
            Collections.singletonList(storageUnitEntity.getBusinessObjectData()));

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
}
