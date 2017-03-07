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
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.IterableUtils;
import org.junit.Test;
import org.springframework.util.Assert;

import org.finra.herd.dao.impl.AbstractHerdDao;
import org.finra.herd.model.api.xml.AttributeValueFilter;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.StoragePolicyPriorityLevel;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

public class BusinessObjectDataDaoTest extends AbstractDaoTest
{

    // BusinessObjectData

    @Test
    public void testGetBusinessObjectDataByAltKey()
    {
        // Execute the same set of tests on business object data entities created with and without subpartition values.
        for (List<String> subPartitionValues : Arrays.asList(SUBPARTITION_VALUES, NO_SUBPARTITION_VALUES))
        {
            // Create a business object data entity.
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, true, BDATA_STATUS);

            // Get the business object data by key.
            BusinessObjectDataEntity resultBusinessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION));

            // Validate the returned object.
            assertNotNull(resultBusinessObjectDataEntity);
            assertEquals(businessObjectDataEntity.getId(), resultBusinessObjectDataEntity.getId());

            // Get the business object data by key without specifying business object format version, which is an optional parameter.
            resultBusinessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION));

            // Validate the returned object.
            assertNotNull(resultBusinessObjectDataEntity);
            assertEquals(businessObjectDataEntity.getId(), resultBusinessObjectDataEntity.getId());

            // Get the business object data by key without specifying both business object format version and business object data version optional parameters.
            resultBusinessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues, null));
            // Validate the returned object.
            assertNotNull(resultBusinessObjectDataEntity);
            assertEquals(businessObjectDataEntity.getId(), resultBusinessObjectDataEntity.getId());
        }
    }

    @Test
    public void testGetBusinessObjectDataByAltKeyNoDataVersionSpecifiedMultipleRecordsFound()
    {
        // Create and persist multiple latest business object data entities for the same format version.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS);

        try
        {
            businessObjectDataDao.getBusinessObjectDataByAltKey(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    null));
            fail("Should throw an IllegalArgumentException when multiple latest business object data instances exist.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one business object data instance with parameters {namespace=\"%s\", " +
                "businessObjectDefinitionName=\"%s\", businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", " +
                "businessObjectFormatVersion=\"%d\", businessObjectDataPartitionValue=\"%s\", businessObjectDataSubPartitionValues=\"%s,%s,%s,%s\", " +
                "businessObjectDataVersion=\"null\", businessObjectDataStatus=\"null\"}.", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2),
                SUBPARTITION_VALUES.get(3)), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataByAltKeyAndStatus()
    {
        // Execute the same set of tests on business object data entities created with and without subpartition values.
        for (List<String> subPartitionValues : Arrays.asList(SUBPARTITION_VALUES, NO_SUBPARTITION_VALUES))
        {
            // Create a business object data entity.
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, true, BDATA_STATUS);

            // Get the business object data by key and business object data status, but without
            // specifying both business object format version and business object data version.
            BusinessObjectDataEntity resultBusinessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKeyAndStatus(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues, null),
                BDATA_STATUS);

            // Validate the results.
            assertNotNull(resultBusinessObjectDataEntity);
            assertEquals(businessObjectDataEntity.getId(), resultBusinessObjectDataEntity.getId());

            // Get the business object data by key using a wrong business object data status and without
            // specifying both business object format version and business object data version.
            resultBusinessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKeyAndStatus(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues, null),
                BDATA_STATUS_2);

            // Validate the results.
            assertNull(resultBusinessObjectDataEntity);
        }
    }

    @Test
    public void testGetBusinessObjectDataByAltKeyAndStatusOlderFormatVersionHasNewerDataVersion()
    {
        // Create two business object data instances that have newer data version in the older format version.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Get the business object data by key using a wrong business object data status and without
        // specifying both business object format version and business object data version.
        BusinessObjectDataEntity resultBusinessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKeyAndStatus(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES, null),
            BDATA_STATUS);

        // Validate the results.
        assertNotNull(resultBusinessObjectDataEntity);
        assertEquals(SECOND_FORMAT_VERSION, resultBusinessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion());
        assertEquals(INITIAL_DATA_VERSION, resultBusinessObjectDataEntity.getVersion());
    }

    @Test
    public void testGetBusinessObjectDataMaxVersion()
    {
        // Execute the same set of tests on the sets of business object data entities with and without subpartition values.
        for (List<String> subPartitionValues : Arrays.asList(SUBPARTITION_VALUES, NO_SUBPARTITION_VALUES))
        {
            // Create multiple version of the same business object data with the latest
            // version not having the latest flag set. The latest flag is incorrectly set for the second version. This is done
            // to validate that the latest flag is not used by this method to find the latest (maximum) business object data version.
            for (Integer version : Arrays.asList(INITIAL_DATA_VERSION, SECOND_DATA_VERSION, THIRD_DATA_VERSION))
            {
                businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        subPartitionValues, version, version.equals(SECOND_DATA_VERSION), BDATA_STATUS);
            }

            // Get the maximum business object data version for this business object data.
            Integer maxVersion = businessObjectDataDao.getBusinessObjectDataMaxVersion(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    null));

            // Validate the results.
            assertEquals(THIRD_DATA_VERSION, maxVersion);
        }
    }

    @Test
    public void testGetBusinessObjectDataPartitionValue()
    {
        // Create database entities required for testing.
        businessObjectDataAvailabilityTestHelper.createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, new ArrayList<>(), new ArrayList<>(),
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, SUBPARTITION_VALUES, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Get the maximum available partition value.
        assertEquals(STORAGE_1_GREATEST_PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, DATA_VERSION,
                BusinessObjectDataStatusEntity.VALID, Arrays.asList(STORAGE_NAME), null, null, null, null));

        // Get the minimum available partition value.
        assertEquals(STORAGE_1_LEAST_PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, DATA_VERSION,
                BusinessObjectDataStatusEntity.VALID, Arrays.asList(STORAGE_NAME), null, null));

        // Get the maximum available partition value by not passing any of the optional parameters.
        assertEquals(STORAGE_1_GREATEST_PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, null, Arrays.asList(STORAGE_NAME),
                null, null, null, null));
    }

    @Test
    public void testGetBusinessObjectDataMaxPartitionValueWithUpperAndLowerBounds()
    {
        // Create database entities required for testing.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION, true, BusinessObjectDataStatusEntity.VALID, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Test retrieving the maximum available partition value using an upper bound partition value.
        assertNull(businessObjectDataDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Arrays.asList(STORAGE_NAME), null, null, PARTITION_VALUE, null));
        assertEquals(PARTITION_VALUE_2, businessObjectDataDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Arrays.asList(STORAGE_NAME), null, null, PARTITION_VALUE_2, null));
        assertEquals(PARTITION_VALUE_2, businessObjectDataDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Arrays.asList(STORAGE_NAME), null, null, PARTITION_VALUE_3, null));

        // Test retrieving the maximum available partition value using a lower bound partition value.
        assertEquals(PARTITION_VALUE_2, businessObjectDataDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Arrays.asList(STORAGE_NAME), null, null, null, PARTITION_VALUE));
        assertEquals(PARTITION_VALUE_2, businessObjectDataDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Arrays.asList(STORAGE_NAME), null, null, null, PARTITION_VALUE_2));
        assertNull(businessObjectDataDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Arrays.asList(STORAGE_NAME), null, null, null, PARTITION_VALUE_3));
    }

    /**
     * This unit test validates that we do not rely on the business object data latest version flag when selecting an aggregate on the business object data
     * partition value.
     */
    @Test
    public void testGetBusinessObjectDataPartitionValueLatestDataVersionNotInStorage()
    {
        // Create a business object format entity.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);
        BusinessObjectFormatEntity businessObjectFormatEntity =
            businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(businessObjectFormatKey, FORMAT_DESCRIPTION, true, PARTITION_KEY);

        // Create two versions of business object data instances with the latest version not located in the test storage.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BDATA_STATUS),
            businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS));

        // Create a storage instance and a storage unit for the initial business object data version only.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME);
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS);
        storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntities.get(0), storageUnitStatusEntity, NO_STORAGE_DIRECTORY_PATH);

        // Get the maximum available partition value in the test storage without specifying business object data version.
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, null, BDATA_STATUS,
                Arrays.asList(STORAGE_NAME), null, null, null, null));

        // Get the minimum available partition value in the test storage without specifying business object data version.
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, null, BDATA_STATUS,
                Arrays.asList(STORAGE_NAME), null, null));
    }

    @Test
    public void testGetBusinessObjectDataPartitionValueBusinessObjectDataStatusSpecified()
    {
        // Create database entities required for testing.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS_2, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_3,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS_2, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Get the maximum available partition value for the relative business object data status without specifying business object data version.
        assertEquals(PARTITION_VALUE_2, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, null, BDATA_STATUS,
                Arrays.asList(STORAGE_NAME), null, null, null, null));
        assertEquals(PARTITION_VALUE_3, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, null, BDATA_STATUS_2,
                Arrays.asList(STORAGE_NAME), null, null, null, null));

        // Get the minimum available partition value the relative business object data status without specifying business object data version.
        assertEquals(PARTITION_VALUE, businessObjectDataDao.getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), null, BDATA_STATUS,
            Arrays.asList(STORAGE_NAME), null, null));
        assertEquals(PARTITION_VALUE_2, businessObjectDataDao.getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), null, BDATA_STATUS_2,
            Arrays.asList(STORAGE_NAME), null, null));
    }

    @Test
    public void testGetBusinessObjectDataPartitionValueNoStorageSpecified()
    {
        // Create database entities required for testing.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE);
        storageUnitDaoTestHelper
            .createStorageUnitEntity(storageEntity.getName(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, STORAGE_UNIT_STATUS,
                NO_STORAGE_DIRECTORY_PATH);

        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Validate that we can retrieve maximum and minimum partition values without specifying storage.
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, null, null, null, null));
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, null, null));
    }

    @Test
    public void testGetBusinessObjectDataPartitionValueStoragePlatformTypeSpecified()
    {
        // Create database entities required for testing.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE);
        storageUnitDaoTestHelper
            .createStorageUnitEntity(storageEntity.getName(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, STORAGE_UNIT_STATUS,
                NO_STORAGE_DIRECTORY_PATH);

        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Validate that we can retrieve maximum and minimum partition values without specifying storage, storage platform, or excluded storage platform type.
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, null, null, null, null));
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, null, null));

        // Validate that we can still retrieve maximum and minimum partition values when we specify correct storage platform type.
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_PLATFORM_CODE, null, null, null));
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_PLATFORM_CODE, null));

        // Validate that we fail to retrieve maximum and minimum partition values when we specify an invalid storage platform type.
        assertNull(businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_PLATFORM_CODE_2, null, null, null));
        assertNull(businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_PLATFORM_CODE_2, null));

        // Validate that specifying storage forces to ignore an invalid storage platform type.
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Arrays.asList(STORAGE_NAME), STORAGE_PLATFORM_CODE_2, null, null, null));
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Arrays.asList(STORAGE_NAME), STORAGE_PLATFORM_CODE_2, null));
    }

    @Test
    public void testGetBusinessObjectDataPartitionValueExcludedStoragePlatformTypeSpecified()
    {
        // Create database entities required for testing.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE);
        storageUnitDaoTestHelper
            .createStorageUnitEntity(storageEntity.getName(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, STORAGE_UNIT_STATUS,
                NO_STORAGE_DIRECTORY_PATH);

        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Validate that we can retrieve maximum and minimum partition values without specifying storage, storage platform, or excluded storage platform type.
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, null, null, null, null));
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, null, null));

        // Validate that we can still retrieve maximum and minimum partition values when we specify an invalid excluded storage platform type.
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, null, STORAGE_PLATFORM_CODE_2, null, null));
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, null, STORAGE_PLATFORM_CODE_2));

        // Validate that we fail to retrieve maximum and minimum partition values when we specify correct excluded storage platform type.
        assertNull(businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, null, STORAGE_PLATFORM_CODE, null, null));
        assertNull(businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, null, STORAGE_PLATFORM_CODE));

        // Validate that specifying storage forces to ignore the excluded storage platform type.
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Arrays.asList(STORAGE_NAME), null, STORAGE_PLATFORM_CODE, null, null));
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Arrays.asList(STORAGE_NAME), null, STORAGE_PLATFORM_CODE));

        // Validate that specifying storage platform type forces to ignore the excluded storage platform type.
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_PLATFORM_CODE, STORAGE_PLATFORM_CODE, null, null));
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, NO_STORAGE_NAMES, STORAGE_PLATFORM_CODE, STORAGE_PLATFORM_CODE));
    }

    @Test
    public void testGetBusinessObjectDataPartitionValueIgnoringNotAvailableStorageUnits()
    {
        // Create database entities required for testing.
        StorageUnitStatusEntity availableStorageUnitStatusEntity =
            storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS, DESCRIPTION, STORAGE_UNIT_STATUS_AVAILABLE_FLAG_SET);
        StorageUnitStatusEntity notAvailableStorageUnitStatusEntity =
            storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS_2, DESCRIPTION, NO_STORAGE_UNIT_STATUS_AVAILABLE_FLAG_SET);
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, availableStorageUnitStatusEntity.getCode(),
                NO_STORAGE_DIRECTORY_PATH);

        // Create a business object format key.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Validate that we can retrieve maximum and minimum partition values.
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Arrays.asList(STORAGE_NAME), null, null, null, null));
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Arrays.asList(STORAGE_NAME), null, null));

        // Change the storage unit status to a "not available" status.
        storageUnitEntity.setStatus(notAvailableStorageUnitStatusEntity);
        storageUnitDao.saveAndRefresh(storageUnitEntity);

        // Validate that we now fail to retrieve maximum and minimum partition values.
        assertNull(businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Arrays.asList(STORAGE_NAME), null, null, null, null));
        assertNull(businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Arrays.asList(STORAGE_NAME), null, null));
    }

    @Test
    public void testGetBusinessObjectDataCount()
    {
        // Create a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION, true,
                PARTITION_KEY, PARTITION_KEY_GROUP);

        // Create a business object data entity associated with the business object format.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Get the number of business object formats that use this partition key group.
        Long result = businessObjectDataDao
            .getBusinessObjectDataCount(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));

        // Validate the results.
        assertEquals(Long.valueOf(1L), result);
    }

    @Test
    public void testGetBusinessObjectDataEntitiesByKey()
    {
        // Execute the same set of tests on the sets of business object data entities with and without subpartition values.
        for (List<String> subPartitionValues : Arrays.asList(SUBPARTITION_VALUES, NO_SUBPARTITION_VALUES))
        {
            // Create two business object data entities that differ on both format version and data version.
            List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, SECOND_DATA_VERSION, false, BDATA_STATUS), businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, THIRD_DATA_VERSION, false, BDATA_STATUS));

            // Retrieve the first business object data entity by specifying the entire business object data key.
            List<BusinessObjectDataEntity> resultBusinessObjectDataEntities = businessObjectDataDao.getBusinessObjectDataEntities(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, SECOND_DATA_VERSION));

            // Validate the results.
            assertNotNull(resultBusinessObjectDataEntities);
            assertEquals(1, resultBusinessObjectDataEntities.size());
            assertEquals(businessObjectDataEntities.get(0).getId(), resultBusinessObjectDataEntities.get(0).getId());

            // Retrieve both business object data entities by not specifying both format and data versions.
            resultBusinessObjectDataEntities = businessObjectDataDao.getBusinessObjectDataEntities(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues, null));

            // Validate the results.
            assertNotNull(resultBusinessObjectDataEntities);
            assertTrue(resultBusinessObjectDataEntities.containsAll(businessObjectDataEntities));
            assertTrue(businessObjectDataEntities.containsAll(resultBusinessObjectDataEntities));
        }
    }

    @Test
    public void testGetBusinessObjectDataEntitiesByPartitionFiltersAndStorage()
    {
        // Create database entities required for testing.
        businessObjectDataAvailabilityTestHelper.createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, new ArrayList<>(), new ArrayList<>(),
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, SUBPARTITION_VALUES, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

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

        // Retrieve the available business object data per specified parameters.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities1 = businessObjectDataDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                partitionFilters, DATA_VERSION, null, STORAGE_NAME);

        // Validate the results.
        assertNotNull(resultBusinessObjectDataEntities1);
        assertEquals(STORAGE_1_AVAILABLE_PARTITION_VALUES.size(), resultBusinessObjectDataEntities1.size());
        for (int i = 0; i < STORAGE_1_AVAILABLE_PARTITION_VALUES.size(); i++)
        {
            assertEquals(STORAGE_1_AVAILABLE_PARTITION_VALUES.get(i), resultBusinessObjectDataEntities1.get(i).getPartitionValue());
        }

        // Retrieve the available business object data without specifying a business object format version, which is an optional parameter.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities2 = businessObjectDataDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), partitionFilters,
                DATA_VERSION, null, STORAGE_NAME);

        // Validate the results.
        assertEquals(resultBusinessObjectDataEntities1, resultBusinessObjectDataEntities2);

        // Retrieve the available business object data without specifying both business object format version and business object data version.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities3 = businessObjectDataDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), partitionFilters,
                null, null, STORAGE_NAME);

        // Validate the results.
        assertEquals(resultBusinessObjectDataEntities1, resultBusinessObjectDataEntities3);

        // Retrieve the business object data with VALID business object data status without
        // specifying both business object format version and business object data version.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities4 = businessObjectDataDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), partitionFilters,
                null, BusinessObjectDataStatusEntity.VALID, STORAGE_NAME);

        // Validate the results.
        assertEquals(resultBusinessObjectDataEntities1, resultBusinessObjectDataEntities4);

        // Retrieve the available business object data with wrong business object data status and without
        // specifying both business object format version and business object data version.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities5 = businessObjectDataDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), partitionFilters,
                null, BDATA_STATUS, STORAGE_NAME);

        // Validate the results.
        assertTrue(resultBusinessObjectDataEntities5.isEmpty());
    }

    @Test
    public void testGetBusinessObjectDataEntitiesByPartitionFiltersAndStorageOlderFormatVersionHasNewerDataVersion()
    {
        // Create two business object data instances that have newer data version in the older format version.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS), businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, SECOND_FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS));

        // Create a storage instance and relative storage units.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME);
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS);
        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities)
        {
            storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatusEntity, NO_STORAGE_DIRECTORY_PATH);
        }

        // Build a list of partition filters to select the "available" business object data.
        List<List<String>> partitionFilters = new ArrayList<>();
        partitionFilters.add(Arrays.asList(PARTITION_VALUE, null, null, null, null));

        // Retrieve the available business object data without specifying both business object format version and business object data version.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities = businessObjectDataDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), partitionFilters,
                null, BDATA_STATUS, STORAGE_NAME);

        // Validate the results.
        assertNotNull(resultBusinessObjectDataEntities);
        assertEquals(1, resultBusinessObjectDataEntities.size());
        assertEquals(SECOND_FORMAT_VERSION, resultBusinessObjectDataEntities.get(0).getBusinessObjectFormat().getBusinessObjectFormatVersion());
        assertEquals(INITIAL_DATA_VERSION, resultBusinessObjectDataEntities.get(0).getVersion());
    }

    /**
     * This unit test validates that we do not rely on the business object data latest version flag when selecting business object entities by partition filters
     * and storage.
     */
    @Test
    public void testGetBusinessObjectDataEntitiesByPartitionFiltersAndStorageLatestDataVersionNotInStorage()
    {
        // Create several versions of business object data instances with the latest version not located in the test storage and
        // the second version having different business object data status.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BDATA_STATUS), businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, false, BDATA_STATUS_2), businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, THIRD_DATA_VERSION, true, BDATA_STATUS));

        // Create a storage instance and relative storage units for the first two business object data versions.
        StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(STORAGE_NAME);
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS);
        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities.subList(0, 2))
        {
            storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatusEntity, NO_STORAGE_DIRECTORY_PATH);
        }

        // Build a list of partition filters to select the "available" business object data.
        List<List<String>> partitionFilters = new ArrayList<>();
        partitionFilters.add(Arrays.asList(PARTITION_VALUE, null, null, null, null));

        // Retrieve a business object data in the test storage without specifying business object data version
        // - the latest available business object data with the specified business object data status.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities = businessObjectDataDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                partitionFilters, null, BDATA_STATUS, STORAGE_NAME);

        // Validate the results - we expect to get back the initial business object data version.
        assertNotNull(resultBusinessObjectDataEntities);
        assertEquals(1, resultBusinessObjectDataEntities.size());
        assertEquals(INITIAL_DATA_VERSION, resultBusinessObjectDataEntities.get(0).getVersion());

        // Retrieve a business object data in the test storage without specifying both business object data status
        // and business object data version - the latest available business object data regardless of the status.
        resultBusinessObjectDataEntities = businessObjectDataDao
            .getBusinessObjectDataEntities(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION),
                partitionFilters, null, null, STORAGE_NAME);

        // Validate the results - we expect to get back the second business object data version.
        assertNotNull(resultBusinessObjectDataEntities);
        assertEquals(1, resultBusinessObjectDataEntities.size());
        assertEquals(SECOND_DATA_VERSION, resultBusinessObjectDataEntities.get(0).getVersion());
    }

    /**
     * Validates that we correctly select business object data entities per specified storage name, threshold minutes and excluded business object status
     * values.
     */
    @Test
    public void testGetBusinessObjectDataFromStorageWithThreshold()
    {
        // Create the database entities required for testing.
        List<String> storageNames = Arrays.asList(STORAGE_NAME, STORAGE_NAME_2);
        List<String> businessObjectDataStatuses = Arrays.asList(BDATA_STATUS, BDATA_STATUS_2);
        List<Integer> createdOnTimestampMinutesOffsets = Arrays.asList(5, 15, 20);
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoTestHelper.createStorageUnitStatusEntity(STORAGE_UNIT_STATUS);
        int counter = 0;
        for (String storageName : storageNames)
        {
            StorageEntity storageEntity = storageDaoTestHelper.createStorageEntity(storageName);
            for (String businessObjectDataStatus : businessObjectDataStatuses)
            {
                for (Integer offset : createdOnTimestampMinutesOffsets)
                {
                    BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                        .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                            String.format("%s-%d", PARTITION_VALUE, counter++), SUBPARTITION_VALUES, DATA_VERSION, true, businessObjectDataStatus);
                    // Apply the offset in minutes to createdOn value.
                    businessObjectDataEntity.setCreatedOn(new Timestamp(businessObjectDataEntity.getCreatedOn().getTime() - offset * 60 * 1000));
                    storageUnitDaoTestHelper
                        .createStorageUnitEntity(storageEntity, businessObjectDataEntity, storageUnitStatusEntity, NO_STORAGE_DIRECTORY_PATH);
                    herdDao.saveAndRefresh(businessObjectDataEntity);
                }
            }
        }

        // Select a subset of test business object entities.
        List<BusinessObjectDataEntity> resultBusinessObjectDataEntities =
            businessObjectDataDao.getBusinessObjectDataFromStorageOlderThan(STORAGE_NAME, 10, Arrays.asList(BDATA_STATUS_2));

        // Validate the results.
        assertNotNull(resultBusinessObjectDataEntities);
        assertEquals(2, resultBusinessObjectDataEntities.size());
        for (BusinessObjectDataEntity businessObjectDataEntity : resultBusinessObjectDataEntities)
        {
            assertEquals(1, businessObjectDataEntity.getStorageUnits().size());
            assertEquals(STORAGE_NAME, IterableUtils.get(businessObjectDataEntity.getStorageUnits(), 0).getStorage().getName());
            assertEquals(BDATA_STATUS, businessObjectDataEntity.getStatus().getCode());
        }
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePolicies()
    {
        // Create and persist a storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // For all possible storage policy priority levels, create and persist a storage policy entity matching to the business object data.
        Map<StoragePolicyPriorityLevel, StoragePolicyEntity> input = new LinkedHashMap<>();

        // Storage policy filter has business object definition, usage, and file type specified.
        input.put(new StoragePolicyPriorityLevel(false, false, false), storagePolicyDaoTestHelper
            .createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET));

        // Storage policy filter has only business object definition specified.
        input.put(new StoragePolicyPriorityLevel(false, true, true), storagePolicyDaoTestHelper
            .createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME_2),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE,
                NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET));

        // Storage policy filter has only usage and file type specified.
        input.put(new StoragePolicyPriorityLevel(true, false, false), storagePolicyDaoTestHelper
            .createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD_2, STORAGE_POLICY_NAME),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, NO_BDEF_NAMESPACE, NO_BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET));

        // Storage policy filter has no fields specified.
        input.put(new StoragePolicyPriorityLevel(true, true, true), storagePolicyDaoTestHelper
            .createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD_2, STORAGE_POLICY_NAME_2),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, NO_BDEF_NAMESPACE, NO_BDEF_NAME, NO_FORMAT_USAGE_CODE,
                NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET));

        // For each storage policy priority level, retrieve business object data matching to the relative storage policy.
        for (Map.Entry<StoragePolicyPriorityLevel, StoragePolicyEntity> entry : input.entrySet())
        {
            StoragePolicyPriorityLevel storagePolicyPriorityLevel = entry.getKey();
            StoragePolicyEntity storagePolicyEntity = entry.getValue();

            // Retrieve the match.
            Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
                .getBusinessObjectDataEntitiesMatchingStoragePolicies(storagePolicyPriorityLevel, Arrays.asList(BDATA_STATUS), 0, MAX_RESULT);

            // Validate the results.
            assertEquals(1, result.size());
            assertTrue(result.containsKey(storageUnitEntity.getBusinessObjectData()));
            assertEquals(storagePolicyEntity, result.get(storageUnitEntity.getBusinessObjectData()));
        }
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesTestingStartPositionAndMaxResult()
    {
        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoTestHelper
            .createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity1 = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Apply the offset in days to business object data "created on" value.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity1.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 1);

        // Create and persist a second storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity2 = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Also apply an offset to business object data "created on" value, but make this business object data older than the first.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity2.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 2);

        // Try to retrieve both business object data instances as matching to the storage policy, but with max result limit set to 1.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 0, 1);

        // Validate the results. Only the oldest business object data should get selected.
        assertEquals(1, result.size());
        assertTrue(result.containsKey(storageUnitEntity2.getBusinessObjectData()));
        assertEquals(storagePolicyEntity, result.get(storageUnitEntity2.getBusinessObjectData()));

        // Try to retrieve the second business object data instance matching to the storage policy
        // by specifying the relative start position and max result limit set.
        result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 1, 1);

        // Validate the results. Now, the second oldest business object data should get selected.
        assertEquals(1, result.size());
        assertTrue(result.containsKey(storageUnitEntity1.getBusinessObjectData()));
        assertEquals(storagePolicyEntity, result.get(storageUnitEntity1.getBusinessObjectData()));
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesMultipleStoragePoliciesMatchBusinessObjectData()
    {
        // Create and persist a storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist two storage policy entities with identical storage policy filters.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME_2),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Retrieve business object data matching storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 0,
                MAX_RESULT);

        // Validate the results. Only a single match should get returned.
        assertEquals(1, result.size());
        assertTrue(result.containsKey(storageUnitEntity.getBusinessObjectData()));
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesInvalidSourceStorage()
    {
        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit which is not in the storage policy filter storage.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME_3, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Try to retrieve the business object data matching to the storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 0,
                MAX_RESULT);

        // Validate the results.
        assertEquals(0, result.size());
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesNoStoragePolicyLatestVersion()
    {
        // Create and persist an enabled storage policy entity that has no latest version flag set.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, NO_LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit in the storage policy filter storage.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Try to retrieve the business object data matching to the storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 0,
                MAX_RESULT);

        // Validate the results.
        assertEquals(0, result.size());
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesInvalidStoragePolicyStatus()
    {
        // Create and persist a disabled storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit in the storage policy filter storage.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Try to retrieve the business object data matching to the storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 0,
                MAX_RESULT);

        // Validate the results.
        assertEquals(0, result.size());
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesInvalidBusinessObjectDataStatus()
    {
        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit in the storage policy filter storage, but having "not supported" business object data status.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS_2, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Try to retrieve the business object data matching to the storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 0,
                MAX_RESULT);

        // Validate the results.
        assertEquals(0, result.size());
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesDestinationStorageUnitAlreadyExists()
    {
        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit in the storage policy filter storage.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Add a storage unit for this business object data in the storage policy destination storage.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME_2, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Try to retrieve the business object data matching to the storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Arrays.asList(BDATA_STATUS), 0,
                MAX_RESULT);

        // Validate the results.
        assertEquals(0, result.size());
    }

    @Test
    public void testGetBusinessObjectDataEntitiesByPartitionValue()
    {
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION,
                true, "VALID");
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, true, "VALID");
        List<BusinessObjectDataEntity> businessObjectDataEntities = businessObjectDataDao.getBusinessObjectDataEntitiesByPartitionValue(PARTITION_VALUE);
        assertEquals(2, businessObjectDataEntities.size());
    }

    @Test
    public void testBusinessObjectDataSearchWithAllSearchKeyFields()
    {
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION,
                true, "VALID");
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION,
                true, "INVALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, true, "INVALID");

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);
        key.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        key.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        key.setBusinessObjectFormatVersion(FORMAT_VERSION);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(2, result.size());

        for (BusinessObjectData data : result)
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(FORMAT_USAGE_CODE.equals(data.getBusinessObjectFormatUsage()));
            Assert.isTrue(FORMAT_FILE_TYPE_CODE.equals(data.getBusinessObjectFormatFileType()));
            Assert.isTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
        }
    }

    @Test
    public void testBusinessObjectDataSearchWithSearchWithBdefKey()
    {
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION,
                true, "VALID");
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME_2, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, true, "INVALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, true, "INVALID");

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
        }
    }

    @Test
    public void testBusinessObjectDataSearchWithAltSearchKeyFields()
    {
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION,
                true, "VALID");
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, true, "INVALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, PARTITION_VALUE, null,
                DATA_VERSION, true, "INVALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, PARTITION_VALUE, null,
                DATA_VERSION, true, "VALID");

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE_2);
        key.setBusinessObjectDefinitionName(BDEF_NAME_2);
        key.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE_2);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(2, result.size());

        businessObjectDataSearchKeys.clear();

        key.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE_2);
        businessObjectDataSearchKeys.add(key);
        filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());

        businessObjectDataSearchKeys.clear();

        key.setBusinessObjectFormatVersion(FORMAT_VERSION_2);
        businessObjectDataSearchKeys.add(key);
        filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());

        businessObjectDataSearchKeys.clear();

        key.setBusinessObjectFormatFileType(null);
        businessObjectDataSearchKeys.add(key);
        filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(2, result.size());

    }

    @Test
    public void testBusinessObjectDataSearchWithSubPartitionValues()
    {
        String[] subpartions = {"P1", "P2", "P3", "P4"};
        List<String> subpartitionList = Arrays.asList(subpartions);

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subpartitionList,
                DATA_VERSION, true, "VALID");
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subpartitionList,
                DATA_VERSION, true, "INVALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subpartitionList,
                DATA_VERSION, true, "INVALID");

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(2, result.size());

        result.get(0).getSubPartitionValues().containsAll(subpartitionList);
    }

    @Test
    public void testBusinessObjectDataSearchWithPartitionValueFilters()
    {
        BusinessObjectDataEntity businessObjectDataEntity = createBusinessObjectEntityForPartitionValueFilterTest();
        String namespace = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode();
        String bDefName = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName();
        String usage = businessObjectDataEntity.getBusinessObjectFormat().getUsage();
        String fileTypeCode = businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode();
        int formatVersion = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion();

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setPartitionKey(PARTITION_KEY);
        List<String> values = new ArrayList<>();
        values.add(PARTITION_VALUE);
        partitionValueFilter.setPartitionValues(values);

        for (int i = 1; i < 5; i++)
        {
            PartitionValueFilter partitionValueFilter2 = new PartitionValueFilter();
            partitionValueFilters.add(partitionValueFilter2);
            partitionValueFilter2.setPartitionKey(PARTITION_KEY + i);
            List<String> values2 = new ArrayList<>();
            //add the expected sub partition value
            values2.add(SUBPARTITION_VALUES.get(i - 1));
            //add some noise, it should be ignored as the query is doing in
            values2.add("NOISE");
            partitionValueFilter2.setPartitionValues(values2);
        }

        key.setPartitionValueFilters(partitionValueFilters);

        key.setNamespace(namespace);
        key.setBusinessObjectDefinitionName(bDefName);
        key.setBusinessObjectFormatUsage(usage);
        key.setBusinessObjectFormatFileType(fileTypeCode);
        key.setBusinessObjectFormatVersion(formatVersion);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(FORMAT_USAGE_CODE.equals(data.getBusinessObjectFormatUsage()));
            Assert.isTrue(FORMAT_FILE_TYPE_CODE.equals(data.getBusinessObjectFormatFileType()));
            Assert.isTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
        }
    }

    @Test
    public void testBusinessObjectDataSearchWithPartitionValueRangeFilters()
    {
        BusinessObjectDataEntity businessObjectDataEntity = createBusinessObjectEntityForPartitionValueFilterTest();
        String namespace = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode();
        String bDefName = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName();
        String usage = businessObjectDataEntity.getBusinessObjectFormat().getUsage();
        String fileTypeCode = businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode();
        int formatVersion = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion();

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setPartitionKey(PARTITION_KEY);

        PartitionValueRange partitionValueRange = new PartitionValueRange();
        partitionValueRange.setStartPartitionValue(PARTITION_VALUE);
        partitionValueRange.setEndPartitionValue(PARTITION_VALUE + "1");
        partitionValueFilter.setPartitionValueRange(partitionValueRange);

        key.setPartitionValueFilters(partitionValueFilters);
        key.setNamespace(namespace);
        key.setBusinessObjectDefinitionName(bDefName);
        key.setBusinessObjectFormatUsage(usage);
        key.setBusinessObjectFormatFileType(fileTypeCode);
        key.setBusinessObjectFormatVersion(formatVersion);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            Assert.isTrue(namespace.equals(data.getNamespace()));
            Assert.isTrue(bDefName.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(usage.equals(data.getBusinessObjectFormatUsage()));
            Assert.isTrue(fileTypeCode.equals(data.getBusinessObjectFormatFileType()));
            Assert.isTrue(formatVersion == data.getBusinessObjectFormatVersion());
        }

    }


    @Test
    public void testBusinessObjectDataSearchWithPartitionValueRangeFiltersSubPartition()
    {
        BusinessObjectDataEntity businessObjectDataEntity = createBusinessObjectEntityForPartitionValueFilterTest();
        String namespace = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode();
        String bDefName = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName();
        String usage = businessObjectDataEntity.getBusinessObjectFormat().getUsage();
        String fileTypeCode = businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode();
        int formatVersion = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion();

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setPartitionKey(PARTITION_KEY + "1");

        PartitionValueRange partitionValueRange = new PartitionValueRange();
        partitionValueRange.setStartPartitionValue(businessObjectDataEntity.getPartitionValue2());
        partitionValueRange.setEndPartitionValue(businessObjectDataEntity.getPartitionValue2() + "1");
        partitionValueFilter.setPartitionValueRange(partitionValueRange);

        key.setPartitionValueFilters(partitionValueFilters);
        key.setNamespace(namespace);
        key.setBusinessObjectDefinitionName(bDefName);
        key.setBusinessObjectFormatUsage(usage);
        key.setBusinessObjectFormatFileType(fileTypeCode);
        key.setBusinessObjectFormatVersion(formatVersion);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            Assert.isTrue(namespace.equals(data.getNamespace()));
            Assert.isTrue(bDefName.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(usage.equals(data.getBusinessObjectFormatUsage()));
            Assert.isTrue(fileTypeCode.equals(data.getBusinessObjectFormatFileType()));
            Assert.isTrue(formatVersion == data.getBusinessObjectFormatVersion());
        }

    }

    @Test
    public void testBusinessObjectDataSearchWithPartitionValueRangeFiltersNegative()
    {
        BusinessObjectDataEntity businessObjectDataEntity = createBusinessObjectEntityForPartitionValueFilterTest();
        String namespace = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode();
        String bDefName = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName();
        String usage = businessObjectDataEntity.getBusinessObjectFormat().getUsage();
        String fileTypeCode = businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode();
        int formatVerion = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion();

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setPartitionKey(PARTITION_KEY + "6");

        PartitionValueRange partitionValueRange = new PartitionValueRange();
        partitionValueRange.setStartPartitionValue(PARTITION_VALUE);
        partitionValueRange.setEndPartitionValue(PARTITION_VALUE + "1");
        partitionValueFilter.setPartitionValueRange(partitionValueRange);

        key.setPartitionValueFilters(partitionValueFilters);
        key.setNamespace(namespace);
        key.setBusinessObjectDefinitionName(bDefName);
        key.setBusinessObjectFormatUsage(usage);
        key.setBusinessObjectFormatFileType(fileTypeCode);
        key.setBusinessObjectFormatVersion(formatVerion);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(0, result.size());
    }

    private BusinessObjectDataEntity createBusinessObjectEntityForPartitionValueFilterTest()
    {
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY);
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY + "1");
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY + "2");
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY + "3");
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }
        {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(PARTITION_KEY + "4");
            schemaColumn.setType("STRING");
            schemaColumns.add(schemaColumn);
        }

        NamespaceEntity namespaceEntity = super.namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        DataProviderEntity dataProviderEntity = super.dataProviderDaoTestHelper.createDataProviderEntity(DATA_PROVIDER_NAME);
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = super.businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(namespaceEntity, BDEF_NAME, dataProviderEntity, NO_BDEF_DESCRIPTION, NO_BDEF_DISPLAY_NAME, NO_ATTRIBUTES,
                NO_SAMPLE_DATA_FILES);
        FileTypeEntity fileTypeEntity = super.fileTypeDaoTestHelper.createFileTypeEntity(FORMAT_FILE_TYPE_CODE, FORMAT_DESCRIPTION);

        BusinessObjectFormatEntity businessObjectFormatEntity = super.businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity, FORMAT_VERSION, null, true, PARTITION_KEY,
                null, NO_ATTRIBUTES, null, null, null, schemaColumns, schemaColumns);
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS, DESCRIPTION, BDATA_STATUS_PRE_REGISTRATION_FLAG_SET);
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true,
                businessObjectDataStatusEntity.getCode());

        return businessObjectDataEntity;
    }

    @Test
    public void testBusinessObjectDataSearchWithPartitionValueAndRangeFilters()
    {
        BusinessObjectDataEntity businessObjectDataEntity = createBusinessObjectEntityForPartitionValueFilterTest();
        String namespace = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode();
        String bDefName = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName();
        String usage = businessObjectDataEntity.getBusinessObjectFormat().getUsage();
        String fileTypeCode = businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode();
        int formatVersion = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion();

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setPartitionKey(PARTITION_KEY.toLowerCase());
        List<String> values = new ArrayList<>();
        values.add(PARTITION_VALUE);
        partitionValueFilter.setPartitionValues(values);

        PartitionValueFilter partitionValueFilter2 = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter2);
        partitionValueFilter2.setPartitionKey(PARTITION_KEY + "1");

        PartitionValueRange partitionValueRange = new PartitionValueRange();
        partitionValueRange.setStartPartitionValue(businessObjectDataEntity.getPartitionValue2());
        partitionValueRange.setEndPartitionValue(businessObjectDataEntity.getPartitionValue2() + "1");
        partitionValueFilter2.setPartitionValueRange(partitionValueRange);

        key.setPartitionValueFilters(partitionValueFilters);

        key.setNamespace(namespace);
        key.setBusinessObjectDefinitionName(bDefName);
        key.setBusinessObjectFormatUsage(usage);
        key.setBusinessObjectFormatFileType(fileTypeCode);
        key.setBusinessObjectFormatVersion(formatVersion);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(FORMAT_USAGE_CODE.equals(data.getBusinessObjectFormatUsage()));
            Assert.isTrue(FORMAT_FILE_TYPE_CODE.equals(data.getBusinessObjectFormatFileType()));
            Assert.isTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
        }
    }


    @Test
    public void testBusinessObjectDataSearchWithAttributeValueFilters()
    {
        // Create and persist an attribute for the business object data
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2);

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataAttributeEntity.getBusinessObjectData();

        String namespace = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode();
        String bDefName = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName();
        String usage = businessObjectDataEntity.getBusinessObjectFormat().getUsage();
        String fileTypeCode = businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode();
        int formatVersion = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion();

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));

        key.setAttributeValueFilters(attributeValueFilters);

        key.setNamespace(namespace);
        key.setBusinessObjectDefinitionName(bDefName);
        key.setBusinessObjectFormatUsage(usage);
        key.setBusinessObjectFormatFileType(fileTypeCode);
        key.setBusinessObjectFormatVersion(formatVersion);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(FORMAT_USAGE_CODE.equals(data.getBusinessObjectFormatUsage()));
            Assert.isTrue(FORMAT_FILE_TYPE_CODE.equals(data.getBusinessObjectFormatFileType()));
            Assert.isTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
            Assert.isTrue(ATTRIBUTE_NAME_1_MIXED_CASE.equals(data.getAttributes().get(0).getName()));
            Assert.isTrue(ATTRIBUTE_VALUE_1.equals(data.getAttributes().get(0).getValue()));
        }
    }

    @Test
    public void testBusinessObjectDataSearchWithAttributeValueFiltersWithAttributeName()
    {
        // Create and persist an attribute for the business object data
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2);

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataAttributeEntity.getBusinessObjectData();

        String namespace = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode();
        String bDefName = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName();
        String usage = businessObjectDataEntity.getBusinessObjectFormat().getUsage();
        String fileTypeCode = businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode();
        int formatVersion = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion();

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(ATTRIBUTE_NAME_1_MIXED_CASE, null));

        key.setAttributeValueFilters(attributeValueFilters);

        key.setNamespace(namespace);
        key.setBusinessObjectDefinitionName(bDefName);
        key.setBusinessObjectFormatUsage(usage);
        key.setBusinessObjectFormatFileType(fileTypeCode);
        key.setBusinessObjectFormatVersion(formatVersion);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(FORMAT_USAGE_CODE.equals(data.getBusinessObjectFormatUsage()));
            Assert.isTrue(FORMAT_FILE_TYPE_CODE.equals(data.getBusinessObjectFormatFileType()));
            Assert.isTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
            Assert.isTrue(ATTRIBUTE_NAME_1_MIXED_CASE.equals(data.getAttributes().get(0).getName()));
            Assert.isTrue(ATTRIBUTE_VALUE_1.equals(data.getAttributes().get(0).getValue()));
        }
    }

    @Test
    public void testBusinessObjectDataSearchWithAttributeValueFiltersWithAttributeValue()
    {
        // Create and persist an attribute for the business object data
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_1);

        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataAttributeEntity.getBusinessObjectData();

        String namespace = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode();
        String bDefName = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName();
        String usage = businessObjectDataEntity.getBusinessObjectFormat().getUsage();
        String fileTypeCode = businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode();
        int formatVersion = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion();

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(null, ATTRIBUTE_VALUE_1));

        key.setAttributeValueFilters(attributeValueFilters);

        key.setNamespace(namespace);
        key.setBusinessObjectDefinitionName(bDefName);
        key.setBusinessObjectFormatUsage(usage);
        key.setBusinessObjectFormatFileType(fileTypeCode);
        key.setBusinessObjectFormatVersion(formatVersion);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());
        Set<String> expectedAttributeNames = new HashSet<String>(Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_NAME_2_MIXED_CASE));
        for (BusinessObjectData data : result)
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(FORMAT_USAGE_CODE.equals(data.getBusinessObjectFormatUsage()));
            Assert.isTrue(FORMAT_FILE_TYPE_CODE.equals(data.getBusinessObjectFormatFileType()));
            Assert.isTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
            assertEquals(data.getAttributes().size(), 2);
            Assert.isTrue(expectedAttributeNames.contains((data.getAttributes().get(0).getName())));
            Assert.isTrue(expectedAttributeNames.contains((data.getAttributes().get(1).getName())));
        }
    }

    @Test
    public void testBusinessObjectDataSearchWithAttributeValueFiltersWithNoAttributeValueFilter()
    {
        // Create and persist an attribute for the business object data
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataAttributeEntity.getBusinessObjectData();

        String namespace = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode();
        String bDefName = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName();
        String usage = businessObjectDataEntity.getBusinessObjectFormat().getUsage();
        String fileTypeCode = businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode();
        int formatVersion = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion();

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        key.setNamespace(namespace);
        key.setBusinessObjectDefinitionName(bDefName);
        key.setBusinessObjectFormatUsage(usage);
        key.setBusinessObjectFormatFileType(fileTypeCode);
        key.setBusinessObjectFormatVersion(formatVersion);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(FORMAT_USAGE_CODE.equals(data.getBusinessObjectFormatUsage()));
            Assert.isTrue(FORMAT_FILE_TYPE_CODE.equals(data.getBusinessObjectFormatFileType()));
            Assert.isTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
            Assert.isNull(data.getAttributes());
        }
    }

    @Test
    public void testBusinessObjectDataSearchWithAttributeValueFiltersPartialAttributeValueMatch()
    {
        // Create and persist an attribute for the business object data
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2);

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataAttributeEntity.getBusinessObjectData();

        String namespace = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode();
        String bDefName = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName();
        String usage = businessObjectDataEntity.getBusinessObjectFormat().getUsage();
        String fileTypeCode = businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode();
        int formatVersion = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion();

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1.substring(2, 5)));

        key.setAttributeValueFilters(attributeValueFilters);

        key.setNamespace(namespace);
        key.setBusinessObjectDefinitionName(bDefName);
        key.setBusinessObjectFormatUsage(usage);
        key.setBusinessObjectFormatFileType(fileTypeCode);
        key.setBusinessObjectFormatVersion(formatVersion);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(FORMAT_USAGE_CODE.equals(data.getBusinessObjectFormatUsage()));
            Assert.isTrue(FORMAT_FILE_TYPE_CODE.equals(data.getBusinessObjectFormatFileType()));
            Assert.isTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
            Assert.isTrue(ATTRIBUTE_NAME_1_MIXED_CASE.equals(data.getAttributes().get(0).getName()));
            Assert.isTrue(ATTRIBUTE_VALUE_1.equals(data.getAttributes().get(0).getValue()));
        }
    }

    @Test
    public void testBusinessObjectDataSearchWithAttributeValueFiltersResponseCheckMatch()
    {
        // Create and persist an attribute for the business object data
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_1);

        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataAttributeEntity.getBusinessObjectData();

        String namespace = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode();
        String bDefName = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName();
        String usage = businessObjectDataEntity.getBusinessObjectFormat().getUsage();
        String fileTypeCode = businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode();
        int formatVersion = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion();

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
        attributeValueFilters.add(new AttributeValueFilter(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_1));
        key.setAttributeValueFilters(attributeValueFilters);

        key.setNamespace(namespace);
        key.setBusinessObjectDefinitionName(bDefName);
        key.setBusinessObjectFormatUsage(usage);
        key.setBusinessObjectFormatFileType(fileTypeCode);
        key.setBusinessObjectFormatVersion(formatVersion);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(FORMAT_USAGE_CODE.equals(data.getBusinessObjectFormatUsage()));
            Assert.isTrue(FORMAT_FILE_TYPE_CODE.equals(data.getBusinessObjectFormatFileType()));
            Assert.isTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
            assertEquals(data.getAttributes().size(), 2);
            Assert.isTrue(ATTRIBUTE_NAME_1_MIXED_CASE.equals(data.getAttributes().get(0).getName()));
            Assert.isTrue(ATTRIBUTE_NAME_2_MIXED_CASE.equals(data.getAttributes().get(1).getName()));
            Assert.isTrue(ATTRIBUTE_VALUE_1.equals(data.getAttributes().get(0).getValue()));
        }
    }

    @Test
    public void testBusinessObjectDataSearchWithLatestValidVersion()
    {
        // Create multiple version of the same business object data with the latest
        // version not having the latest flag set. The latest flag is incorrectly set for the second version. This is done
        // to validate that the latest flag is not used by this method to find the latest (maximum) business object data version
        BusinessObjectData expectedBData1 = null;
        String BData_Status = "VALID";
        for (Integer version : Arrays.asList(INITIAL_DATA_VERSION, SECOND_DATA_VERSION, THIRD_DATA_VERSION))
        {
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, version, version.equals(SECOND_DATA_VERSION), BData_Status);

            if (version == THIRD_DATA_VERSION)
            {
                expectedBData1 =
                    new BusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                        AbstractDaoTest.PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, version, businessObjectDataEntity.getLatestVersion(), BData_Status,
                        null, null, null, null, null);
            }
        }

        BusinessObjectData expectedBData2 = null;
        for (Integer version : Arrays.asList(INITIAL_DATA_VERSION, SECOND_DATA_VERSION, THIRD_DATA_VERSION))
        {
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, PARTITION_VALUE,
                    SUBPARTITION_VALUES, version, version.equals(SECOND_DATA_VERSION), BData_Status);

            if (version == THIRD_DATA_VERSION)
            {
                expectedBData2 =
                    new BusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2,
                        AbstractDaoTest.PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, version, businessObjectDataEntity.getLatestVersion(), BData_Status,
                        null, null, null, null, null);
            }
        }

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);
        key.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        key.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        key.setFilterOnLatestValidVersion(true);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(2, result.size());
        assertTrue(result.contains(expectedBData1));
        assertTrue(result.contains(expectedBData2));
    }

    @Test
    public void testBusinessObjectDataSearchWithLatestValidVersionNoExisting()
    {
        // Create multiple version of the same business object data with the latest
        // version not having the latest flag set. The latest flag is incorrectly set for the second version. This is done
        // to validate that the latest flag is not used by this method to find the latest (maximum) business object data version.
        for (Integer version : Arrays.asList(INITIAL_DATA_VERSION, SECOND_DATA_VERSION, THIRD_DATA_VERSION))
        {
            businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, version, version.equals(SECOND_DATA_VERSION), BDATA_STATUS);
        }

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);
        key.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        key.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        key.setBusinessObjectFormatVersion(FORMAT_VERSION);
        key.setFilterOnLatestValidVersion(true);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        assertEquals(0, result.size());
    }

    @Test
    public void testBusinessObjectDataSearchWithLatestValidVersionLatestVersionIsInvalid()
    {
        // create multiple version of the same business object data with the latest
        // version not having the latest flag set. the latest flag is incorrectly set for the second version. this is done
        // to validate that the latest flag is not used by this method to find the latest (maximum) business object data version.
        BusinessObjectData expectedBData = null;
        for (Integer version : Arrays.asList(INITIAL_DATA_VERSION, SECOND_DATA_VERSION, THIRD_DATA_VERSION))
        {
            String BData_Status = "VALID";
            if (version == THIRD_DATA_VERSION)
            {
                BData_Status = "INVALID";
            }
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, version, version.equals(SECOND_DATA_VERSION), BData_Status);
            if (version == SECOND_DATA_VERSION)
            {
                expectedBData =
                    new BusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                        AbstractDaoTest.PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, version, businessObjectDataEntity.getLatestVersion(), BData_Status,
                        null, null, null, null, null);
            }

        }

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();

        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);
        key.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        key.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        key.setBusinessObjectFormatVersion(FORMAT_VERSION);
        key.setFilterOnLatestValidVersion(true);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        List<BusinessObjectData> expectedResult = new ArrayList<>(Arrays.asList(expectedBData));
        assertEquals(1, result.size());
        assertEquals(result, expectedResult);
    }

    @Test
    public void testGetBusinessObjectDataByBusinessObjectDefinition()
    {
        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create two business object data entities that belong to the same business object definition, but to two different business object formats.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Retrieve the keys for business object data by specifying business object definition and without setting a maximum number of results.
        assertEquals(Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION)),
            businessObjectDataDao.getBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionEntity, NO_MAX_RESULTS));

        // Retrieve the keys for business object data by specifying business object definition and with maximum number of results set to 1.
        assertEquals(Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION)), businessObjectDataDao.getBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionEntity, MAX_RESULTS_1));
    }

    @Test
    public void testGetBusinessObjectDataByBusinessObjectFormat()
    {
        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY, PARTITION_KEY_GROUP);

        // Create two business object data entities that belong to the same business object format version.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Retrieve the keys for business object data by specifying business object format and without setting a maximum number of results.
        assertEquals(Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION),
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION)), businessObjectDataDao.getBusinessObjectDataByBusinessObjectFormat(businessObjectFormatEntity, NO_MAX_RESULTS));

        // Retrieve the keys for business object data by specifying business object format and with maximum number of results set to 1.
        assertEquals(Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION)),
            businessObjectDataDao.getBusinessObjectDataByBusinessObjectFormat(businessObjectFormatEntity, MAX_RESULTS_1));
    }

    @Test
    public void testBusinessObjectDataSearchWithMaxRecordsExceeded() throws Exception
    {
        // Override configuration.
        int maxRecordSizeOverride = 2;
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_RESULTS.getKey(), maxRecordSizeOverride);
        modifyPropertySourceInEnvironment(overrideMap);


        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION,
                true, "VALID");
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION,
                true, "INVALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, PARTITION_VALUE, null,
                DATA_VERSION, true, "INVALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, PARTITION_VALUE, null,
                DATA_VERSION, true, "VALID");

        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);

        int recordCountExpected = 3;

        String expectedErrorMessage = String
            .format("Result limit of %d exceeded. Total result size %d. Modify filters to further limit results.", maxRecordSizeOverride, recordCountExpected);

        try
        {
            List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(filters);
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(e.getMessage(), expectedErrorMessage);
        }
    }
}
