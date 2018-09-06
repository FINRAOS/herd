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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.impl.AbstractHerdDao;
import org.finra.herd.model.api.xml.AttributeValueFilter;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.RegistrationDateRangeFilter;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.StoragePolicyPriorityLevel;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.RetentionTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

public class BusinessObjectDataDaoTest extends AbstractDaoTest
{
    /**
     * The default page number for the business object data search
     */
    private static final Integer DEFAULT_PAGE_NUMBER = 1;

    /**
     * The default page size for the business object data search
     */
    private static final Integer DEFAULT_PAGE_SIZE = 1_000;

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
                BusinessObjectDataStatusEntity.VALID, Collections.singletonList(STORAGE_NAME), null, null, null, null));

        // Get the minimum available partition value.
        assertEquals(STORAGE_1_LEAST_PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, DATA_VERSION,
                BusinessObjectDataStatusEntity.VALID, Collections.singletonList(STORAGE_NAME), null, null));

        // Get the maximum available partition value by not passing any of the optional parameters.
        assertEquals(STORAGE_1_GREATEST_PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, null,
                Collections.singletonList(STORAGE_NAME), null, null, null, null));
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
            Collections.singletonList(STORAGE_NAME), null, null, PARTITION_VALUE, null));
        assertEquals(PARTITION_VALUE_2, businessObjectDataDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Collections.singletonList(STORAGE_NAME), null, null, PARTITION_VALUE_2, null));
        assertEquals(PARTITION_VALUE_2, businessObjectDataDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Collections.singletonList(STORAGE_NAME), null, null, PARTITION_VALUE_3, null));

        // Test retrieving the maximum available partition value using a lower bound partition value.
        assertEquals(PARTITION_VALUE_2, businessObjectDataDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Collections.singletonList(STORAGE_NAME), null, null, null, PARTITION_VALUE));
        assertEquals(PARTITION_VALUE_2, businessObjectDataDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Collections.singletonList(STORAGE_NAME), null, null, null, PARTITION_VALUE_2));
        assertNull(businessObjectDataDao.getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null), null, BusinessObjectDataStatusEntity.VALID,
            Collections.singletonList(STORAGE_NAME), null, null, null, PARTITION_VALUE_3));
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
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(businessObjectFormatKey, FORMAT_DESCRIPTION, FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY);

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
                Collections.singletonList(STORAGE_NAME), null, null, null, null));

        // Get the minimum available partition value in the test storage without specifying business object data version.
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, null, BDATA_STATUS,
                Collections.singletonList(STORAGE_NAME), null, null));
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
                Collections.singletonList(STORAGE_NAME), null, null, null, null));
        assertEquals(PARTITION_VALUE_3, businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, null, BDATA_STATUS_2,
                Collections.singletonList(STORAGE_NAME), null, null, null, null));

        // Get the minimum available partition value the relative business object data status without specifying business object data version.
        assertEquals(PARTITION_VALUE, businessObjectDataDao.getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), null, BDATA_STATUS,
            Collections.singletonList(STORAGE_NAME), null, null));
        assertEquals(PARTITION_VALUE_2, businessObjectDataDao.getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION), null, BDATA_STATUS_2,
            Collections.singletonList(STORAGE_NAME), null, null));
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
                NO_BDATA_STATUS, Collections.singletonList(STORAGE_NAME), STORAGE_PLATFORM_CODE_2, null, null, null));
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Collections.singletonList(STORAGE_NAME), STORAGE_PLATFORM_CODE_2, null));
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
                NO_BDATA_STATUS, Collections.singletonList(STORAGE_NAME), null, STORAGE_PLATFORM_CODE, null, null));
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Collections.singletonList(STORAGE_NAME), null, STORAGE_PLATFORM_CODE));

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
                NO_BDATA_STATUS, Collections.singletonList(STORAGE_NAME), null, null, null, null));
        assertEquals(PARTITION_VALUE, businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Collections.singletonList(STORAGE_NAME), null, null));

        // Change the storage unit status to a "not available" status.
        storageUnitEntity.setStatus(notAvailableStorageUnitStatusEntity);
        storageUnitDao.saveAndRefresh(storageUnitEntity);

        // Validate that we now fail to retrieve maximum and minimum partition values.
        assertNull(businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Collections.singletonList(STORAGE_NAME), null, null, null, null));
        assertNull(businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, NO_DATA_VERSION,
                NO_BDATA_STATUS, Collections.singletonList(STORAGE_NAME), null, null));
    }

    @Test
    public void testGetBusinessObjectDataCount()
    {
        // Create a business object format.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY, PARTITION_KEY_GROUP);

        // Create a business object data entity associated with the business object format.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Get the number of business object formats that use this partition key group.
        Long result = businessObjectDataDao
            .getBusinessObjectDataCount(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION));

        // Validate the results.
        assertEquals(Long.valueOf(1L), result);
    }

    @Test
    public void testGetBusinessObjectDataCountBySearchKeyWithAllSearchKeyFields()
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(FORMAT_VERSION);

        Long result = businessObjectDataDao.getBusinessObjectDataCountBySearchKey(businessObjectDataSearchKey);
        assertEquals(Long.valueOf(2), result);
    }

    @Test
    public void testGetBusinessObjectDataCountMissingOptionalParameters()
    {
        // Create business object data entities required for testing.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.INVALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME_2, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Execute the count query by passing business object data search key parameters, except for filters.
        assertEquals(Long.valueOf(2), businessObjectDataDao.getBusinessObjectDataCountBySearchKey(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION)));

        // Execute the count query without optional parameters.
        assertEquals(Long.valueOf(5), businessObjectDataDao.getBusinessObjectDataCountBySearchKey(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION)));
    }

    @Test
    public void testGetBusinessObjectDataCountInvalidParameterValues()
    {
        // Create business object data entities required for testing.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.INVALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Execute the count query by passing business object data search key parameters, except for filters.
        assertEquals(Long.valueOf(2), businessObjectDataDao.getBusinessObjectDataCountBySearchKey(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION)));

        // Execute the count query using non-existing/invalid parameter values.
        assertEquals(Long.valueOf(0), businessObjectDataDao.getBusinessObjectDataCountBySearchKey(
            new BusinessObjectDataSearchKey(I_DO_NOT_EXIST, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION)));
        assertEquals(Long.valueOf(0), businessObjectDataDao.getBusinessObjectDataCountBySearchKey(
            new BusinessObjectDataSearchKey(NAMESPACE, I_DO_NOT_EXIST, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION)));
        assertEquals(Long.valueOf(0), businessObjectDataDao.getBusinessObjectDataCountBySearchKey(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, I_DO_NOT_EXIST, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION)));
        assertEquals(Long.valueOf(0), businessObjectDataDao.getBusinessObjectDataCountBySearchKey(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION)));
        assertEquals(Long.valueOf(0), businessObjectDataDao.getBusinessObjectDataCountBySearchKey(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION)));
    }

    @Test
    public void testGetBusinessObjectDataCountBySearchKeyWithRetentionExpirationFilterNoRetentionInformationConfigured()
    {
        // Create business object data entities required for testing.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.INVALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Execute the count query by passing business object data search key parameters, except for filters.
        assertEquals(Long.valueOf(2), businessObjectDataDao.getBusinessObjectDataCountBySearchKey(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION)));

        // Execute the same count query, but with retention expiration filter.
        // We expect 0 count, since no retention information is configured for the business object format.
        assertEquals(Long.valueOf(0), businessObjectDataDao.getBusinessObjectDataCountBySearchKey(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, FILTER_ON_RETENTION_EXPIRATION)));
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

    @Test
    public void testGetBusinessObjectDataFromStorageWithThreshold()
    {
        // Test offset value in minutes.
        final int OFFSET_IN_MINUTES = 10;

        // Create two storage entities.
        List<StorageEntity> storageEntities =
            Arrays.asList(storageDaoTestHelper.createStorageEntity(STORAGE_NAME), storageDaoTestHelper.createStorageEntity(STORAGE_NAME_2));

        // Create a business object data entity and apply test offset value to its createdOn timestamp.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS);
        businessObjectDataEntity.setCreatedOn(new Timestamp(businessObjectDataEntity.getCreatedOn().getTime() - OFFSET_IN_MINUTES * 60 * 1000));
        herdDao.saveAndRefresh(businessObjectDataEntity);

        // Create a storage unit for the business object data in the first storage.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(storageEntities.get(0), businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Select business object data by specifying valid input parameters.
        assertEquals(Collections.singletonList(businessObjectDataEntity), businessObjectDataDao
            .getBusinessObjectDataFromStorageOlderThan(storageEntities.get(0), OFFSET_IN_MINUTES - 1, Collections.singletonList(BDATA_STATUS)));

        // Try invalid values for all input parameters.
        assertEquals(0, businessObjectDataDao
            .getBusinessObjectDataFromStorageOlderThan(storageEntities.get(1), OFFSET_IN_MINUTES - 1, Collections.singletonList(BDATA_STATUS)).size());
        assertEquals(0, businessObjectDataDao
            .getBusinessObjectDataFromStorageOlderThan(storageEntities.get(0), OFFSET_IN_MINUTES + 1, Collections.singletonList(BDATA_STATUS)).size());
        assertEquals(0, businessObjectDataDao
            .getBusinessObjectDataFromStorageOlderThan(storageEntities.get(0), OFFSET_IN_MINUTES - 1, Collections.singletonList(BDATA_STATUS_2)).size());
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePolicies()
    {
        // Create and persist a storage unit with ENABLED status in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // For all possible storage policy priority levels, create and persist a storage policy entity matching to the business object data.
        Map<StoragePolicyPriorityLevel, StoragePolicyEntity> input = new LinkedHashMap<>();

        // Storage policy filter has business object definition, usage, and file type specified.
        input.put(new StoragePolicyPriorityLevel(false, false, false), storagePolicyDaoTestHelper
            .createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET));

        // Storage policy filter has only business object definition specified.
        input.put(new StoragePolicyPriorityLevel(false, true, true), storagePolicyDaoTestHelper
            .createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME_2),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE,
                NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET));

        // Storage policy filter has only usage and file type specified.
        input.put(new StoragePolicyPriorityLevel(true, false, false), storagePolicyDaoTestHelper
            .createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD_2, STORAGE_POLICY_NAME),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, NO_BDEF_NAMESPACE, NO_BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET));

        // Storage policy filter has no fields specified.
        input.put(new StoragePolicyPriorityLevel(true, true, true), storagePolicyDaoTestHelper
            .createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD_2, STORAGE_POLICY_NAME_2),
                StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, NO_BDEF_NAMESPACE, NO_BDEF_NAME, NO_FORMAT_USAGE_CODE,
                NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET));

        // For each storage policy priority level, retrieve business object data matching to the relative storage policy.
        for (Map.Entry<StoragePolicyPriorityLevel, StoragePolicyEntity> entry : input.entrySet())
        {
            StoragePolicyPriorityLevel storagePolicyPriorityLevel = entry.getKey();
            StoragePolicyEntity storagePolicyEntity = entry.getValue();

            // Retrieve the match.
            Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
                .getBusinessObjectDataEntitiesMatchingStoragePolicies(storagePolicyPriorityLevel, Collections.singletonList(BDATA_STATUS), 0, 0, MAX_RESULT);

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
                STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit with ENABLED status in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity1 = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Apply the offset in days to business object data "created on" value.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity1.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 1);

        // Create and persist a second storage unit with ENABLED status in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity2 = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Also apply an offset to business object data "created on" value, but make this business object data older than the first.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity2.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 2);

        // Try to retrieve both business object data instances as matching to the storage policy, but with max result limit set to 1.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Collections.singletonList(BDATA_STATUS),
                0, 0, 1);

        // Validate the results. Only the oldest business object data should get selected.
        assertEquals(1, result.size());
        assertTrue(result.containsKey(storageUnitEntity2.getBusinessObjectData()));
        assertEquals(storagePolicyEntity, result.get(storageUnitEntity2.getBusinessObjectData()));

        // Try to retrieve the second business object data instance matching to the storage policy
        // by specifying the relative start position and max result limit set.
        result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Collections.singletonList(BDATA_STATUS),
                0, 1, 1);

        // Validate the results. Now, the second oldest business object data should get selected.
        assertEquals(1, result.size());
        assertTrue(result.containsKey(storageUnitEntity1.getBusinessObjectData()));
        assertEquals(storagePolicyEntity, result.get(storageUnitEntity1.getBusinessObjectData()));
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesMultipleStoragePoliciesMatchBusinessObjectData()
    {
        // Create and persist a storage unit with ENABLED status in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist two storage policy entities with identical storage policy filters.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME_2),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Retrieve business object data matching storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Collections.singletonList(BDATA_STATUS),
                0, 0, MAX_RESULT);

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
            STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit with ENABLED status which is not in the storage policy filter storage.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME_3, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Try to retrieve the business object data matching to the storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Collections.singletonList(BDATA_STATUS),
                0, 0, MAX_RESULT);

        // Validate the results.
        assertEquals(0, result.size());
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesNoStoragePolicyLatestVersion()
    {
        // Create and persist an enabled storage policy entity that has no latest version flag set.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, NO_LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit with ENABLED status in the storage policy filter storage.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Try to retrieve the business object data matching to the storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Collections.singletonList(BDATA_STATUS),
                0, 0, MAX_RESULT);

        // Validate the results.
        assertEquals(0, result.size());
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesInvalidStoragePolicyStatus()
    {
        // Create and persist a disabled storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.DISABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit with ENABLED status in the storage policy filter storage.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Try to retrieve the business object data matching to the storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Collections.singletonList(BDATA_STATUS),
                0, 0, MAX_RESULT);

        // Validate the results.
        assertEquals(0, result.size());
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesInvalidBusinessObjectDataStatus()
    {
        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit in the storage policy filter storage, but having "not supported" business object data status.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS_2, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Try to retrieve the business object data matching to the storage policy.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Collections.singletonList(BDATA_STATUS),
                0, 0, MAX_RESULT);

        // Validate the results.
        assertEquals(0, result.size());
    }

    @Test
    public void testBusinessObjectDataEntitiesMatchingStoragePoliciesWithStoragePolicyTransitionMaxAllowedAttempts()
    {
        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Retrieve the business object data matching to the storage policy, when storagePolicyTransitionMaxAllowedAttempts is not specified.
        Map<BusinessObjectDataEntity, StoragePolicyEntity> result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Collections.singletonList(BDATA_STATUS),
                0, 0, MAX_RESULT);

        // Validate the results. A single match should get returned.
        assertEquals(1, result.size());
        assertTrue(result.containsKey(storageUnitEntity.getBusinessObjectData()));

        // Retrieve the business object data matching to the storage policy, when storagePolicyTransitionMaxAllowedAttempts is specified,
        // storage unit has storagePolicyTransitionFailedAttempts set to NULL.
        storageUnitEntity.setStoragePolicyTransitionFailedAttempts(null);
        storageUnitDao.saveAndRefresh(storageUnitEntity);
        result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Collections.singletonList(BDATA_STATUS),
                1, 0, MAX_RESULT);

        // Validate the results. A single match should get returned.
        assertEquals(1, result.size());
        assertTrue(result.containsKey(storageUnitEntity.getBusinessObjectData()));

        // Retrieve the business object data matching to the storage policy, when storagePolicyTransitionMaxAllowedAttempts is specified,
        // storage unit has storagePolicyTransitionFailedAttempts < storagePolicyTransitionMaxAllowedAttempts.
        storageUnitEntity.setStoragePolicyTransitionFailedAttempts(0);
        storageUnitDao.saveAndRefresh(storageUnitEntity);
        result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Collections.singletonList(BDATA_STATUS),
                1, 0, MAX_RESULT);

        // Validate the results. A single match should get returned.
        assertEquals(1, result.size());
        assertTrue(result.containsKey(storageUnitEntity.getBusinessObjectData()));

        // Try to retrieve the business object data matching to the storage policy, when storagePolicyTransitionMaxAllowedAttempts is specified,
        // storage unit has storagePolicyTransitionFailedAttempts == storagePolicyTransitionMaxAllowedAttempts.
        storageUnitEntity.setStoragePolicyTransitionFailedAttempts(1);
        storageUnitDao.saveAndRefresh(storageUnitEntity);
        result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Collections.singletonList(BDATA_STATUS),
                1, 0, MAX_RESULT);

        // Validate the results. No matches should get returned.
        assertEquals(0, result.size());

        // Try to retrieve the business object data matching to the storage policy, when storagePolicyTransitionMaxAllowedAttempts is specified,
        // storage unit has storagePolicyTransitionFailedAttempts > storagePolicyTransitionMaxAllowedAttempts.
        storageUnitEntity.setStoragePolicyTransitionFailedAttempts(2);
        storageUnitDao.saveAndRefresh(storageUnitEntity);
        result = businessObjectDataDao
            .getBusinessObjectDataEntitiesMatchingStoragePolicies(new StoragePolicyPriorityLevel(false, false, false), Collections.singletonList(BDATA_STATUS),
                1, 0, MAX_RESULT);

        // Validate the results. No matches should get returned.
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(FORMAT_VERSION);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(2, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_USAGE_CODE, data.getBusinessObjectFormatUsage());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
        }
    }

    @Test
    public void testBusinessObjectDataSearchWithPageNumAndPageSize()
    {
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION,
                true, "VALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, true, "VALID");

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);

        // Test getting the first page
        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, 1, 1);

        assertTrue(result.size() == 1);

        for (BusinessObjectData data : result)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertEquals(FORMAT_USAGE_CODE, data.getBusinessObjectFormatUsage());
        }

        // Test getting the second page
        result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, 2, 1);

        assertTrue(result.size() == 1);

        for (BusinessObjectData data : result)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertEquals(FORMAT_USAGE_CODE_2, data.getBusinessObjectFormatUsage());
        }

        // Test getting a larger page than there are results remaining
        result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, 1, 3);

        assertTrue(result.size() == 2);

        // Test getting a page that does not exist
        result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, 3, 1);

        assertTrue(result.size() == 0);
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE_2);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME_2);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE_2);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(2, result.size());

        businessObjectDataSearchKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE_2);

        result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        businessObjectDataSearchKey.setBusinessObjectFormatVersion(FORMAT_VERSION_2);

        result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        businessObjectDataSearchKey.setBusinessObjectFormatFileType(null);

        result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(2, result.size());
    }

    @Test
    public void testBusinessObjectDataSearchMissingOptionalParameters()
    {
        // Create business object data entities required for testing.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.INVALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME_2, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Execute the search query by passing business object data search key parameters, except for filters.
        assertEquals(2, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());

        // Execute the search query without optional parameters.
        assertEquals(5, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());
    }

    @Test
    public void testBusinessObjectDataSearchInvalidParameterValues()
    {
        // Create business object data entities required for testing.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.INVALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Execute the search query by passing business object data search key parameters, except for filters.
        assertEquals(2, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());

        // Execute the search query using non-existing/invalid parameter values.
        assertEquals(0, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(I_DO_NOT_EXIST, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());
        assertEquals(0, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, I_DO_NOT_EXIST, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());
        assertEquals(0, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, I_DO_NOT_EXIST, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());
        assertEquals(0, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());
        assertEquals(0, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();

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

        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);

        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(bDefName);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(usage);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(fileTypeCode);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(formatVersion);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_USAGE_CODE, data.getBusinessObjectFormatUsage());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setPartitionKey(PARTITION_KEY);

        PartitionValueRange partitionValueRange = new PartitionValueRange();
        partitionValueRange.setStartPartitionValue(PARTITION_VALUE);
        partitionValueRange.setEndPartitionValue(PARTITION_VALUE + "1");
        partitionValueFilter.setPartitionValueRange(partitionValueRange);

        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(bDefName);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(usage);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(fileTypeCode);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(formatVersion);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(namespace, data.getNamespace());
            assertEquals(bDefName, data.getBusinessObjectDefinitionName());
            assertEquals(usage, data.getBusinessObjectFormatUsage());
            assertEquals(fileTypeCode, data.getBusinessObjectFormatFileType());
            assertTrue(formatVersion == data.getBusinessObjectFormatVersion());
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setPartitionKey(PARTITION_KEY + "1");

        PartitionValueRange partitionValueRange = new PartitionValueRange();
        partitionValueRange.setStartPartitionValue(businessObjectDataEntity.getPartitionValue2());
        partitionValueRange.setEndPartitionValue(businessObjectDataEntity.getPartitionValue2() + "1");
        partitionValueFilter.setPartitionValueRange(partitionValueRange);

        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(bDefName);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(usage);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(fileTypeCode);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(formatVersion);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(namespace, data.getNamespace());
            assertEquals(bDefName, data.getBusinessObjectDefinitionName());
            assertEquals(usage, data.getBusinessObjectFormatUsage());
            assertEquals(fileTypeCode, data.getBusinessObjectFormatFileType());
            assertTrue(formatVersion == data.getBusinessObjectFormatVersion());
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setPartitionKey(PARTITION_KEY + "6");

        PartitionValueRange partitionValueRange = new PartitionValueRange();
        partitionValueRange.setStartPartitionValue(PARTITION_VALUE);
        partitionValueRange.setEndPartitionValue(PARTITION_VALUE + "1");
        partitionValueFilter.setPartitionValueRange(partitionValueRange);

        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(bDefName);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(usage);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(fileTypeCode);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(formatVerion);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
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
            .createBusinessObjectFormatEntity(businessObjectDefinitionEntity, FORMAT_USAGE_CODE, fileTypeEntity, FORMAT_VERSION, null, null, true,
                PARTITION_KEY, null, NO_ATTRIBUTES, null, null, null, schemaColumns, schemaColumns);
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS, DESCRIPTION, BDATA_STATUS_PRE_REGISTRATION_FLAG_SET);

        return businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(businessObjectFormatEntity, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true,
                businessObjectDataStatusEntity.getCode());
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();

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

        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);

        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(bDefName);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(usage);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(fileTypeCode);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(formatVersion);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_USAGE_CODE, data.getBusinessObjectFormatUsage());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
        }
    }

    @Test
    public void testBusinessObjectDataSearchWithRegistrationDateRangeFilter() throws DatatypeConfigurationException
    {
        BusinessObjectDataEntity businessObjectDataEntity = createBusinessObjectEntityForPartitionValueFilterTest();
        String namespace = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getNamespace().getCode();
        String bDefName = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition().getName();
        String usage = businessObjectDataEntity.getBusinessObjectFormat().getUsage();
        String fileTypeCode = businessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode();
        int formatVersion = businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion();

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();

        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(bDefName);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(usage);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(fileTypeCode);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(formatVersion);

        XMLGregorianCalendar start;
        XMLGregorianCalendar end;

        // Start date is less then createdOn and end date is greater then createdOn.
        start = HerdDateUtils.getXMLGregorianCalendarValue(HerdDateUtils.addDays(businessObjectDataEntity.getCreatedOn(), -1));
        end = HerdDateUtils.getXMLGregorianCalendarValue(HerdDateUtils.addDays(businessObjectDataEntity.getCreatedOn(), 1));
        businessObjectDataSearchKey.setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(namespace, data.getNamespace());
            assertEquals(bDefName, data.getBusinessObjectDefinitionName());
            assertEquals(usage, data.getBusinessObjectFormatUsage());
            assertEquals(fileTypeCode, data.getBusinessObjectFormatFileType());
            assertTrue(formatVersion == data.getBusinessObjectFormatVersion());
        }

        // Start date is less then createdOn and end date is null.
        start = HerdDateUtils.getXMLGregorianCalendarValue(HerdDateUtils.addDays(businessObjectDataEntity.getCreatedOn(), -1));
        businessObjectDataSearchKey.setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, null));
        result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(namespace, data.getNamespace());
            assertEquals(bDefName, data.getBusinessObjectDefinitionName());
            assertEquals(usage, data.getBusinessObjectFormatUsage());
            assertEquals(fileTypeCode, data.getBusinessObjectFormatFileType());
            assertTrue(formatVersion == data.getBusinessObjectFormatVersion());
        }

        // Start date is null and end date is greater then createdOn.
        end = HerdDateUtils.getXMLGregorianCalendarValue(HerdDateUtils.addDays(businessObjectDataEntity.getCreatedOn(), 1));
        businessObjectDataSearchKey.setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(null, end));
        result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(namespace, data.getNamespace());
            assertEquals(bDefName, data.getBusinessObjectDefinitionName());
            assertEquals(usage, data.getBusinessObjectFormatUsage());
            assertEquals(fileTypeCode, data.getBusinessObjectFormatFileType());
            assertTrue(formatVersion == data.getBusinessObjectFormatVersion());
        }

        // Start date is greater then createdOn and end date is null.
        start = HerdDateUtils.getXMLGregorianCalendarValue(HerdDateUtils.addDays(businessObjectDataEntity.getCreatedOn(), 1));
        businessObjectDataSearchKey.setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, null));
        result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(0, result.size());

        // Start date is null and end date is less then createdOn.
        end = HerdDateUtils.getXMLGregorianCalendarValue(HerdDateUtils.addDays(businessObjectDataEntity.getCreatedOn(), -1));
        businessObjectDataSearchKey.setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(null, end));
        result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(0, result.size());

        // Start date is equal to createdOn and end date is equal to createdOn.
        start = HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDataEntity.getCreatedOn());
        end = HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDataEntity.getCreatedOn());
        businessObjectDataSearchKey.setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(namespace, data.getNamespace());
            assertEquals(bDefName, data.getBusinessObjectDefinitionName());
            assertEquals(usage, data.getBusinessObjectFormatUsage());
            assertEquals(fileTypeCode, data.getBusinessObjectFormatFileType());
            assertTrue(formatVersion == data.getBusinessObjectFormatVersion());
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
        businessObjectDataSearchKey.setAttributeValueFilters(attributeValueFilters);
        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(bDefName);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(usage);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(fileTypeCode);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(formatVersion);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_USAGE_CODE, data.getBusinessObjectFormatUsage());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
            assertEquals(ATTRIBUTE_NAME_1_MIXED_CASE, data.getAttributes().get(0).getName());
            assertEquals(ATTRIBUTE_VALUE_1, data.getAttributes().get(0).getValue());
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(ATTRIBUTE_NAME_1_MIXED_CASE, null));
        businessObjectDataSearchKey.setAttributeValueFilters(attributeValueFilters);
        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(bDefName);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(usage);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(fileTypeCode);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(formatVersion);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_USAGE_CODE, data.getBusinessObjectFormatUsage());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
            assertEquals(ATTRIBUTE_NAME_1_MIXED_CASE, data.getAttributes().get(0).getName());
            assertEquals(ATTRIBUTE_VALUE_1, data.getAttributes().get(0).getValue());
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(null, ATTRIBUTE_VALUE_1));
        businessObjectDataSearchKey.setAttributeValueFilters(attributeValueFilters);
        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(bDefName);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(usage);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(fileTypeCode);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(formatVersion);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());
        Set<String> expectedAttributeNames = new HashSet<>(Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_NAME_2_MIXED_CASE));
        for (BusinessObjectData data : result)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_USAGE_CODE, data.getBusinessObjectFormatUsage());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
            assertEquals(data.getAttributes().size(), 2);
            assertTrue(expectedAttributeNames.contains((data.getAttributes().get(0).getName())));
            assertTrue(expectedAttributeNames.contains((data.getAttributes().get(1).getName())));
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(bDefName);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(usage);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(fileTypeCode);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(formatVersion);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_USAGE_CODE, data.getBusinessObjectFormatUsage());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
            assertNull(data.getAttributes());
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
        attributeValueFilters.add(new AttributeValueFilter(ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_1));
        businessObjectDataSearchKey.setAttributeValueFilters(attributeValueFilters);
        businessObjectDataSearchKey.setNamespace(namespace);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(bDefName);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(usage);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(fileTypeCode);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(formatVersion);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(1, result.size());

        for (BusinessObjectData data : result)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_USAGE_CODE, data.getBusinessObjectFormatUsage());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertTrue(FORMAT_VERSION == data.getBusinessObjectFormatVersion());
            assertEquals(data.getAttributes().size(), 2);
            assertEquals(ATTRIBUTE_NAME_1_MIXED_CASE, data.getAttributes().get(0).getName());
            assertEquals(ATTRIBUTE_NAME_2_MIXED_CASE, data.getAttributes().get(1).getName());
            assertEquals(ATTRIBUTE_VALUE_1, data.getAttributes().get(0).getValue());
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

            if (version.equals(THIRD_DATA_VERSION))
            {
                expectedBData1 =
                    new BusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                        AbstractDaoTest.PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, version, businessObjectDataEntity.getLatestVersion(), BData_Status,
                        null, null, null, null, null, null);
            }
        }

        BusinessObjectData expectedBData2 = null;
        for (Integer version : Arrays.asList(INITIAL_DATA_VERSION, SECOND_DATA_VERSION, THIRD_DATA_VERSION))
        {
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, PARTITION_VALUE,
                    SUBPARTITION_VALUES, version, version.equals(SECOND_DATA_VERSION), BData_Status);

            if (version.equals(THIRD_DATA_VERSION))
            {
                expectedBData2 =
                    new BusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2,
                        AbstractDaoTest.PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, version, businessObjectDataEntity.getLatestVersion(), BData_Status,
                        null, null, null, null, null, null);
            }
        }

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataSearchKey.setFilterOnLatestValidVersion(true);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
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

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        businessObjectDataSearchKey.setFilterOnLatestValidVersion(true);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
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
            if (version.equals(THIRD_DATA_VERSION))
            {
                BData_Status = "INVALID";
            }
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, version, version.equals(SECOND_DATA_VERSION), BData_Status);
            if (version.equals(SECOND_DATA_VERSION))
            {
                expectedBData =
                    new BusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                        AbstractDaoTest.PARTITION_KEY, PARTITION_VALUE, SUBPARTITION_VALUES, version, businessObjectDataEntity.getLatestVersion(), BData_Status,
                        null, null, null, null, null, null);
            }

        }

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(FORMAT_VERSION);
        businessObjectDataSearchKey.setFilterOnLatestValidVersion(true);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        List<BusinessObjectData> expectedResult = new ArrayList<>(Collections.singletonList(expectedBData));
        assertEquals(1, result.size());
        assertEquals(result, expectedResult);
    }

    @Test
    public void testGetBusinessObjectDataByBusinessObjectDefinition()
    {
        // Create two business object definition entities.
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities = Arrays
            .asList(businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION),
                businessObjectDefinitionDaoTestHelper
                    .createBusinessObjectDefinitionEntity(BDEF_NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2));

        // Create two business object data entities that belong to the first business object definition, but to two different business object formats.
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
            businessObjectDataDao.getBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionEntities.get(0), NO_MAX_RESULTS));

        // Retrieve the keys for business object data by specifying business object definition and with maximum number of results set to 1.
        assertEquals(Collections.singletonList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION)), businessObjectDataDao.getBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionEntities.get(0), MAX_RESULTS_1));

        // Try to retrieve business object data keys for the second business object definition.
        assertEquals(new ArrayList<>(),
            businessObjectDataDao.getBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionEntities.get(1), NO_MAX_RESULTS));
    }

    @Test
    public void testGetBusinessObjectDataByBusinessObjectFormat()
    {
        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY, PARTITION_KEY_GROUP);

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
        assertEquals(Collections.singletonList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION)),
            businessObjectDataDao.getBusinessObjectDataByBusinessObjectFormat(businessObjectFormatEntity, MAX_RESULTS_1));
    }

    @Test
    public void testBusinessObjectDataSearchWithRetentionExpirationFilterNoRetentionInformationConfigured()
    {
        // Create business object data entities required for testing.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.INVALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Execute the search query by passing business object data search key parameters, except for filters.
        assertEquals(2, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());

        // Execute the same search query, but with retention expiration filter.
        // We expect no results, since no retention information is configured for the business object format.
        assertEquals(0, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());
    }

    @Test
    public void testBusinessObjectDataSearchWithRetentionExpirationFilterPartitionValueRetentionType()
    {
        // Create a partition value offset by test retention period in days from today.
        java.util.Date retentionThresholdDate = DateUtils.addDays(new java.util.Date(), -1 * RETENTION_PERIOD_DAYS);
        String partitionValue = DateFormatUtils.format(retentionThresholdDate, AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK);

        // Create business object data entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, partitionValue, SUBPARTITION_VALUES,
                INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.INVALID);
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, partitionValue, SUBPARTITION_VALUES,
                SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Execute the search query by passing business object data search key parameters, except for filters.
        assertEquals(2, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());

        // Get retention type entity for PARTITION_VALUE.
        RetentionTypeEntity retentionTypeEntity = retentionTypeDao.getRetentionTypeByCode(RetentionTypeEntity.PARTITION_VALUE);
        assertNotNull(retentionTypeEntity);

        // Configure retention information for the business object format, so the test business object date is not expired per retention configuration.
        businessObjectDataEntity.getBusinessObjectFormat().setRetentionType(retentionTypeEntity);
        businessObjectDataEntity.getBusinessObjectFormat().setRetentionPeriodInDays(RETENTION_PERIOD_DAYS + 1);

        // Validate the results by executing search request with retention expiration filter enabled.
        // Business object data is expected not to be selected, since it is not expired yet per retention configuration.
        assertEquals(0, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());

        // Configure retention information for the business object format, so the test
        // business object data partition value is the same as retention expiration date.
        businessObjectDataEntity.getBusinessObjectFormat().setRetentionPeriodInDays(RETENTION_PERIOD_DAYS);

        // Validate the results by executing search request with retention expiration filter enabled.
        // Business object data is expected not to be selected, since it is not expired yet per retention configuration.
        assertEquals(0, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());

        // Configure retention information for the business object format, so the test business object date is expired per retention configuration.
        businessObjectDataEntity.getBusinessObjectFormat().setRetentionPeriodInDays(RETENTION_PERIOD_DAYS - 1);

        // Validate the results by executing search request with retention expiration filter enabled.
        // Business object data is still expected to get selected as it is expired per retention configuration.
        assertEquals(2, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());
    }

    @Test
    public void testBusinessObjectDataSearchWithRetentionExpirationFilterBusinessObjectDataRetentionDateRetentionType()
    {
        // Create business object data entities required for testing.
        List<BusinessObjectDataEntity> businessObjectDataEntities = Arrays.asList(businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.INVALID), businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID));

        // Execute the search query by passing business object data search key parameters, except for filters.
        assertEquals(2, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());

        // Get retention type entity for BDATA_RETENTION_DATE.
        RetentionTypeEntity retentionTypeEntity = retentionTypeDao.getRetentionTypeByCode(RetentionTypeEntity.BDATA_RETENTION_DATE);
        assertNotNull(retentionTypeEntity);

        // Configure retention information for the business object format.
        businessObjectDataEntities.get(0).getBusinessObjectFormat().setRetentionType(retentionTypeEntity);

        // Validate the results by executing search request with retention expiration filter enabled.
        // Business object data is expected not to be selected, since it has no explicit retention date configured.
        assertEquals(0, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());

        // Create retention expiration date values required for testing.
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        Timestamp oneDayAhead = HerdDateUtils.addDays(currentTime, 1);
        Timestamp oneDayAgo = HerdDateUtils.addDays(currentTime, -1);

        // Update business object data to have retention expiration date in the future.
        businessObjectDataEntities.get(0).setRetentionExpiration(oneDayAhead);
        businessObjectDataEntities.get(1).setRetentionExpiration(oneDayAhead);

        // Validate the results by executing search request with retention expiration filter enabled.
        // Business object data is expected not to be selected, since its retention expiration date is in the future.
        assertEquals(0, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());

        // Update business object data to have retention expiration date in the past.
        businessObjectDataEntities.get(0).setRetentionExpiration(oneDayAgo);
        businessObjectDataEntities.get(1).setRetentionExpiration(oneDayAgo);

        // Validate the results by executing search request with retention expiration filter enabled.
        // Business object data is expected to be selected, since its retention expiration date is in the past.
        assertEquals(2, businessObjectDataDao.searchBusinessObjectData(
            new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, FILTER_ON_RETENTION_EXPIRATION),
            DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE).size());
    }

    @Test
    public void testBusinessObjectDataSearchWithRetentionExpirationFilterMultipleFormats()
    {
        int retentionPeriod = 180;
        java.util.Date date = DateUtils.addDays(new java.util.Date(), -1 * retentionPeriod);
        String partitionValueDate = DateFormatUtils.format(date, AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK);

        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, partitionValueDate, null,
                DATA_VERSION, true, "VALID");

        BusinessObjectDataEntity businessObjectDataEntity2 = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, partitionValueDate, null,
                DATA_VERSION, true, "INVALID");

        BusinessObjectDataEntity businessObjectDataEntity3 = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_3, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, partitionValueDate, null,
                DATA_VERSION, true, "INVALID");

        BusinessObjectDataEntity businessObjectDataEntity4 = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_3, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION, partitionValueDate, null,
                DATA_VERSION, true, "INVALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, partitionValueDate, null,
                DATA_VERSION, true, "INVALID");

        RetentionTypeEntity existingRetentionType = retentionTypeDao.getRetentionTypeByCode(RetentionTypeEntity.PARTITION_VALUE);
        if (existingRetentionType == null)
        {
            retentionTypeDaoTestHelper.createRetentionTypeEntity(RetentionTypeEntity.PARTITION_VALUE);
        }
        businessObjectDataEntity.getBusinessObjectFormat().setRetentionType(existingRetentionType);
        businessObjectDataEntity.getBusinessObjectFormat().setRetentionPeriodInDays(retentionPeriod - 1);

        businessObjectDataEntity2.getBusinessObjectFormat().setRetentionType(existingRetentionType);
        businessObjectDataEntity2.getBusinessObjectFormat().setRetentionPeriodInDays(retentionPeriod - 1);

        businessObjectDataEntity3.getBusinessObjectFormat().setRetentionType(existingRetentionType);
        businessObjectDataEntity3.getBusinessObjectFormat().setRetentionPeriodInDays(null);

        RetentionTypeEntity notFoundRetentionTypeEntity =
            retentionTypeDaoTestHelper.createRetentionTypeEntity(RetentionTypeEntity.PARTITION_VALUE + "NOTFOUND");
        businessObjectDataEntity4.getBusinessObjectFormat().setRetentionType(notFoundRetentionTypeEntity);
        businessObjectDataEntity4.getBusinessObjectFormat().setRetentionPeriodInDays(retentionPeriod);

        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKey.setFilterOnRetentionExpiration(true);

        List<BusinessObjectData> result = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE);
        assertEquals(2, result.size());
    }

    @Test
    public void testGetBusinessObjectDataMaxMinPartitionValueWhenBusinessObjectFormatKeyDoesNotExist()
    {
        // Create database entities required for testing.
        businessObjectDataAvailabilityTestHelper.createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(null, new ArrayList<>(), new ArrayList<>(),
            BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, SUBPARTITION_VALUES, ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);

        // Create a business object format key that does not exist
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey("NAME_SPACE_VALUE_THAT_DOES_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Get the maximum available partition value - should be null.
        assertNull(businessObjectDataDao
            .getBusinessObjectDataMaxPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, DATA_VERSION,
                BusinessObjectDataStatusEntity.VALID, Collections.singletonList(STORAGE_NAME), null, null, null, null));

        // Get the minimum available partition value - should be null.
        assertNull(businessObjectDataDao
            .getBusinessObjectDataMinPartitionValue(BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION, businessObjectFormatKey, DATA_VERSION,
                BusinessObjectDataStatusEntity.VALID, Collections.singletonList(STORAGE_NAME), null, null));
    }
}
