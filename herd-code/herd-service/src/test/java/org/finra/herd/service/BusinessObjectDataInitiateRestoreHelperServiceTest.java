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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * This class tests functionality within the business object data initiate restore helper service.
 */
public class BusinessObjectDataInitiateRestoreHelperServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "businessObjectDataInitiateRestoreHelperServiceImpl")
    private BusinessObjectDataInitiateRestoreHelperService businessObjectDataInitiateRestoreHelperServiceImpl;

    @Test
    public void testPrepareToInitiateRestore() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Execute a before step for the initiate a business object data restore request.
        BusinessObjectDataRestoreDto storagePolicyTransitionParamsDto =
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);

        // Validate the returned object.
        assertEquals(businessObjectDataKey, storagePolicyTransitionParamsDto.getBusinessObjectDataKey());
    }

    @Test
    public void testPrepareToInitiateRestoreMissingRequiredParameters()
    {
        // Try to execute a before step for the initiate a business object data restore request without specifying business object definition name.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request without specifying business object format usage.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request without specifying business object format file type.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request without specifying business object format version.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request without specifying primary partition value.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request without specifying 1st subpartition value.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT, SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION));
            fail("Should throw an IllegalArgumentException when 1st subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request without specifying 2nd subpartition value.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), BLANK_TEXT, SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION));
            fail("Should throw an IllegalArgumentException when 2nd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request without specifying 3rd subpartition value.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), BLANK_TEXT, SUBPARTITION_VALUES.get(3)), DATA_VERSION));
            fail("Should throw an IllegalArgumentException when 3rd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request without specifying 4th subpartition value.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), BLANK_TEXT), DATA_VERSION));
            fail("Should throw an IllegalArgumentException when 4th subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request without specifying business object data version.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, NO_DATA_VERSION));
            fail("Should throw an IllegalArgumentException when business object data version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version must be specified.", e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreMissingOptionalParameters() throws Exception
    {
        // Create a business object data key without sub-partition values.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Execute a before step for the initiate a business object data restore request for business object data without sub-partition values.
        BusinessObjectDataRestoreDto storagePolicyTransitionParamsDto =
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);

        // Validate the returned object.
        assertEquals(businessObjectDataKey, storagePolicyTransitionParamsDto.getBusinessObjectDataKey());
    }

    @Test
    public void testPrepareToInitiateRestoreTrimParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Execute a before step for the initiate a business object data restore request using input parameters with leading and trailing empty spaces.
        BusinessObjectDataRestoreDto storagePolicyTransitionParamsDto = businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
            new BusinessObjectDataKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION));

        // Validate the returned object.
        assertEquals(businessObjectDataKey, storagePolicyTransitionParamsDto.getBusinessObjectDataKey());
    }

    @Test
    public void testPrepareToInitiateRestoreUpperCaseParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Execute a before step for the initiate a business object data restore request
        // using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectDataRestoreDto storagePolicyTransitionParamsDto = businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
            new BusinessObjectDataKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION));

        // Validate the returned object.
        assertEquals(businessObjectDataKey, storagePolicyTransitionParamsDto.getBusinessObjectDataKey());
    }

    @Test
    public void testPrepareToInitiateRestoreLowerCaseParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Execute a before step for the initiate a business object data restore request
        // using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectDataRestoreDto storagePolicyTransitionParamsDto = businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
            new BusinessObjectDataKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION));

        // Validate the returned object.
        assertEquals(businessObjectDataKey, storagePolicyTransitionParamsDto.getBusinessObjectDataKey());
    }

    @Test
    public void testPrepareToInitiateRestoreInvalidParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Validate that this business object data exists.
        assertNotNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));

        // Try to execute a before step for the initiate a business object data restore request using an invalid namespace.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid business object definition name.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid format usage.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid format file type.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid business object format version.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid primary partition value.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST",
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    "I_DO_NOT_EXIST", SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid sub-partition value.
        for (int i = 0; i < SUBPARTITION_VALUES.size(); i++)
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.set(i, "I_DO_NOT_EXIST");
            try
            {
                businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                    new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        testSubPartitionValues, DATA_VERSION));
                fail("Should throw an ObjectNotFoundException when not able to find business object data.");
            }
            catch (ObjectNotFoundException e)
            {
                assertEquals(businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                        PARTITION_VALUE, testSubPartitionValues, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
            }
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid business object data version.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, INVALID_DATA_VERSION));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, INVALID_DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreBusinessObjectDataNoExists()
    {
        // Try to execute a before step for the initiate a business object data restore request
        // for a non-existing business object data passing subpartition values.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreGlacierStorageUnitNoExists() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity without a Glacier storage unit.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Try to execute a before step for the initiate a business object data restore request when Glacier storage unit does not exist.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalArgumentException when Glacier storage unit does not exist.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data is not archived. Business object data: {%s}",
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreMultipleGlacierStorageUnitsExist() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create multiple enabled Glacier storage units for this business object data.
        storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.GLACIER, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED,
            STORAGE_DIRECTORY_PATH);
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME_2, StoragePlatformEntity.GLACIER, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED,
                STORAGE_DIRECTORY_PATH);

        // Try to execute a before step for the initiate a business object data restore request
        // when business object data has multiple enabled Glacier storage units.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalArgumentException when business object data has multiple enabled Glacier storage units.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data has multiple (2) GLACIER storage units. Business object data: {%s}",
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreGlacierStorageUnitNotEnabled() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a non-enabled Glacier storage unit along with other database entities required for testing.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.GLACIER, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS,
                STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Try to execute a before step for the initiate a business object data restore request when Glacier storage unit is not enabled.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalArgumentException when Glacier storage unit is not enabled.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data is not archived. Business object data: {%s}",
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreGlacierStorageUnitNoStoragePath() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a non-enabled Glacier storage unit along with other database entities required for testing.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME_GLACIER, StoragePlatformEntity.GLACIER, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Try to execute a before step for the initiate a business object data restore request when Glacier storage unit is not enabled.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalArgumentException when Glacier storage unit is not enabled.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Business object data has no storage directory path specified in \"%s\" %s storage. Business object data: {%s}", STORAGE_NAME_GLACIER,
                    StoragePlatformEntity.GLACIER, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)),
                e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreGlacierStorageUnitStoragePathDoesNotStartWithOriginBucketName() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with Glacier storage unit storage directory path not starting with the origin S3 bucket name.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
            StorageUnitStatusEntity.DISABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED, TEST_S3_KEY_PREFIX);

        // Try to execute a before step for the initiate a business object data restore request
        // when Glacier storage unit storage directory path does not start with the origin S3 bucket name.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalArgumentException when Glacier storage unit storage directory path does not start with the origin S3 bucket name.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                "Storage directory path \"%s\" for business object data in \"%s\" %s storage does not start with the origin S3 bucket name. " +
                    "Origin S3 bucket name: {%s}, origin storage: {%s}, business object data: {%s}", TEST_S3_KEY_PREFIX, STORAGE_NAME_GLACIER,
                StoragePlatformEntity.GLACIER, S3_BUCKET_NAME_ORIGIN, STORAGE_NAME_ORIGIN,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreOriginStorageUnitNoExists() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.DISABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Get the Glacier storage unit entity.
        StorageUnitEntity glacierStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_GLACIER, businessObjectDataEntity);

        // Remove a reference to the origin storage unit from the glacier storage unit.
        glacierStorageUnitEntity.setParentStorageUnit(null);

        // Try to execute a before step for the initiate a business object data restore request when glacier storage unit has no origin origin unit reference.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalArgumentException when glacier storage unit has no origin origin unit reference.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Glacier storage unit in \"%s\" storage has no origin S3 storage unit. Business object data: {%s}", STORAGE_NAME_GLACIER,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreOriginStorageUnitNotS3() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.DISABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Create a non-S3 storage unit for this business object data.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, TEST_S3_KEY_PREFIX);

        // Get the Glacier storage unit entity.
        StorageUnitEntity glacierStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_GLACIER, businessObjectDataEntity);

        // Set the origin storage unit to be a non-S3 storage unit.
        glacierStorageUnitEntity.setParentStorageUnit(storageUnitEntity);

        // Try to execute a before step for the initiate a business object data restore request when origin storage unit is not S3.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalArgumentException when origin storage unit is not S3.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Glacier storage unit in \"%s\" storage has no origin S3 storage unit. Business object data: {%s}", STORAGE_NAME_GLACIER,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreOriginStorageUnitAlreadyEnabled() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with an enabled origin storage unit.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, StorageUnitStatusEntity.ENABLED,
                STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Try to execute a before step for the initiate a business object data restore request when origin storage unit is already enabled.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalArgumentException when origin storage unit is enabled.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data is already available in \"%s\" S3 storage. Business object data: {%s}", STORAGE_NAME_ORIGIN,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreOriginStorageUnitAlreadyRestoring() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with an origin storage unit already being restored.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
            StorageUnitStatusEntity.RESTORING, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED,
            S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Try to execute a before step for the initiate a business object data restore request when origin storage unit already being restored.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalArgumentException when origin storage unit already being restored.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data is already being restored to \"%s\" S3 storage. Business object data: {%s}", STORAGE_NAME_ORIGIN,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreOriginStorageUnitNotDisabled() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with a non-DISABLED origin storage unit.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, STORAGE_UNIT_STATUS,
                STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Try to execute a before step for the initiate a business object data restore request when origin storage unit is not disabled.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalArgumentException when origin storage unit is not disabled.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Origin S3 storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. Business object data: {%s}",
                    STORAGE_NAME_ORIGIN, StorageUnitStatusEntity.DISABLED, STORAGE_UNIT_STATUS,
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreOriginStorageUnitNoStorageFiles() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.DISABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Get the origin storage unit entity.
        StorageUnitEntity originStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_ORIGIN, businessObjectDataEntity);

        // Remove storage files from the origin storage unit.
        originStorageUnitEntity.getStorageFiles().clear();

        // Try to execute a before step for the initiate a business object data restore request when origin storage unit has no storage files.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalArgumentException when origin storage unit has no storage files.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Business object data has no storage files registered in \"%s\" origin storage. Business object data: {%s}", STORAGE_NAME_ORIGIN,
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreNoGlacierStorageBucketName() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with Glacier storage not having S3 bucket name configured.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
            StorageUnitStatusEntity.DISABLED, STORAGE_NAME_GLACIER, null, StorageUnitStatusEntity.ENABLED, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Try to execute a before step for the initiate a business object data restore request
        // when Glacier storage does not have an S3 bucket name configured.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalStateException when when Glacier storage does not have an S3 bucket name configured");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), STORAGE_NAME_GLACIER), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreNoOriginStorageBucketName() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with the origin storage not having S3 bucket name configured.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, null, StorageUnitStatusEntity.DISABLED,
                STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Try to execute a before step for the initiate a business object data restore request
        // when the origin storage does not have an S3 bucket name configured.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey);
            fail("Should throw an IllegalStateException when when the origin storage does not have an S3 bucket name configured");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), STORAGE_NAME_ORIGIN), e.getMessage());
        }
    }

    /**
     * This method is to get coverage for the business object data initiate restore helper service methods that have an explicit annotation for transaction
     * propagation.
     */
    @Test
    public void testBusinessObjectDataInitiateRestoreHelperServiceMethodsNewTransactionPropagation()
    {
        try
        {
            businessObjectDataInitiateRestoreHelperServiceImpl.prepareToInitiateRestore(new BusinessObjectDataKey());
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataInitiateRestoreHelperServiceImpl.executeS3SpecificSteps(null);
            fail("Should throw an NullPointerException.");
        }
        catch (NullPointerException e)
        {
            assertNull(e.getMessage());
        }

        try
        {
            businessObjectDataInitiateRestoreHelperServiceImpl.executeInitiateRestoreAfterStep(null);
            fail("Should throw an NullPointerException.");
        }
        catch (NullPointerException e)
        {
            assertNull(e.getMessage());
        }
    }
}
