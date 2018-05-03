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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.core.HerdDateUtils;
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

    /**
     * This method is to get coverage for the business object data initiate restore helper service methods that have an explicit annotation for transaction
     * propagation.
     */
    @Test
    public void testBusinessObjectDataInitiateRestoreHelperServiceMethodsNewTransactionPropagation()
    {
        try
        {
            businessObjectDataInitiateRestoreHelperServiceImpl.prepareToInitiateRestore(new BusinessObjectDataKey(), EXPIRATION_IN_DAYS);
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

    @Test
    public void testPrepareToInitiateRestore() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Get the current time and compute the expected timestamp value.
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        Timestamp expectedTimestamp = HerdDateUtils.addDays(currentTime, EXPIRATION_IN_DAYS);

        // Execute a before step for the initiate a business object data restore request.
        BusinessObjectDataRestoreDto storagePolicyTransitionParamsDto =
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey, EXPIRATION_IN_DAYS);

        // Validate the returned object.
        assertEquals(businessObjectDataKey, storagePolicyTransitionParamsDto.getBusinessObjectDataKey());

        // Validate that restore expiration time is set correctly on the storage unit.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        assertNotNull(storageUnitEntity.getRestoreExpirationOn());
        Long differenceInMilliseconds = storageUnitEntity.getRestoreExpirationOn().getTime() - expectedTimestamp.getTime();
        assertTrue(Math.abs(differenceInMilliseconds) < 1000);
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
                    SUBPARTITION_VALUES, DATA_VERSION), EXPIRATION_IN_DAYS);
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
                new BusinessObjectDataKey(I_DO_NOT_EXIST, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(I_DO_NOT_EXIST, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid business object definition name.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, I_DO_NOT_EXIST, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, I_DO_NOT_EXIST, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid format usage.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, I_DO_NOT_EXIST, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, I_DO_NOT_EXIST, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid format file type.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid business object format version.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
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
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, I_DO_NOT_EXIST,
                    SUBPARTITION_VALUES, DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    I_DO_NOT_EXIST, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid sub-partition value.
        for (int i = 0; i < SUBPARTITION_VALUES.size(); i++)
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.set(i, I_DO_NOT_EXIST);
            try
            {
                businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                    new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        testSubPartitionValues, DATA_VERSION), EXPIRATION_IN_DAYS);
                fail();
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
                    SUBPARTITION_VALUES, INVALID_DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, INVALID_DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the initiate a business object data restore request using an invalid expiration time value.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), 0);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Expiration in days value must be a positive integer.", e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreLowerCaseParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Execute a before step for the initiate a business object data restore request
        // using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectDataRestoreDto storagePolicyTransitionParamsDto = businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
            new BusinessObjectDataKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION), EXPIRATION_IN_DAYS);

        // Validate the returned object.
        assertEquals(businessObjectDataKey, storagePolicyTransitionParamsDto.getBusinessObjectDataKey());
    }

    @Test
    public void testPrepareToInitiateRestoreMissingOptionalParameters() throws Exception
    {
        // Create a business object data key without sub-partition values.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Execute a before step for the initiate a business object data restore request for business object data without sub-partition values.
        BusinessObjectDataRestoreDto storagePolicyTransitionParamsDto =
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey, NO_EXPIRATION_IN_DAYS);

        // Validate the returned object.
        assertEquals(businessObjectDataKey, storagePolicyTransitionParamsDto.getBusinessObjectDataKey());

        // Validate that restore expiration time is set correctly on the storage unit.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);
        assertNotNull(storageUnitEntity.getRestoreExpirationOn());
        Long differenceInDays =
            TimeUnit.DAYS.convert(storageUnitEntity.getRestoreExpirationOn().getTime() - storageUnitEntity.getCreatedOn().getTime(), TimeUnit.MILLISECONDS);
        assertTrue((Integer) ConfigurationValue.BDATA_RESTORE_EXPIRATION_IN_DAYS_DEFAULT.getDefaultValue() - differenceInDays <= 1);
    }

    @Test
    public void testPrepareToInitiateRestoreMissingRequiredParameters()
    {
        // Try to execute a before step for the initiate a business object data restore request without specifying business object definition name.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
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
                    DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
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
                    DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
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
                    SUBPARTITION_VALUES, DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
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
                    DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
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
                    Arrays.asList(BLANK_TEXT, SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION),
                EXPIRATION_IN_DAYS);
            fail();
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
                    Arrays.asList(SUBPARTITION_VALUES.get(0), BLANK_TEXT, SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION),
                EXPIRATION_IN_DAYS);
            fail();
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
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), BLANK_TEXT, SUBPARTITION_VALUES.get(3)), DATA_VERSION),
                EXPIRATION_IN_DAYS);
            fail();
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
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), BLANK_TEXT), DATA_VERSION),
                EXPIRATION_IN_DAYS);
            fail();
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
                    SUBPARTITION_VALUES, NO_DATA_VERSION), EXPIRATION_IN_DAYS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version must be specified.", e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreMultipleS3StorageUnitsExist() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create multiple archived storage units for this business object data.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.S3, businessObjectDataEntity, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME_2, StoragePlatformEntity.S3, businessObjectDataEntity, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Try to execute a before step for the initiate a business object data restore request
        // when business object data has multiple S3 storage units.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey, EXPIRATION_IN_DAYS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data has multiple (2) S3 storage units. Business object data: {%s}",
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreStorageHasNoS3BucketName() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with the storage not having S3 bucket name configured.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME, NO_S3_BUCKET_NAME, StorageUnitStatusEntity.ARCHIVED,
                LOCAL_FILES);

        // Try to execute a before step for the initiate a business object data restore request
        // when the storage does not have an S3 bucket name configured.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey, EXPIRATION_IN_DAYS);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreStorageUnitAlreadyRestoring() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with a storage unit already being restored.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, StorageUnitStatusEntity.RESTORING,
                LOCAL_FILES);

        // Try to execute a before step for the initiate a business object data restore request when storage unit is already being restored.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey, EXPIRATION_IN_DAYS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data is already being restored in \"%s\" S3 storage. Business object data: {%s}", STORAGE_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreStorageUnitHasNoStorageFiles() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, StorageUnitStatusEntity.ARCHIVED,
                LOCAL_FILES);

        // Get the storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        // Remove storage files from the storage unit.
        storageUnitEntity.getStorageFiles().clear();

        // Try to execute a before step for the initiate a business object data restore request when storage unit has no storage files.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey, EXPIRATION_IN_DAYS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data has no storage files registered in \"%s\" storage. Business object data: {%s}", STORAGE_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreStorageUnitNoExists() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity without a storage unit.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Try to execute a before step for the initiate a business object data restore request when storage unit does not exist.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey, EXPIRATION_IN_DAYS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data has no S3 storage unit. Business object data: {%s}",
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreStorageUnitNotInArchivedState() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit not in ARCHIVED state along with other database entities required for testing.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.S3, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS,
                NO_STORAGE_DIRECTORY_PATH);

        // Try to execute a before step for the initiate a business object data restore request when storage unit is not in ARCHIVED state.
        try
        {
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey, EXPIRATION_IN_DAYS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                "Business object data is not archived. S3 storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. " +
                    "Business object data: {%s}", STORAGE_NAME, StorageUnitStatusEntity.ARCHIVED, STORAGE_UNIT_STATUS,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToInitiateRestoreTrimParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Execute a before step for the initiate a business object data restore request using input parameters with leading and trailing empty spaces.
        BusinessObjectDataRestoreDto storagePolicyTransitionParamsDto = businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
            new BusinessObjectDataKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), NO_SUBPARTITION_VALUES, DATA_VERSION),
            EXPIRATION_IN_DAYS);

        // Validate the returned object.
        assertEquals(businessObjectDataKey, storagePolicyTransitionParamsDto.getBusinessObjectDataKey());
    }

    @Test
    public void testPrepareToInitiateRestoreUpperCaseParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Execute a before step for the initiate a business object data restore request
        // using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectDataRestoreDto storagePolicyTransitionParamsDto = businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(
            new BusinessObjectDataKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION), EXPIRATION_IN_DAYS);

        // Validate the returned object.
        assertEquals(businessObjectDataKey, storagePolicyTransitionParamsDto.getBusinessObjectDataKey());
    }
}
