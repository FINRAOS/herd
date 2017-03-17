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
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.dao.impl.MockS3OperationsImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StorageUnitAlternateKeyDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.impl.BusinessObjectDataFinalizeRestoreHelperServiceImpl;

/**
 * This class tests functionality within the business object data finalize restore helper service.
 */
public class BusinessObjectDataFinalizeRestoreHelperServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "businessObjectDataFinalizeRestoreHelperServiceImpl")
    private BusinessObjectDataFinalizeRestoreHelperServiceImpl businessObjectDataFinalizeRestoreHelperServiceImpl;

    @Test
    public void testPrepareToFinalizeRestore() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey);

        // Get the origin storage unit entity.
        StorageUnitEntity originStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_ORIGIN, businessObjectDataEntity);

        // Prepare to finalize a restore for business object data.
        BusinessObjectDataRestoreDto resultStoragePolicyTransitionParamsDto =
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX,
                storageFileHelper.createStorageFilesFromEntities(originStorageUnitEntity.getStorageFiles()), STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER,
                S3_BUCKET_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, null, resultStoragePolicyTransitionParamsDto);
    }

    @Test
    public void testPrepareToFinalizeRestoreMissingOptionalParameters() throws Exception
    {
        // Create a business object data key without sub-partition values.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey);

        // Get the origin storage unit entity.
        StorageUnitEntity originStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_ORIGIN, businessObjectDataEntity);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Prepare to finalize a restore for business object data without sub-partition values.
        BusinessObjectDataRestoreDto resultStoragePolicyTransitionParamsDto =
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX,
                storageFileHelper.createStorageFilesFromEntities(originStorageUnitEntity.getStorageFiles()), STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER,
                S3_BUCKET_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, null, resultStoragePolicyTransitionParamsDto);
    }

    @Test
    public void testPrepareToFinalizeRestoreInvalidParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Validate that this business object data exists.
        assertNotNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));

        // Try to prepare to finalize a restore using an invalid namespace.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME_ORIGIN));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid business object definition name.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME_ORIGIN));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid format usage.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME_ORIGIN));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid format file type.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME_ORIGIN));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid business object format version.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME_ORIGIN));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid primary partition value.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST",
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME_ORIGIN));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    "I_DO_NOT_EXIST", SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid sub-partition value.
        for (int i = 0; i < SUBPARTITION_VALUES.size(); i++)
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.set(i, "I_DO_NOT_EXIST");
            try
            {
                businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                    new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        testSubPartitionValues, DATA_VERSION, STORAGE_NAME_ORIGIN));
                fail("Should throw an ObjectNotFoundException when not able to find business object data.");
            }
            catch (ObjectNotFoundException e)
            {
                assertEquals(businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                        PARTITION_VALUE, testSubPartitionValues, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
            }
        }

        // Try to prepare to finalize a restore using an invalid business object data version.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, INVALID_DATA_VERSION, STORAGE_NAME_ORIGIN));
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, INVALID_DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid Glacier storage name.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, "I_DO_NOT_EXIST"));
            fail("Should throw an ObjectNotFoundException when not able to find Glacier storage unit.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", "I_DO_NOT_EXIST",
                businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataKeyAsString(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                        PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToFinalizeRestoreBusinessObjectDataNoExists()
    {
        // Try to prepare to finalize a restore for a non-existing business object data passing subpartition values.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME_ORIGIN));
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
    public void testPrepareToFinalizeRestoreGlacierStorageUnitNoExists() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity without a Glacier storage unit.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Try to prepare to finalize a restore when Glacier storage unit does not exist.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);
            fail("Should throw an ObjectNotFoundException when Glacier storage unit does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", STORAGE_NAME_GLACIER,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToFinalizeRestoreGlacierStorageUnitNotEnabled() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a non-enabled Glacier storage unit along with other database entities required for testing.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME_GLACIER, StoragePlatformEntity.GLACIER, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS,
                STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Try to prepare to finalize a restore when Glacier storage unit is not enabled.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);
            fail("Should throw an IllegalArgumentException when Glacier storage unit is not enabled.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data is not archived. Business object data: {%s}",
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToFinalizeRestoreGlacierStorageUnitNoStoragePath() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a non-enabled Glacier storage unit along with other database entities required for testing.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME_GLACIER, StoragePlatformEntity.GLACIER, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Try to prepare to finalize a restore when Glacier storage unit is not enabled.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);
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
    public void testPrepareToFinalizeRestoreGlacierStorageUnitStoragePathDoesNotStartWithOriginBucketName() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with Glacier storage unit storage directory path not starting with the origin S3 bucket name.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
            StorageUnitStatusEntity.RESTORING, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED, TEST_S3_KEY_PREFIX);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Try to prepare to finalize a restore
        // when Glacier storage unit storage directory path does not start with the origin S3 bucket name.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);
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
    public void testPrepareToFinalizeRestoreOriginStorageUnitNoExists() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.DISABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Get the Glacier storage unit entity.
        StorageUnitEntity glacierStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_GLACIER, businessObjectDataEntity);

        // Remove a reference to the origin storage unit from the glacier storage unit.
        glacierStorageUnitEntity.setParentStorageUnit(null);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Try to prepare to finalize a restore when glacier storage unit has no origin origin unit reference.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);
            fail("Should throw an IllegalArgumentException when glacier storage unit has no origin origin unit reference.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Glacier storage unit in \"%s\" storage has no origin S3 storage unit. Business object data: {%s}", STORAGE_NAME_GLACIER,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToFinalizeRestoreOriginStorageUnitNotS3() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.DISABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Create a non-S3 storage unit for this business object data.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, TEST_S3_KEY_PREFIX);

        // Get the Glacier storage unit entity.
        StorageUnitEntity glacierStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_GLACIER, businessObjectDataEntity);

        // Set the origin storage unit to be a non-S3 storage unit.
        glacierStorageUnitEntity.setParentStorageUnit(storageUnitEntity);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Try to prepare to finalize a restore when origin storage unit is not S3.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);
            fail("Should throw an IllegalArgumentException when origin storage unit is not S3.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Glacier storage unit in \"%s\" storage has no origin S3 storage unit. Business object data: {%s}", STORAGE_NAME_GLACIER,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToFinalizeRestoreOriginStorageUnitAlreadyEnabled() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with an enabled origin storage unit.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, StorageUnitStatusEntity.ENABLED,
                STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Try to prepare to finalize a restore when origin storage unit is already enabled.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);
            fail("Should throw an IllegalArgumentException when origin storage unit is enabled.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data is already available in \"%s\" S3 storage. Business object data: {%s}", STORAGE_NAME_ORIGIN,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToFinalizeRestoreOriginStorageUnitNotRestoring() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with a non-RESTORING origin storage unit.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, STORAGE_UNIT_STATUS,
                STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Try to prepare to finalize a restore when origin storage unit is not in RESTORING state.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);
            fail("Should throw an IllegalArgumentException when origin storage unit is not in RESTORING state.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Origin S3 storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. Business object data: {%s}",
                    STORAGE_NAME_ORIGIN, StorageUnitStatusEntity.RESTORING, STORAGE_UNIT_STATUS,
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToFinalizeRestoreOriginStorageUnitNoStorageFiles() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.RESTORING, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Get the origin storage unit entity.
        StorageUnitEntity originStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_ORIGIN, businessObjectDataEntity);

        // Remove storage files from the origin storage unit.
        originStorageUnitEntity.getStorageFiles().clear();

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Try to prepare to finalize a restore when origin storage unit has no storage files.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);
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
    public void testPrepareToFinalizeRestoreNoGlacierStorageBucketName() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with Glacier storage not having S3 bucket name configured.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
            StorageUnitStatusEntity.DISABLED, STORAGE_NAME_GLACIER, null, StorageUnitStatusEntity.ENABLED, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Try to prepare to finalize a restore
        // when Glacier storage does not have an S3 bucket name configured.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);
            fail("Should throw an IllegalStateException when when Glacier storage does not have an S3 bucket name configured");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), STORAGE_NAME_GLACIER), e.getMessage());
        }
    }

    @Test
    public void testPrepareToFinalizeRestoreNoOriginStorageBucketName() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with the origin storage not having S3 bucket name configured.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey, STORAGE_NAME_ORIGIN, null, StorageUnitStatusEntity.RESTORING,
                STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ENABLED, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        // Try to prepare to finalize a restore
        // when the origin storage does not have an S3 bucket name configured.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);
            fail("Should throw an IllegalStateException when when the origin storage does not have an S3 bucket name configured");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), STORAGE_NAME_ORIGIN), e.getMessage());
        }
    }

    @Test
    public void testExecuteS3SpecificSteps() throws Exception
    {
        // Create S3FileTransferRequestParamsDto to access the origin and Glacier S3 bucket locations.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto originS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_ORIGIN).s3KeyPrefix(TEST_S3_KEY_PREFIX + "/").build();
        S3FileTransferRequestParamsDto glacierS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_GLACIER).s3KeyPrefix(S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX + "/")
                .build();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a list of origin storage files.
        List<StorageFile> originStorageFiles = new ArrayList<>();
        for (String filePath : LOCAL_FILES)
        {
            originStorageFiles.add(new StorageFile(TEST_S3_KEY_PREFIX + "/" + filePath, FILE_SIZE_1_KB, NO_ROW_COUNT));
        }

        // Create a business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, originStorageFiles, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, null);

        try
        {
            // Put relative Standard storage class S3 files in the Glacier S3 bucket.
            for (StorageFile originStorageFile : originStorageFiles)
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(
                    new PutObjectRequest(S3_BUCKET_NAME_GLACIER, String.format("%s/%s", S3_BUCKET_NAME_ORIGIN, originStorageFile.getFilePath()),
                        new ByteArrayInputStream(new byte[originStorageFile.getFileSizeBytes().intValue()]), metadata), null);
            }

            // Execute S3 specific steps to finalize a restore for the Glacier storage unit.
            businessObjectDataFinalizeRestoreHelperService.executeS3SpecificSteps(businessObjectDataRestoreDto);

            // Validate that we have the restored origin S3 files at the expected origin S3 location.
            assertEquals(originStorageFiles.size(), s3Dao.listDirectory(originS3FileTransferRequestParamsDto).size());

            // Validate that we have the Glacier S3 files at the expected Glacier S3 location.
            assertEquals(originStorageFiles.size(), s3Dao.listDirectory(glacierS3FileTransferRequestParamsDto).size());
        }
        finally
        {
            // Delete test files from S3 storage.
            for (S3FileTransferRequestParamsDto params : Arrays.asList(originS3FileTransferRequestParamsDto, glacierS3FileTransferRequestParamsDto))
            {
                if (!s3Dao.listDirectory(params).isEmpty())
                {
                    s3Dao.deleteDirectory(params);
                }
            }

            s3Operations.rollback();
        }
    }

    @Test
    public void testExecuteS3SpecificStepsNoLogging() throws Exception
    {
        // Create S3FileTransferRequestParamsDto to access the origin and Glacier S3 bucket locations.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto originS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_ORIGIN).s3KeyPrefix(TEST_S3_KEY_PREFIX + "/").build();
        S3FileTransferRequestParamsDto glacierS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_GLACIER).s3KeyPrefix(S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX + "/")
                .build();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a list of origin storage files.
        List<StorageFile> originStorageFiles = new ArrayList<>();
        for (String filePath : LOCAL_FILES)
        {
            originStorageFiles.add(new StorageFile(TEST_S3_KEY_PREFIX + "/" + filePath, FILE_SIZE_1_KB, NO_ROW_COUNT));
        }

        // Create a business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, originStorageFiles, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, null);

        try
        {
            // Put relative Standard storage class S3 files in the Glacier S3 bucket.
            for (StorageFile originStorageFile : originStorageFiles)
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(
                    new PutObjectRequest(S3_BUCKET_NAME_GLACIER, String.format("%s/%s", S3_BUCKET_NAME_ORIGIN, originStorageFile.getFilePath()),
                        new ByteArrayInputStream(new byte[originStorageFile.getFileSizeBytes().intValue()]), metadata), null);
            }

            // Execute S3 specific steps without logging.
            executeWithoutLogging(BusinessObjectDataFinalizeRestoreHelperServiceImpl.class, () -> {
                businessObjectDataFinalizeRestoreHelperService.executeS3SpecificSteps(businessObjectDataRestoreDto);
            });

            // Validate that we have the restored origin S3 files at the expected origin S3 location.
            assertEquals(originStorageFiles.size(), s3Dao.listDirectory(originS3FileTransferRequestParamsDto).size());

            // Validate that we have the Glacier S3 files at the expected Glacier S3 location.
            assertEquals(originStorageFiles.size(), s3Dao.listDirectory(glacierS3FileTransferRequestParamsDto).size());
        }
        finally
        {
            // Delete test files from S3 storage.
            for (S3FileTransferRequestParamsDto params : Arrays.asList(originS3FileTransferRequestParamsDto, glacierS3FileTransferRequestParamsDto))
            {
                if (!s3Dao.listDirectory(params).isEmpty())
                {
                    s3Dao.deleteDirectory(params);
                }
            }

            s3Operations.rollback();
        }
    }

    @Test
    public void testExecuteS3SpecificStepsGlacierS3FileStillRestoring() throws Exception
    {
        // Create S3FileTransferRequestParamsDto to access the origin and Glacier S3 bucket locations.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto originS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_ORIGIN).s3KeyPrefix(TEST_S3_KEY_PREFIX + "/").build();
        S3FileTransferRequestParamsDto glacierS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_GLACIER).s3KeyPrefix(S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX + "/")
                .build();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, Arrays.asList(new StorageFile(TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE, FILE_SIZE_1_KB, NO_ROW_COUNT)),
                STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, null);

        try
        {
            // Put a "still restoring" Glacier storage class S3 file in the Glacier S3 bucket.
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
            metadata.setOngoingRestore(true);
            s3Operations.putObject(
                new PutObjectRequest(S3_BUCKET_NAME_GLACIER, String.format("%s/%s/%s", S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX, LOCAL_FILE),
                    new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]), metadata), null);

            // Try to execute S3 specific steps to finalize a restore for the Glacier storage unit when Glacier S3 file is still restoring.
            try
            {
                businessObjectDataFinalizeRestoreHelperService.executeS3SpecificSteps(businessObjectDataRestoreDto);
                fail("Should throw an IllegalStateException when a Glacier S3 file is still restoring.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("Archived Glacier S3 file \"%s/%s/%s\" is not restored. " +
                    "StorageClass {GLACIER}, OngoingRestore flag {true}, Glacier S3 bucket name {%s}", S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX, LOCAL_FILE,
                    S3_BUCKET_NAME_GLACIER), e.getMessage());
            }

            // Validate that the origin S3 location is empty.
            assertTrue(s3Dao.listDirectory(originS3FileTransferRequestParamsDto).isEmpty());

            // Validate that we have a Glacier S3 file at the expected Glacier S3 location.
            assertEquals(1, s3Dao.listDirectory(glacierS3FileTransferRequestParamsDto).size());
        }
        finally
        {
            // Delete test files from S3 storage.
            for (S3FileTransferRequestParamsDto params : Arrays.asList(originS3FileTransferRequestParamsDto, glacierS3FileTransferRequestParamsDto))
            {
                if (!s3Dao.listDirectory(params).isEmpty())
                {
                    s3Dao.deleteDirectory(params);
                }
            }

            s3Operations.rollback();
        }
    }

    @Test
    public void testExecuteS3SpecificStepsOriginPrefixNotEmpty() throws Exception
    {
        // Create S3FileTransferRequestParamsDto to access the origin and Glacier S3 bucket locations.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto originS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_ORIGIN).s3KeyPrefix(TEST_S3_KEY_PREFIX + "/").build();
        S3FileTransferRequestParamsDto glacierS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_GLACIER).s3KeyPrefix(S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX + "/")
                .build();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a list of origin storage files.
        List<StorageFile> originStorageFiles = new ArrayList<>();
        for (String filePath : LOCAL_FILES)
        {
            originStorageFiles.add(new StorageFile(TEST_S3_KEY_PREFIX + "/" + filePath, FILE_SIZE_1_KB, NO_ROW_COUNT));
        }

        // Create a business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, originStorageFiles, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, null);

        try
        {
            // Put a "still restoring" Glacier storage class S3 file in the Glacier S3 bucket.
            for (StorageFile originStorageFile : originStorageFiles)
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(
                    new PutObjectRequest(S3_BUCKET_NAME_GLACIER, String.format("%s/%s", S3_BUCKET_NAME_ORIGIN, originStorageFile.getFilePath()),
                        new ByteArrayInputStream(new byte[originStorageFile.getFileSizeBytes().intValue()]), metadata), null);
            }

            // Put an S3 file under the origin S3 key prefix in the origin S3 bucket.
            s3Operations.putObject(
                new PutObjectRequest(S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]),
                    null), null);

            // Try to execute S3 specific steps to finalize a restore when origin S3 key prefix is not empty.
            try
            {
                businessObjectDataFinalizeRestoreHelperService.executeS3SpecificSteps(businessObjectDataRestoreDto);
                fail("Should throw an IllegalStateException when destination S3 key prefix is not empty.");
            }
            catch (IllegalStateException e)
            {
                assertEquals(String
                    .format("The origin S3 key prefix is not empty. S3 bucket name: {%s}, S3 key prefix: {%s/}", S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX),
                    e.getMessage());
            }

            // Validate that the origin S3 location is still not empty.
            assertEquals(1, s3Dao.listDirectory(originS3FileTransferRequestParamsDto).size());

            // Validate that we have a Glacier S3 file at the expected Glacier S3 location.
            assertEquals(originStorageFiles.size(), s3Dao.listDirectory(glacierS3FileTransferRequestParamsDto).size());
        }
        finally
        {
            // Delete test files from S3 storage.
            for (S3FileTransferRequestParamsDto params : Arrays.asList(originS3FileTransferRequestParamsDto, glacierS3FileTransferRequestParamsDto))
            {
                if (!s3Dao.listDirectory(params).isEmpty())
                {
                    s3Dao.deleteDirectory(params);
                }
            }

            s3Operations.rollback();
        }
    }

    @Test
    public void testExecuteS3SpecificStepsNonGlacierStorageClassAndS3CopyFails() throws Exception
    {
        // Create S3FileTransferRequestParamsDto to access the origin and Glacier S3 bucket locations.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto originS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_ORIGIN).s3KeyPrefix(TEST_S3_KEY_PREFIX + "/").build();
        S3FileTransferRequestParamsDto glacierS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_GLACIER).s3KeyPrefix(S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX + "/")
                .build();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a list of origin storage files.
        List<StorageFile> originStorageFiles = new ArrayList<>();
        for (String filePath : LOCAL_FILES)
        {
            originStorageFiles.add(new StorageFile(TEST_S3_KEY_PREFIX + "/" + filePath, FILE_SIZE_1_KB, NO_ROW_COUNT));
        }

        // Add a mocked S3 file name to the list to trigger an Amazon service exception when we try to perform an S3 copy operation.
        originStorageFiles
            .add(new StorageFile(TEST_S3_KEY_PREFIX + "/" + MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION, FILE_SIZE_1_KB, NO_ROW_COUNT));

        // Create a business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, originStorageFiles, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, null);

        try
        {
            // For unit test code coverage, put relative Standard storage class S3 files in the Glacier S3 bucket.
            for (StorageFile originStorageFile : originStorageFiles)
            {
                ObjectMetadata metadata = new ObjectMetadata();
                // For unit test code coverage, we also represent Standard storage class by a null value for the STORAGE_CLASS header.
                metadata.setHeader(Headers.STORAGE_CLASS, originStorageFile.getFilePath().endsWith(LOCAL_FILES.get(0)) ? null : StorageClass.Standard);
                s3Operations.putObject(
                    new PutObjectRequest(S3_BUCKET_NAME_GLACIER, String.format("%s/%s", S3_BUCKET_NAME_ORIGIN, originStorageFile.getFilePath()),
                        new ByteArrayInputStream(new byte[originStorageFile.getFileSizeBytes().intValue()]), metadata), null);
            }

            // Try to execute S3 specific steps to finalize a restore when S3 copy operation fails with an Amazon service exception.
            try
            {
                businessObjectDataFinalizeRestoreHelperService.executeS3SpecificSteps(businessObjectDataRestoreDto);
                fail("Should throw an IllegalStateException when an S3 copy operation fails.");
            }
            catch (IllegalStateException e)
            {
                assertEquals(String.format("Failed to copy S3 file. Source storage: {%s}, source S3 bucket name: {%s}, source S3 object key: {%s/%s/%s}, " +
                    "target storage: {%s}, target S3 bucket name: {%s}, target S3 object key: {%s/%s}, business object data: {%s}", STORAGE_NAME_GLACIER,
                    S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX, MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION,
                    STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX, MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION,
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
            }

            // Validate that the origin S3 location is empty.
            assertTrue(s3Dao.listDirectory(originS3FileTransferRequestParamsDto).isEmpty());

            // Validate that we have a Glacier S3 file at the expected Glacier S3 location.
            assertEquals(originStorageFiles.size(), s3Dao.listDirectory(glacierS3FileTransferRequestParamsDto).size());
        }
        finally
        {
            // Delete test files from S3 storage.
            for (S3FileTransferRequestParamsDto params : Arrays.asList(originS3FileTransferRequestParamsDto, glacierS3FileTransferRequestParamsDto))
            {
                if (!s3Dao.listDirectory(params).isEmpty())
                {
                    s3Dao.deleteDirectory(params);
                }
            }

            s3Operations.rollback();
        }
    }

    @Test
    public void testEnableOriginStorageUnit() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey);

        // Get the origin storage unit entity.
        StorageUnitEntity originStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_ORIGIN, businessObjectDataEntity);

        // Validate that the origin storage unit status is RESTORING.
        assertEquals(StorageUnitStatusEntity.RESTORING, originStorageUnitEntity.getStatus().getCode());

        // Create a business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, Arrays.asList(new StorageFile(TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE, FILE_SIZE_1_KB, NO_ROW_COUNT)),
                STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, null);

        // Enable origin storage unit to finalize a restore for business object data.
        businessObjectDataFinalizeRestoreHelperService.enableOriginStorageUnit(businessObjectDataRestoreDto);

        // Validate that the origin storage unit status is now ENABLED.
        assertEquals(StorageUnitStatusEntity.ENABLED, originStorageUnitEntity.getStatus().getCode());
    }

    /**
     * This method is to get coverage for the business object data finalize restore helper service methods that have an explicit annotation for transaction
     * propagation.
     */
    @Test
    public void testBusinessObjectDataFinalizeRestoreHelperServiceMethodsNewTransactionPropagation()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        try
        {
            businessObjectDataFinalizeRestoreHelperServiceImpl.prepareToFinalizeRestore(glacierStorageUnitKey);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectDataKey, NO_BDATA_STATUS),
                e.getMessage());
        }

        // Create an origin storage file.
        StorageFile originStorageFile = new StorageFile(TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE, FILE_SIZE_1_KB, NO_ROW_COUNT);

        // Create a business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, Arrays.asList(originStorageFile), STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, null);

        try
        {
            businessObjectDataFinalizeRestoreHelperServiceImpl.executeS3SpecificSteps(businessObjectDataRestoreDto);
            fail("Should throw an ObjectNotFoundException.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Archived file \"%s/%s\" does not exist in \"%s\" storage.", S3_BUCKET_NAME_ORIGIN, originStorageFile.getFilePath(),
                STORAGE_NAME_GLACIER), e.getMessage());
        }

        try
        {
            businessObjectDataFinalizeRestoreHelperServiceImpl.enableOriginStorageUnit(businessObjectDataRestoreDto);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectDataKey, NO_BDATA_STATUS),
                e.getMessage());
        }
    }
}
