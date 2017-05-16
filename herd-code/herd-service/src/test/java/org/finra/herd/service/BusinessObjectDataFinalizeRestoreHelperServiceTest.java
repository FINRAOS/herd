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

        // Create a storage unit key.
        StorageUnitAlternateKeyDto storageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME);

        try
        {
            businessObjectDataFinalizeRestoreHelperServiceImpl.prepareToFinalizeRestore(storageUnitKey);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectDataKey, NO_BDATA_STATUS),
                e.getMessage());
        }

        // Create a storage file.
        StorageFile storageFile = new StorageFile(TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE, FILE_SIZE_1_KB, NO_ROW_COUNT);

        // Create a business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME, NO_S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, Arrays.asList(storageFile), NO_EXCEPTION);

        try
        {
            businessObjectDataFinalizeRestoreHelperServiceImpl.executeS3SpecificSteps(businessObjectDataRestoreDto);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Registered file \"%s\" does not exist in \"%s\" storage.", storageFile.getFilePath(), STORAGE_NAME), e.getMessage());
        }

        try
        {
            businessObjectDataFinalizeRestoreHelperServiceImpl.completeFinalizeRestore(businessObjectDataRestoreDto);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectDataKey, NO_BDATA_STATUS),
                e.getMessage());
        }
    }

    @Test
    public void testCompleteFinalizeRestore() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey);

        // Get the origin storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        // Validate that the storage unit status is RESTORING.
        assertEquals(StorageUnitStatusEntity.RESTORING, storageUnitEntity.getStatus().getCode());

        // Create a business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, Arrays.asList(new StorageFile(TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE, FILE_SIZE_1_KB, NO_ROW_COUNT)), NO_EXCEPTION);

        // Enable origin storage unit to finalize a restore for business object data.
        businessObjectDataFinalizeRestoreHelperService.completeFinalizeRestore(businessObjectDataRestoreDto);

        // Validate that the storage unit status is RESTORED.
        assertEquals(StorageUnitStatusEntity.RESTORED, storageUnitEntity.getStatus().getCode());
    }

    @Test
    public void testExecuteS3SpecificSteps() throws Exception
    {
        // Create S3FileTransferRequestParamsDto to access the S3 bucket.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withS3KeyPrefix(TEST_S3_KEY_PREFIX + "/").build();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a list of storage files.
        List<StorageFile> storageFiles = new ArrayList<>();
        for (String filePath : LOCAL_FILES)
        {
            storageFiles.add(new StorageFile(TEST_S3_KEY_PREFIX + "/" + filePath, FILE_SIZE_1_KB, NO_ROW_COUNT));
        }

        // Create a business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME, NO_S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, storageFiles, NO_EXCEPTION);

        try
        {
            // Put relative Glacier storage class S3 files in the S3 bucket.
            for (StorageFile storageFile : storageFiles)
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, storageFile.getFilePath(),
                    new ByteArrayInputStream(new byte[storageFile.getFileSizeBytes().intValue()]), metadata), NO_S3_CLIENT);
            }

            // Execute S3 specific steps to finalize a restore for the Glacier storage unit.
            businessObjectDataFinalizeRestoreHelperService.executeS3SpecificSteps(businessObjectDataRestoreDto);

            // Validate that we have the restored S3 files at the expected S3 location.
            assertEquals(storageFiles.size(), s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());
        }
        finally
        {
            // Delete test files from S3 storage.
            if (!s3Dao.listDirectory(s3FileTransferRequestParamsDto).isEmpty())
            {
                s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
            }

            s3Operations.rollback();
        }
    }

    @Test
    public void testExecuteS3SpecificStepsGlacierS3FileStillRestoring() throws Exception
    {
        // Create S3FileTransferRequestParamsDto to access the S3 bucket.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withS3KeyPrefix(TEST_S3_KEY_PREFIX + "/").build();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data restore DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME, NO_S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, Arrays.asList(new StorageFile(TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE, FILE_SIZE_1_KB, NO_ROW_COUNT)), NO_EXCEPTION);

        try
        {
            // Put a "still restoring" Glacier storage class S3 file in the Glacier S3 bucket.
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
            metadata.setOngoingRestore(true);
            s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, String.format("%s/%s", TEST_S3_KEY_PREFIX, LOCAL_FILE),
                new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]), metadata), NO_S3_CLIENT);

            // Try to execute S3 specific steps to finalize a restore for the storage unit when Glacier S3 file is still restoring.
            try
            {
                businessObjectDataFinalizeRestoreHelperService.executeS3SpecificSteps(businessObjectDataRestoreDto);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String
                    .format("Archived Glacier S3 file \"%s/%s\" is not restored. " + "StorageClass {GLACIER}, OngoingRestore flag {true}, S3 bucket name {%s}",
                        TEST_S3_KEY_PREFIX, LOCAL_FILE, S3_BUCKET_NAME), e.getMessage());
            }

            // Validate that we have a Glacier S3 file at the expected S3 location.
            assertEquals(1, s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());
        }
        finally
        {
            // Delete test files from S3 storage.
            if (!s3Dao.listDirectory(s3FileTransferRequestParamsDto).isEmpty())
            {
                s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
            }

            s3Operations.rollback();
        }
    }

    @Test
    public void testPrepareToFinalizeRestore() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a storage unit key.
        StorageUnitAlternateKeyDto storageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey);

        // Get the storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        // Prepare to finalize a restore for the business object data.
        BusinessObjectDataRestoreDto result = businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(storageUnitKey);

        // Validate the returned object.
        assertEquals(new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME, NO_S3_ENDPOINT, S3_BUCKET_NAME, result.getS3KeyPrefix(),
            NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS, storageFileHelper.createStorageFilesFromEntities(storageUnitEntity.getStorageFiles()),
            NO_EXCEPTION), result);
    }

    @Test
    public void testPrepareToFinalizeRestoreBusinessObjectDataNoExists()
    {
        // Try to prepare to finalize a restore for a non-existing business object data.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }
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
                new StorageUnitAlternateKeyDto(I_DO_NOT_EXIST, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(I_DO_NOT_EXIST, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid business object definition name.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, I_DO_NOT_EXIST, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, I_DO_NOT_EXIST, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid format usage.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, I_DO_NOT_EXIST, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, I_DO_NOT_EXIST, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid format file type.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid business object format version.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME));
            fail();
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
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, I_DO_NOT_EXIST,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    I_DO_NOT_EXIST, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid sub-partition value.
        for (int i = 0; i < SUBPARTITION_VALUES.size(); i++)
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.set(i, I_DO_NOT_EXIST);
            try
            {
                businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                    new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        testSubPartitionValues, DATA_VERSION, STORAGE_NAME));
                fail();
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
                    SUBPARTITION_VALUES, INVALID_DATA_VERSION, STORAGE_NAME));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, INVALID_DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to prepare to finalize a restore using an invalid storage name.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(
                new StorageUnitAlternateKeyDto(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, I_DO_NOT_EXIST));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", I_DO_NOT_EXIST,
                businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataKeyAsString(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                        PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION)), e.getMessage());
        }
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

        // Get the storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        // Create a storage unit key.
        StorageUnitAlternateKeyDto storageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME);

        // Prepare to finalize a restore for business object data without sub-partition values.
        BusinessObjectDataRestoreDto result = businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(storageUnitKey);

        // Validate the returned object.
        assertEquals(new BusinessObjectDataRestoreDto(businessObjectDataKey, STORAGE_NAME, NO_S3_ENDPOINT, S3_BUCKET_NAME, result.getS3KeyPrefix(),
            NO_STORAGE_UNIT_STATUS, NO_STORAGE_UNIT_STATUS, storageFileHelper.createStorageFilesFromEntities(storageUnitEntity.getStorageFiles()),
            NO_EXCEPTION), result);
    }

    @Test
    public void testPrepareToFinalizeRestoreNoS3BucketName() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with the storage not having an S3 bucket name configured.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey, STORAGE_NAME, NO_S3_BUCKET_NAME, StorageUnitStatusEntity.RESTORING);

        // Create a storage unit key.
        StorageUnitAlternateKeyDto storageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME);

        // Try to prepare to finalize a restore when S3 storage does not have an S3 bucket name configured.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(storageUnitKey);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testPrepareToFinalizeRestoreStorageUnitAlreadyRestored() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing with an already restored storage unit.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, StorageUnitStatusEntity.RESTORED);

        // Create a storage unit key.
        StorageUnitAlternateKeyDto storageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME);

        // Try to prepare to finalize a restore when storage unit is already restored.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(storageUnitKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data is already restored in \"%s\" S3 storage. Business object data: {%s}", STORAGE_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToFinalizeRestoreStorageUnitHasNoStorageFiles() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, StorageUnitStatusEntity.RESTORING);

        // Get the origin storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        // Remove storage files from the storage unit.
        storageUnitEntity.getStorageFiles().clear();

        // Create a storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME);

        // Try to prepare to finalize a restore when storage unit has no storage files.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(glacierStorageUnitKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data has no storage files registered in \"%s\" storage. Business object data: {%s}", STORAGE_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToFinalizeRestoreStorageUnitNoExists() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data entity without a storage unit.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create a storage unit key.
        StorageUnitAlternateKeyDto storageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME);

        // Try to prepare to finalize a restore when a storage unit does not exist.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(storageUnitKey);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", STORAGE_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToFinalizeRestoreStorageUnitNotInRestoringState() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit not in RESTORING state along with other database entities required for testing.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, StoragePlatformEntity.S3, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS,
                NO_STORAGE_DIRECTORY_PATH);

        // Create a storage unit key.
        StorageUnitAlternateKeyDto storageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME);

        // Try to prepare to finalize a restore when storage unit is not in RESTORING state.
        try
        {
            businessObjectDataFinalizeRestoreHelperService.prepareToFinalizeRestore(storageUnitKey);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("S3 storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. Business object data: {%s}",
                    STORAGE_NAME, StorageUnitStatusEntity.RESTORING, STORAGE_UNIT_STATUS,
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }
}
