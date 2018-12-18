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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
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
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.impl.BusinessObjectDataFinalizeRestoreServiceImpl;

/**
 * This class tests functionality within the business object data finalize restore service.
 */
public class BusinessObjectDataFinalizeRestoreServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "businessObjectDataFinalizeRestoreServiceImpl")
    private BusinessObjectDataFinalizeRestoreServiceImpl businessObjectDataFinalizeRestoreServiceImpl;

    /**
     * This method is to get coverage for the business object data finalize restore service methods that have an explicit annotation for transaction
     * propagation.
     */
    @Test
    public void testBusinessObjectDataFinalizeRestoreServiceMethodsNewTransactionPropagation()
    {
        assertEquals(0, businessObjectDataFinalizeRestoreServiceImpl.getS3StorageUnitsToRestore(MAX_RESULT).size());

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit key.
        BusinessObjectDataStorageUnitKey storageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME);

        try
        {
            businessObjectDataFinalizeRestoreServiceImpl.finalizeRestore(storageUnitKey);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectDataKey, NO_BDATA_STATUS),
                e.getMessage());
        }
    }

    @Test
    public void testFinalizeRestore() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Get the expected S3 key prefix for the business object data key.
        String s3KeyPrefix = AbstractServiceTest
            .getExpectedS3KeyPrefix(businessObjectDataKey, AbstractServiceTest.DATA_PROVIDER_NAME, AbstractServiceTest.PARTITION_KEY,
                AbstractServiceTest.NO_SUB_PARTITION_KEYS);

        // Create S3FileTransferRequestParamsDto to access the S3 bucket.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withS3KeyPrefix(s3KeyPrefix + "/").build();

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey);

        // Get the storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        // Create a storage unit key.
        BusinessObjectDataStorageUnitKey storageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME);

        try
        {
            // Put relative "already restored" Glacier storage class S3 files in the S3 bucket.
            for (StorageFileEntity storageFileEntity : storageUnitEntity.getStorageFiles())
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, storageFileEntity.getPath(),
                    new ByteArrayInputStream(new byte[storageFileEntity.getFileSizeBytes().intValue()]), metadata), NO_S3_CLIENT);
            }

            // Add one more S3 file, which is an unregistered zero byte file.
            // The validation is expected not to fail when detecting an unregistered zero byte S3 file.
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
            metadata.setOngoingRestore(false);
            s3Operations
                .putObject(new PutObjectRequest(S3_BUCKET_NAME, s3KeyPrefix + "/unregistered.dat", new ByteArrayInputStream(new byte[0]), metadata), null);

            // Assert that we got all files listed under the test S3 prefix.
            assertEquals(storageUnitEntity.getStorageFiles().size() + 1, s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());

            // Finalize a restore of the storage unit.
            businessObjectDataFinalizeRestoreService.finalizeRestore(storageUnitKey);

            // Validate that the storage unit status is now RESTORED.
            assertEquals(StorageUnitStatusEntity.RESTORED, storageUnitEntity.getStatus().getCode());

            // Validate that we have the S3 files at the expected S3 location.
            assertEquals(storageUnitEntity.getStorageFiles().size() + 1, s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());
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
    public void testFinalizeRestoreAmazonServiceException() throws Exception
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Get the expected S3 key prefix for the business object data key.
        String s3KeyPrefix = AbstractServiceTest
            .getExpectedS3KeyPrefix(businessObjectDataKey, AbstractServiceTest.DATA_PROVIDER_NAME, AbstractServiceTest.PARTITION_KEY,
                AbstractServiceTest.NO_SUB_PARTITION_KEYS);

        // Create S3FileTransferRequestParamsDto to access the S3 bucket.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withS3KeyPrefix(s3KeyPrefix + "/").build();

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey);

        // Get the storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        // Add a mocked S3 file name to the storage unit that would trigger an Amazon service exception when we try to get metadata for the object.
        storageFileDaoTestHelper
            .createStorageFileEntity(storageUnitEntity, String.format("%s/%s", s3KeyPrefix, MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION),
                FILE_SIZE_1_KB, ROW_COUNT);

        // Create a storage unit key.
        BusinessObjectDataStorageUnitKey storageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME);

        try
        {
            // Put relative "already restored" Glacier storage class S3 files in the S3 bucket.
            for (StorageFileEntity storageFileEntity : storageUnitEntity.getStorageFiles())
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, storageFileEntity.getPath(),
                    new ByteArrayInputStream(new byte[storageFileEntity.getFileSizeBytes().intValue()]), metadata), null);
            }

            // Try to finalize a restore for the storage unit when get S3 object metadata operation fails with an Amazon service exception.
            try
            {
                businessObjectDataFinalizeRestoreService.finalizeRestore(storageUnitKey);
                fail("Should throw an IllegalStateException when a get S3 object metadata operation fails.");
            }
            catch (IllegalStateException e)
            {
                assertEquals(String.format("Fail to check restore status for \"%s/%s\" key in \"%s\" bucket. " +
                        "Reason: InternalError (Service: null; Status Code: 0; Error Code: InternalError; Request ID: null)", s3KeyPrefix,
                    MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION, S3_BUCKET_NAME), e.getMessage());
            }

            // Validate that the storage unit status is still in RESTORING state.
            assertEquals(StorageUnitStatusEntity.RESTORING, storageUnitEntity.getStatus().getCode());

            // Validate that we have the S3 files at the expected S3 location.
            assertEquals(storageUnitEntity.getStorageFiles().size(), s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());
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
    public void testGetS3StorageUnitsToRestore() throws Exception
    {
        // Create a list of business object data keys.
        List<BusinessObjectDataKey> businessObjectDataKeys = Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION));

        // Create database entities required for testing.
        for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys)
        {
            storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, StorageUnitStatusEntity.RESTORING);
        }

        // Select the RESTORING storage units.
        List<BusinessObjectDataStorageUnitKey> resultStorageUnitKeys = businessObjectDataFinalizeRestoreService.getS3StorageUnitsToRestore(MAX_RESULT);

        // Validate the results.
        assertEquals(businessObjectDataKeys.size(), resultStorageUnitKeys.size());
        for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys)
        {
            assertTrue(resultStorageUnitKeys.contains(storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME)));
        }

        // Try to select the RESTORING storage units with max result limit set to 1. Only one storage unit is expected to be returned.
        assertEquals(1, businessObjectDataFinalizeRestoreService.getS3StorageUnitsToRestore(1).size());
    }
}
