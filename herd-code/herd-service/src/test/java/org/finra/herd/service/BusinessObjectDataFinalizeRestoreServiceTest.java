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
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StorageUnitAlternateKeyDto;
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

    @Test
    public void testGetGlacierStorageUnitsToRestore() throws Exception
    {
        // Create a list of business object data keys.
        List<BusinessObjectDataKey> businessObjectDataKeys = Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
            SUBPARTITION_VALUES, DATA_VERSION));

        // Create database entities required for testing.
        for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys)
        {
            createBusinessObjectDataEntityInRestoringState(businessObjectDataKey, STORAGE_NAME_ORIGIN, StorageUnitStatusEntity.RESTORING, STORAGE_NAME_GLACIER,
                StorageUnitStatusEntity.ENABLED);
        }

        // Select the RESTORING Glacier storage units.
        List<StorageUnitAlternateKeyDto> resultStorageUnitKeys = businessObjectDataFinalizeRestoreService.getGlacierStorageUnitsToRestore(MAX_RESULT);

        // Validate the results.
        assertEquals(businessObjectDataKeys.size(), resultStorageUnitKeys.size());
        for (BusinessObjectDataKey businessObjectDataKey : businessObjectDataKeys)
        {
            assertTrue(resultStorageUnitKeys.contains(storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER)));
        }

        // Try to select the RESTORING Glacier storage units with max result limit set to 1. Only one Glacier storage unit is expected to be returned.
        assertEquals(1, businessObjectDataFinalizeRestoreService.getGlacierStorageUnitsToRestore(1).size());
    }

    @Test
    public void testFinalizeRestore() throws Exception
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

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey);

        // Get the origin storage unit entity.
        StorageUnitEntity originStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_ORIGIN, businessObjectDataEntity);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        try
        {
            // Put relative "already restored" Glacier storage class S3 files in the Glacier S3 bucket.
            for (StorageFileEntity storageFileEntity : originStorageUnitEntity.getStorageFiles())
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME_GLACIER, String.format("%s/%s", S3_BUCKET_NAME_ORIGIN, storageFileEntity.getPath()),
                    new ByteArrayInputStream(new byte[storageFileEntity.getFileSizeBytes().intValue()]), metadata), null);
            }

            // Finalize a restore of the Glacier storage unit.
            businessObjectDataFinalizeRestoreService.finalizeRestore(glacierStorageUnitKey);

            // Validate that the origin storage unit status is now ENABLED.
            assertEquals(StorageUnitStatusEntity.ENABLED, originStorageUnitEntity.getStatus().getCode());

            // Validate that we have the restored origin S3 files at the expected origin S3 location.
            assertEquals(originStorageUnitEntity.getStorageFiles().size(), s3Dao.listDirectory(originS3FileTransferRequestParamsDto).size());

            // Validate that we have the Glacier S3 files at the expected Glacier S3 location.
            assertEquals(originStorageUnitEntity.getStorageFiles().size(), s3Dao.listDirectory(glacierS3FileTransferRequestParamsDto).size());
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
    public void testFinalizeRestoreAmazonServiceException() throws Exception
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

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = createDatabaseEntitiesForFinalizeRestoreTesting(businessObjectDataKey);

        // Get the origin storage unit entity.
        StorageUnitEntity originStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_ORIGIN, businessObjectDataEntity);

        // Add a mocked S3 file name to the origin storage unit that would trigger an Amazon service exception when we try to get metadata for the object.
        createStorageFileEntity(originStorageUnitEntity, String.format("%s/%s", TEST_S3_KEY_PREFIX, MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION),
            FILE_SIZE_1_KB, ROW_COUNT);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        try
        {
            // Put relative "already restored" Glacier storage class S3 files in the Glacier S3 bucket.
            for (StorageFileEntity storageFileEntity : originStorageUnitEntity.getStorageFiles())
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME_GLACIER, String.format("%s/%s", S3_BUCKET_NAME_ORIGIN, storageFileEntity.getPath()),
                    new ByteArrayInputStream(new byte[storageFileEntity.getFileSizeBytes().intValue()]), metadata), null);
            }

            // Try to finalize a restore for the Glacier storage unit when get S3 object metadata operation fails with an Amazon service exception.
            try
            {
                businessObjectDataFinalizeRestoreService.finalizeRestore(glacierStorageUnitKey);
                fail("Should throw an IllegalStateException when a get S3 object metadata operation fails.");
            }
            catch (IllegalStateException e)
            {
                assertEquals(String.format("Fail to check restore status for \"%s/%s/%s\" key in \"%s\" bucket. " +
                    "Reason: InternalError (Service: null; Status Code: 0; Error Code: null; Request ID: null)", S3_BUCKET_NAME_ORIGIN, TEST_S3_KEY_PREFIX,
                    MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION, S3_BUCKET_NAME_GLACIER), e.getMessage());
            }

            // Validate that the origin storage unit status is still RESTORING.
            assertEquals(StorageUnitStatusEntity.RESTORING, originStorageUnitEntity.getStatus().getCode());

            // Validate that the origin S3 location is empty.
            assertTrue(s3Dao.listDirectory(originS3FileTransferRequestParamsDto).isEmpty());

            // Validate that we have the Glacier S3 files at the expected Glacier S3 location.
            assertEquals(originStorageUnitEntity.getStorageFiles().size(), s3Dao.listDirectory(glacierS3FileTransferRequestParamsDto).size());
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

    /**
     * This method is to get coverage for the business object data finalize restore service methods that have an explicit annotation for transaction
     * propagation.
     */
    @Test
    public void testBusinessObjectDataFinalizeRestoreServiceMethodsNewTransactionPropagation()
    {
        assertEquals(0, businessObjectDataFinalizeRestoreServiceImpl.getGlacierStorageUnitsToRestore(MAX_RESULT).size());

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a Glacier storage unit key.
        StorageUnitAlternateKeyDto glacierStorageUnitKey = storageUnitHelper.createStorageUnitKey(businessObjectDataKey, STORAGE_NAME_GLACIER);

        try
        {
            businessObjectDataFinalizeRestoreServiceImpl.finalizeRestore(glacierStorageUnitKey);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectDataKey, NO_BDATA_STATUS), e.getMessage());
        }
    }
}
