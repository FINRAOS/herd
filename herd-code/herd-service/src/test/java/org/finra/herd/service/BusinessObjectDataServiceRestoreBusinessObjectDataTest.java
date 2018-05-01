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
import java.util.Collections;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.google.common.collect.Iterables;
import org.junit.Test;

import org.finra.herd.dao.impl.MockS3OperationsImpl;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * This class tests restoreBusinessObjectData functionality within the business object data service.
 */
public class BusinessObjectDataServiceRestoreBusinessObjectDataTest extends AbstractServiceTest
{
    @Test
    public void testRestoreBusinessObjectData() throws Exception
    {
        // Create S3FileTransferRequestParamsDto to access the S3 bucket.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withS3KeyPrefix(S3_BUCKET_NAME + "/" + TEST_S3_KEY_PREFIX + "/").build();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Get the storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        try
        {
            // Put relative Glacier storage class files into the S3 bucket flagged as not being currently restored.
            for (StorageFileEntity storageFileEntity : storageUnitEntity.getStorageFiles())
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, storageFileEntity.getPath(),
                    new ByteArrayInputStream(new byte[storageFileEntity.getFileSizeBytes().intValue()]), metadata), NO_S3_CLIENT);
            }

            // Initiate a restore request for the business object data.
            BusinessObjectData businessObjectData = businessObjectDataService.restoreBusinessObjectData(businessObjectDataKey, EXPIRATION_IN_DAYS);

            // Validate the returned object.
            businessObjectDataServiceTestHelper
                .validateBusinessObjectData(businessObjectDataEntity.getId(), businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS, businessObjectData);

            // Validate that the storage unit status is RESTORING.
            assertEquals(StorageUnitStatusEntity.RESTORING, storageUnitEntity.getStatus().getCode());

            // Validate that there is now ongoing restore request for all Glacier objects.
            for (StorageFileEntity storageFileEntity : storageUnitEntity.getStorageFiles())
            {
                ObjectMetadata objectMetadata = s3Operations.getObjectMetadata(S3_BUCKET_NAME, storageFileEntity.getPath(), NO_S3_CLIENT);
                assertTrue(objectMetadata.getOngoingRestore());
            }
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
    public void testRestoreBusinessObjectDataAmazonServiceException() throws Exception
    {
        // Create S3FileTransferRequestParamsDto to access the S3 bucket location.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withS3KeyPrefix(S3_BUCKET_NAME + "/" + TEST_S3_KEY_PREFIX + "/").build();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Get the storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        // Get the expected S3 key prefix for the business object data key.
        String s3KeyPrefix = AbstractServiceTest
            .getExpectedS3KeyPrefix(businessObjectDataKey, AbstractServiceTest.DATA_PROVIDER_NAME, AbstractServiceTest.PARTITION_KEY,
                AbstractServiceTest.NO_SUB_PARTITION_KEYS);

        // Add a mocked S3 file name to the storage unit that would trigger an Amazon service exception when we request to restore objects.
        storageFileDaoTestHelper
            .createStorageFileEntity(storageUnitEntity, String.format("%s/%s", s3KeyPrefix, MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION),
                FILE_SIZE_1_KB, ROW_COUNT);

        try
        {
            // Put relative Glacier storage class files into the Glacier S3 bucket flagged as not being currently restored.
            for (StorageFileEntity storageFileEntity : storageUnitEntity.getStorageFiles())
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, storageFileEntity.getPath(),
                    new ByteArrayInputStream(new byte[storageFileEntity.getFileSizeBytes().intValue()]), metadata), NO_S3_CLIENT);
            }

            // Try to initiate a restore request for the business object data when S3 restore object operation fails with an Amazon service exception.
            try
            {
                businessObjectDataService.restoreBusinessObjectData(businessObjectDataKey, EXPIRATION_IN_DAYS);
                fail();
            }
            catch (IllegalStateException e)
            {
                assertEquals(String.format("Failed to initiate a restore request for \"%s/%s\" key in \"%s\" bucket. " +
                        "Reason: InternalError (Service: null; Status Code: 0; Error Code: InternalError; Request ID: null)", s3KeyPrefix,
                    MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION, S3_BUCKET_NAME), e.getMessage());
            }

            // Validate that the storage unit status is still ARCHIVED.
            assertEquals(StorageUnitStatusEntity.ARCHIVED, storageUnitEntity.getStatus().getCode());
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
    public void testRestoreBusinessObjectDataNonGlacierStorageClass() throws Exception
    {
        // Create S3FileTransferRequestParamsDto to access the S3 bucket.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto glacierS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().withS3BucketName(S3_BUCKET_NAME).withS3KeyPrefix(S3_BUCKET_NAME + "/" + TEST_S3_KEY_PREFIX + "/").build();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey, AbstractServiceTest.STORAGE_NAME, AbstractServiceTest.S3_BUCKET_NAME,
                StorageUnitStatusEntity.ARCHIVED, Collections.singletonList(LOCAL_FILE));

        // Get the storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        try
        {
            // Put relative non-Glacier storage class files into the S3 bucket.
            for (StorageFileEntity storageFileEntity : storageUnitEntity.getStorageFiles())
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Standard);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME, storageFileEntity.getPath(),
                    new ByteArrayInputStream(new byte[storageFileEntity.getFileSizeBytes().intValue()]), metadata), NO_S3_CLIENT);
            }

            // Try to initiate a restore request for the business object data.
            try
            {
                businessObjectDataService.restoreBusinessObjectData(businessObjectDataKey, EXPIRATION_IN_DAYS);
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("S3 file \"%s\" is not archived (found %s storage class when expecting %s). S3 Bucket Name: \"%s\"",
                    Iterables.get(storageUnitEntity.getStorageFiles(), 0).getPath(), StorageClass.Standard.toString(), StorageClass.Glacier.toString(),
                    S3_BUCKET_NAME), e.getMessage());
            }

            // Validate that the storage unit status is still ARCHIVED.
            assertEquals(StorageUnitStatusEntity.ARCHIVED, storageUnitEntity.getStatus().getCode());
        }
        finally
        {
            // Delete test files from S3 storage.
            if (!s3Dao.listDirectory(glacierS3FileTransferRequestParamsDto).isEmpty())
            {
                s3Dao.deleteDirectory(glacierS3FileTransferRequestParamsDto);
            }

            s3Operations.rollback();
        }
    }
}
