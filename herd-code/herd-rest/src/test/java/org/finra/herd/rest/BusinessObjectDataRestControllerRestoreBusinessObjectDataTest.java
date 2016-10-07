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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * This class tests restoreBusinessObjectData functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerRestoreBusinessObjectDataTest extends AbstractRestTest
{
    @Test
    public void testRestoreBusinessObjectData() throws Exception
    {
        // Create S3FileTransferRequestParamsDto to access the Glacier S3 bucket location.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto glacierS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_GLACIER).s3KeyPrefix(S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX + "/")
                .build();

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Get the origin storage unit entity.
        StorageUnitEntity originStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_ORIGIN, businessObjectDataEntity);

        try
        {
            // Put relative Glacier storage class files into the Glacier S3 bucket flagged as not being currently restored.
            for (StorageFileEntity storageFileEntity : originStorageUnitEntity.getStorageFiles())
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME_GLACIER, String.format("%s/%s", S3_BUCKET_NAME_ORIGIN, storageFileEntity.getPath()),
                    new ByteArrayInputStream(new byte[storageFileEntity.getFileSizeBytes().intValue()]), metadata), null);
            }

            // Initiate a restore request for the business object data.
            BusinessObjectData businessObjectData = businessObjectDataRestController
                .restoreBusinessObjectData(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                    getDelimitedFieldValues(SUBPARTITION_VALUES));

            // Validate the returned object.
            businessObjectDataServiceTestHelper
                .validateBusinessObjectData(businessObjectDataEntity.getId(), businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS, businessObjectData);

            // Validate that the origin storage unit status is RESTORING.
            assertEquals(StorageUnitStatusEntity.RESTORING, originStorageUnitEntity.getStatus().getCode());

            // Validate that there is now ongoing restore request for all Glacier objects.
            for (StorageFileEntity storageFileEntity : originStorageUnitEntity.getStorageFiles())
            {
                ObjectMetadata objectMetadata =
                    s3Operations.getObjectMetadata(S3_BUCKET_NAME_GLACIER, String.format("%s/%s", S3_BUCKET_NAME_ORIGIN, storageFileEntity.getPath()), null);
                assertTrue(objectMetadata.getOngoingRestore());
            }
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

    @Test
    public void testRestoreBusinessObjectDataMissingOptionalParameters() throws Exception
    {
        // Create S3FileTransferRequestParamsDto to access the Glacier S3 bucket location.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto glacierS3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME_GLACIER).s3KeyPrefix(S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX + "/")
                .build();

        // Create a business object data key without sub-partition values.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForInitiateRestoreTesting(businessObjectDataKey);

        // Get the origin storage unit entity.
        StorageUnitEntity originStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_ORIGIN, businessObjectDataEntity);

        try
        {
            // Put relative Glacier storage class files into the Glacier S3 bucket flagged as not being currently restored.
            for (StorageFileEntity storageFileEntity : originStorageUnitEntity.getStorageFiles())
            {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Glacier);
                metadata.setOngoingRestore(false);
                s3Operations.putObject(new PutObjectRequest(S3_BUCKET_NAME_GLACIER, String.format("%s/%s", S3_BUCKET_NAME_ORIGIN, storageFileEntity.getPath()),
                    new ByteArrayInputStream(new byte[storageFileEntity.getFileSizeBytes().intValue()]), metadata), null);
            }

            // Initiate a restore request for the business object data that has no sub-partition values.
            BusinessObjectData businessObjectData = businessObjectDataRestController
                .restoreBusinessObjectData(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                    null);

            // Validate the returned object.
            businessObjectDataServiceTestHelper
                .validateBusinessObjectData(businessObjectDataEntity.getId(), businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS, businessObjectData);

            // Validate that the origin storage unit status is RESTORING.
            assertEquals(StorageUnitStatusEntity.RESTORING, originStorageUnitEntity.getStatus().getCode());

            // Validate that there is now ongoing restore request for all Glacier objects.
            for (StorageFileEntity storageFileEntity : originStorageUnitEntity.getStorageFiles())
            {
                ObjectMetadata objectMetadata =
                    s3Operations.getObjectMetadata(S3_BUCKET_NAME_GLACIER, String.format("%s/%s", S3_BUCKET_NAME_ORIGIN, storageFileEntity.getPath()), null);
                assertTrue(objectMetadata.getOngoingRestore());
            }
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
