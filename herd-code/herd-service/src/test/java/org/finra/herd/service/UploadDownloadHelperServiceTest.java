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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

import com.amazonaws.services.s3.model.PutObjectRequest;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.dao.impl.MockS3OperationsImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.CompleteUploadSingleParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.impl.UploadDownloadHelperServiceImpl;

public class UploadDownloadHelperServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "uploadDownloadHelperServiceImpl")
    private UploadDownloadHelperService uploadDownloadHelperServiceImpl;

    /**
     * Cleans up the S3 test path that we are using.
     */
    @After
    public void cleanEnv() throws IOException
    {
        // Clean up the source and target S3 folders. Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        for (S3FileTransferRequestParamsDto params : Arrays.asList(
            S3FileTransferRequestParamsDto.builder().s3BucketName(storageDaoTestHelper.getS3LoadingDockBucketName()).s3KeyPrefix(TEST_S3_KEY_PREFIX + "/")
                .build(),
            S3FileTransferRequestParamsDto.builder().s3BucketName(storageDaoTestHelper.getS3ExternalBucketName()).s3KeyPrefix(TEST_S3_KEY_PREFIX + "/")
                .build()))
        {
            if (!s3Dao.listDirectory(params).isEmpty())
            {
                s3Dao.deleteDirectory(params);
            }
        }

        s3Operations.rollback();
    }

    @Test
    public void testPrepareForFileMove()
    {
        // Create and persists entities required for testing.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING);
        StorageUnitEntity sourceStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE), sourceBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(sourceStorageUnitEntity, TARGET_S3_KEY, FILE_SIZE_1_KB, NO_ROW_COUNT);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING);
        StorageUnitEntity targetStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE), targetBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(targetStorageUnitEntity, TARGET_S3_KEY, FILE_SIZE_1_KB, NO_ROW_COUNT);

        // Put a 1 KB file in S3.
        PutObjectRequest putObjectRequest =
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]),
                null);
        s3Operations.putObject(putObjectRequest, null);

        // Create an uninitialized DTO to hold parameters required to perform a complete upload single message processing.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto = new CompleteUploadSingleParamsDto();

        // Prepare for the file move operation.
        uploadDownloadHelperService.prepareForFileMove(TARGET_S3_KEY, completeUploadSingleParamsDto);

        // Refresh the data entities.
        sourceBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate the source and target business object data statuses.
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, targetBusinessObjectDataEntity.getStatus().getCode());

        // Validate the updated DTO parameters.
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getSourceNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, completeUploadSingleParamsDto.getSourceOldStatus());
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getTargetNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, completeUploadSingleParamsDto.getTargetOldStatus());
    }

    @Test
    public void testPrepareForFileMoveSourceStorageFileNoExists() throws Exception
    {
        // Create and persists entities required for testing without creating source or target storage units.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING);

        // Create an uninitialized DTO to hold parameters required to perform a complete upload single message processing.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto = new CompleteUploadSingleParamsDto();

        // Try to prepare for the file move operation. This step will fail due to a non-existing source storage file.
        executeWithoutLogging(UploadDownloadHelperServiceImpl.class, () -> {
            uploadDownloadHelperService.prepareForFileMove(FILE_NAME, completeUploadSingleParamsDto);
        });

        // Refresh the data entities.
        sourceBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate the source and target business object data statuses.
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, targetBusinessObjectDataEntity.getStatus().getCode());

        // Validate the returned DTO parameters.
        assertNull(completeUploadSingleParamsDto.getSourceNewStatus());
        assertNull(completeUploadSingleParamsDto.getSourceOldStatus());
        assertNull(completeUploadSingleParamsDto.getTargetNewStatus());
        assertNull(completeUploadSingleParamsDto.getTargetOldStatus());
    }

    @Test
    public void testPrepareForFileMoveSourceStatusNotUploading() throws Exception
    {
        // Create and persists entities required for testing with the source business object data not having "UPLOADING" status.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        StorageUnitEntity sourceStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE), sourceBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(sourceStorageUnitEntity, FILE_NAME, FILE_SIZE_1_KB, NO_ROW_COUNT);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING);
        StorageUnitEntity targetStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE), targetBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(targetStorageUnitEntity, FILE_NAME, FILE_SIZE_1_KB, NO_ROW_COUNT);

        // Create an uninitialized DTO to hold parameters required to perform a complete upload single message processing.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto = new CompleteUploadSingleParamsDto();

        // Try to execute the prepare for the file move operation when the source business object data does not have "UPLOADING" status.
        executeWithoutLogging(UploadDownloadHelperServiceImpl.class, () -> {
            uploadDownloadHelperService.prepareForFileMove(FILE_NAME, completeUploadSingleParamsDto);
        });

        // Refresh the data entities.
        sourceBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate the source and target business object data statuses.
        assertEquals(BusinessObjectDataStatusEntity.VALID, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, targetBusinessObjectDataEntity.getStatus().getCode());

        // Validate the updated DTO parameters.
        assertNull(completeUploadSingleParamsDto.getSourceNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.VALID, completeUploadSingleParamsDto.getSourceOldStatus());
        assertNull(completeUploadSingleParamsDto.getTargetNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, completeUploadSingleParamsDto.getTargetOldStatus());
    }

    @Test
    public void testPrepareForFileMoveTargetStatusNotUploading() throws Exception
    {
        // Create and persists entities required for testing with the target business object data not having "UPLOADING" status.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING);
        StorageUnitEntity sourceStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE), sourceBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(sourceStorageUnitEntity, FILE_NAME, FILE_SIZE_1_KB, NO_ROW_COUNT);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
        StorageUnitEntity targetStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE), targetBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(targetStorageUnitEntity, FILE_NAME, FILE_SIZE_1_KB, NO_ROW_COUNT);

        // Create an uninitialized DTO to hold parameters required to perform a complete upload single message processing.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto = new CompleteUploadSingleParamsDto();

        // Try to execute the prepare for the file move operation when the target business object data does not have "UPLOADING" status.
        executeWithoutLogging(UploadDownloadHelperServiceImpl.class, () -> {
            uploadDownloadHelperService.prepareForFileMove(FILE_NAME, completeUploadSingleParamsDto);
        });

        // Refresh the data entities.
        sourceBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate the source and target business object data statuses.
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.VALID, targetBusinessObjectDataEntity.getStatus().getCode());

        // Validate the updated DTO parameters.
        assertNull(completeUploadSingleParamsDto.getSourceNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, completeUploadSingleParamsDto.getSourceOldStatus());
        assertNull(completeUploadSingleParamsDto.getTargetNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.VALID, completeUploadSingleParamsDto.getTargetOldStatus());
    }

    @Test
    public void testPrepareForFileMoveSourceS3FileNoExists() throws Exception
    {
        // Create and persists entities required for testing.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING);
        StorageUnitEntity sourceStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE), sourceBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(sourceStorageUnitEntity, FILE_NAME, FILE_SIZE_1_KB, NO_ROW_COUNT);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING);
        StorageUnitEntity targetStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE), targetBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(targetStorageUnitEntity, FILE_NAME, FILE_SIZE_1_KB, NO_ROW_COUNT);

        // Create an uninitialized DTO to hold parameters required to perform a complete upload single message processing.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto = new CompleteUploadSingleParamsDto();

        // Try to prepare for the file move operation. This step will fail due to a non-existing source S3 file.
        executeWithoutLogging(UploadDownloadHelperServiceImpl.class, () -> {
            uploadDownloadHelperService.prepareForFileMove(FILE_NAME, completeUploadSingleParamsDto);
        });

        // Refresh the data entities.
        sourceBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate the source and target business object data statuses.
        assertEquals(BusinessObjectDataStatusEntity.DELETED, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.INVALID, targetBusinessObjectDataEntity.getStatus().getCode());

        // Validate the updated DTO parameters.
        assertEquals(BusinessObjectDataStatusEntity.DELETED, completeUploadSingleParamsDto.getSourceNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, completeUploadSingleParamsDto.getSourceOldStatus());
        assertEquals(BusinessObjectDataStatusEntity.INVALID, completeUploadSingleParamsDto.getTargetNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, completeUploadSingleParamsDto.getTargetOldStatus());
    }

    @Test
    public void testPrepareForFileMoveTargetS3FileAlreadyExists() throws Exception
    {
        // Create and persists entities required for testing.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING);
        StorageUnitEntity sourceStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE), sourceBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(sourceStorageUnitEntity, TARGET_S3_KEY, FILE_SIZE_1_KB, NO_ROW_COUNT);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.UPLOADING);
        StorageUnitEntity targetStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE), targetBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(targetStorageUnitEntity, TARGET_S3_KEY, FILE_SIZE_1_KB, NO_ROW_COUNT);

        // Put a 1 KB file in both source and target S3 buckets.
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]),
                null), null);
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3ExternalBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]), null),
            null);

        // Create an uninitialized DTO to hold parameters required to perform a complete upload single message processing.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto = new CompleteUploadSingleParamsDto();

        // Try to prepare for the file move operation. This step will fail due to an already existing target S3 file.
        executeWithoutLogging(UploadDownloadHelperServiceImpl.class, () -> {
            uploadDownloadHelperService.prepareForFileMove(TARGET_S3_KEY, completeUploadSingleParamsDto);
        });

        // Refresh the data entities.
        sourceBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate the source and target business object data statuses.
        assertEquals(BusinessObjectDataStatusEntity.DELETED, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.INVALID, targetBusinessObjectDataEntity.getStatus().getCode());

        // Validate the updated DTO parameters.
        assertEquals(BusinessObjectDataStatusEntity.DELETED, completeUploadSingleParamsDto.getSourceNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, completeUploadSingleParamsDto.getSourceOldStatus());
        assertEquals(BusinessObjectDataStatusEntity.INVALID, completeUploadSingleParamsDto.getTargetNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, completeUploadSingleParamsDto.getTargetOldStatus());
    }

    @Test
    public void testPerformFileMove()
    {
        // Create source and target business object data entities.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);

        // Put a 1 KB file in S3.
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]),
                null), null);

        // Initialize parameters required to perform a file move.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto =
            new CompleteUploadSingleParamsDto(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity),
                storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, BusinessObjectDataStatusEntity.UPLOADING,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity),
                storageDaoTestHelper.getS3ExternalBucketName(), TARGET_S3_KEY, BusinessObjectDataStatusEntity.UPLOADING,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, MockS3OperationsImpl.MOCK_KMS_ID, emrHelper.getAwsParamsDto());

        // Perform the file move.
        uploadDownloadHelperService.performFileMove(completeUploadSingleParamsDto);

        // Refresh the data entities.
        sourceBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate the source business object data status.
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getSourceNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, completeUploadSingleParamsDto.getSourceOldStatus());

        // Validate the target business object data status.
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, targetBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.VALID, completeUploadSingleParamsDto.getTargetNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getTargetOldStatus());
    }

    @Test
    public void testPerformFileMoveS3CopyFails() throws Exception
    {
        // Create source and target business object data entities.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);

        // Initialize parameters required to perform a file move.
        // Make the S3 copy operation fail by passing a special mocked file name.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto =
            new CompleteUploadSingleParamsDto(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity), EMPTY_S3_BUCKET_NAME,
                MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION, BusinessObjectDataStatusEntity.UPLOADING,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity),
                EMPTY_S3_BUCKET_NAME, MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION, BusinessObjectDataStatusEntity.UPLOADING,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, MockS3OperationsImpl.MOCK_KMS_ID, emrHelper.getAwsParamsDto());

        // Try to perform the file move.
        executeWithoutLogging(UploadDownloadHelperServiceImpl.class, () -> {
            uploadDownloadHelperService.performFileMove(completeUploadSingleParamsDto);
        });

        // Refresh the data entities
        sourceBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate the source business object data status.
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getSourceNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.UPLOADING, completeUploadSingleParamsDto.getSourceOldStatus());

        // Validate the target business object data status.
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, targetBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.INVALID, completeUploadSingleParamsDto.getTargetNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getTargetOldStatus());
    }

    @Test
    public void testExecuteFileMoveAfterSteps()
    {
        // Create and persists entities required for testing.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        StorageUnitEntity sourceStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE), sourceBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(sourceStorageUnitEntity, TARGET_S3_KEY, FILE_SIZE_1_KB, NO_ROW_COUNT);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        StorageUnitEntity targetStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE), targetBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(targetStorageUnitEntity, TARGET_S3_KEY, FILE_SIZE_1_KB, NO_ROW_COUNT);

        // Put a 1 KB file in S3.
        PutObjectRequest putObjectRequest =
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]),
                null);
        s3Operations.putObject(putObjectRequest, null);

        // Initialize parameters required to perform the file move post steps.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto =
            new CompleteUploadSingleParamsDto(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity),
                storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, BusinessObjectDataStatusEntity.UPLOADING,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity),
                storageDaoTestHelper.getS3ExternalBucketName(), TARGET_S3_KEY, BusinessObjectDataStatusEntity.RE_ENCRYPTING,
                BusinessObjectDataStatusEntity.VALID, MockS3OperationsImpl.MOCK_KMS_ID, emrHelper.getAwsParamsDto());

        // Execute the file move post steps.
        uploadDownloadHelperService.executeFileMoveAfterSteps(completeUploadSingleParamsDto);

        // Refresh the data entities.
        sourceBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate the source and target business object data statuses.
        assertEquals(BusinessObjectDataStatusEntity.DELETED, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.VALID, targetBusinessObjectDataEntity.getStatus().getCode());

        // Validate the updated DTO parameters.
        assertEquals(BusinessObjectDataStatusEntity.DELETED, completeUploadSingleParamsDto.getSourceNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getSourceOldStatus());
        assertEquals(BusinessObjectDataStatusEntity.VALID, completeUploadSingleParamsDto.getTargetNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getTargetOldStatus());
    }

    @Test
    public void testExecuteFileMoveAfterStepsTargetStatusNotReEncrypting()
    {
        // Create and persists entities required for testing with the target business object data not having "RE-ENCRYPTING" status.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        StorageUnitEntity sourceStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE), sourceBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(sourceStorageUnitEntity, TARGET_S3_KEY, FILE_SIZE_1_KB, NO_ROW_COUNT);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS);
        StorageUnitEntity targetStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE), targetBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(targetStorageUnitEntity, TARGET_S3_KEY, FILE_SIZE_1_KB, NO_ROW_COUNT);

        // Put a 1 KB file in the source S3 bucket.
        PutObjectRequest putObjectRequest =
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]),
                null);
        s3Operations.putObject(putObjectRequest, null);

        // Initialize parameters required to perform the file move post steps.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto =
            new CompleteUploadSingleParamsDto(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity),
                storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, BusinessObjectDataStatusEntity.UPLOADING,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity),
                storageDaoTestHelper.getS3ExternalBucketName(), TARGET_S3_KEY, BusinessObjectDataStatusEntity.RE_ENCRYPTING,
                BusinessObjectDataStatusEntity.VALID, MockS3OperationsImpl.MOCK_KMS_ID, emrHelper.getAwsParamsDto());

        // Try to execute the file move post steps when the target business object data does not have "RE-ENCRYPTING" status.
        uploadDownloadHelperService.executeFileMoveAfterSteps(completeUploadSingleParamsDto);

        // Refresh the data entities.
        sourceBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate the source and target business object data statuses.
        assertEquals(BusinessObjectDataStatusEntity.DELETED, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BDATA_STATUS, targetBusinessObjectDataEntity.getStatus().getCode());

        // Validate the updated DTO parameters.
        assertEquals(BusinessObjectDataStatusEntity.DELETED, completeUploadSingleParamsDto.getSourceNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getSourceOldStatus());
        assertNull(completeUploadSingleParamsDto.getTargetNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getTargetOldStatus());
    }

    @Test
    public void testExecuteFileMoveAfterStepsNewTargetStatusNotValid()
    {
        // Create and persists entities required for testing.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        StorageUnitEntity sourceStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE), sourceBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(sourceStorageUnitEntity, TARGET_S3_KEY, FILE_SIZE_1_KB, NO_ROW_COUNT);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        StorageUnitEntity targetStorageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_EXTERNAL_STORAGE), targetBusinessObjectDataEntity,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);
        storageFileDaoTestHelper.createStorageFileEntity(targetStorageUnitEntity, TARGET_S3_KEY, FILE_SIZE_1_KB, NO_ROW_COUNT);

        // Put a 1 KB file in the source S3 bucket.
        PutObjectRequest putObjectRequest =
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]),
                null);
        s3Operations.putObject(putObjectRequest, null);

        // Initialize parameters required to perform the file move post steps with new target status not being set to "VALID".
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto =
            new CompleteUploadSingleParamsDto(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity),
                storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, BusinessObjectDataStatusEntity.UPLOADING,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity),
                storageDaoTestHelper.getS3ExternalBucketName(), TARGET_S3_KEY, BusinessObjectDataStatusEntity.RE_ENCRYPTING,
                BusinessObjectDataStatusEntity.INVALID, MockS3OperationsImpl.MOCK_KMS_ID, emrHelper.getAwsParamsDto());

        // Try to execute the file move post steps when new target business object data status is not set to "VALID".
        uploadDownloadHelperService.executeFileMoveAfterSteps(completeUploadSingleParamsDto);

        // Refresh the data entities.
        sourceBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));
        targetBusinessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

        // Validate the source and target business object data statuses.
        assertEquals(BusinessObjectDataStatusEntity.DELETED, sourceBusinessObjectDataEntity.getStatus().getCode());
        assertEquals(BusinessObjectDataStatusEntity.INVALID, targetBusinessObjectDataEntity.getStatus().getCode());

        // Validate the updated DTO parameters.
        assertEquals(BusinessObjectDataStatusEntity.DELETED, completeUploadSingleParamsDto.getSourceNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getSourceOldStatus());
        assertEquals(BusinessObjectDataStatusEntity.INVALID, completeUploadSingleParamsDto.getTargetNewStatus());
        assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getTargetOldStatus());
    }

    /**
     * This method is to get coverage for the upload download helper service methods that have explicit annotations for transaction propagation.
     */
    @Test
    public void testUploadDownloadHelperServiceMethodsNewTransactionPropagation() throws Exception
    {
        executeWithoutLogging(UploadDownloadHelperServiceImpl.class, () -> {
            CompleteUploadSingleParamsDto completeUploadSingleParamsDto;

            // Try to perform a prepare file move operation with a non-existing S3 key (file path).
            completeUploadSingleParamsDto = new CompleteUploadSingleParamsDto();
            uploadDownloadHelperServiceImpl.prepareForFileMove("KEY_DOES_NOT_EXIST", completeUploadSingleParamsDto);

            // Validate the updated complete upload single parameters DTO.
            assertNull(completeUploadSingleParamsDto.getSourceBusinessObjectDataKey());
            assertNull(completeUploadSingleParamsDto.getSourceNewStatus());
            assertNull(completeUploadSingleParamsDto.getSourceOldStatus());
            assertNull(completeUploadSingleParamsDto.getTargetBusinessObjectDataKey());
            assertNull(completeUploadSingleParamsDto.getTargetNewStatus());
            assertNull(completeUploadSingleParamsDto.getTargetOldStatus());

            // Try to perform an S3 file move. The S3 copy operation will fail since we are passing a special mocked file name.
            completeUploadSingleParamsDto = new CompleteUploadSingleParamsDto(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION), EMPTY_S3_BUCKET_NAME, MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION,
                BusinessObjectDataStatusEntity.UPLOADING, BusinessObjectDataStatusEntity.RE_ENCRYPTING,
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION), EMPTY_S3_BUCKET_NAME, MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION,
                BusinessObjectDataStatusEntity.UPLOADING, BusinessObjectDataStatusEntity.RE_ENCRYPTING, MockS3OperationsImpl.MOCK_KMS_ID,
                emrHelper.getAwsParamsDto());
            uploadDownloadHelperServiceImpl.performFileMove(completeUploadSingleParamsDto);

            // Validate the updated complete upload single parameters DTO.
            assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getSourceNewStatus());
            assertEquals(BusinessObjectDataStatusEntity.UPLOADING, completeUploadSingleParamsDto.getSourceOldStatus());
            assertEquals(BusinessObjectDataStatusEntity.INVALID, completeUploadSingleParamsDto.getTargetNewStatus());
            assertEquals(BusinessObjectDataStatusEntity.RE_ENCRYPTING, completeUploadSingleParamsDto.getTargetOldStatus());

            // Try to execute the file move post steps by passing a non-initialized complete upload single parameters DTO.
            uploadDownloadHelperServiceImpl.executeFileMoveAfterSteps(new CompleteUploadSingleParamsDto());

            // Try to update the business object data status for a non-existing business object data.
            try
            {
                uploadDownloadHelperServiceImpl.updateBusinessObjectDataStatus(
                    new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        NO_SUBPARTITION_VALUES, DATA_VERSION), null);
                fail("Should throw an ObjectNotFoundException.");
            }
            catch (ObjectNotFoundException e)
            {
                assertEquals(businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                        PARTITION_VALUE, NO_SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
            }
        });
    }

    @Test
    public void testDeleteSourceFileFromS3()
    {
        // Create and persists entities required for testing.
        // Create source and target business object data entities.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);

        // Put a 1 KB file in S3.
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]),
                null), null);

        // Initialize parameters required to delete the S3 source file
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto =
            new CompleteUploadSingleParamsDto(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity),
                storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, BusinessObjectDataStatusEntity.UPLOADING,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity),
                storageDaoTestHelper.getS3ExternalBucketName(), TARGET_S3_KEY, BusinessObjectDataStatusEntity.UPLOADING,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, MockS3OperationsImpl.MOCK_KMS_ID, emrHelper.getAwsParamsDto());

        // Delete the source file from S3
        uploadDownloadHelperService.deleteSourceFileFromS3(completeUploadSingleParamsDto);
    }

    @Test
    public void testDeleteSourceFileFromS3Fails()  throws Exception
    {
        // Create and persists entities required for testing.
        // Create source and target business object data entities.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        BusinessObjectDataEntity targetBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.RE_ENCRYPTING);

        // Put a 1 KB file in S3.
        s3Operations.putObject(
            new PutObjectRequest(storageDaoTestHelper.getS3LoadingDockBucketName(), TARGET_S3_KEY, new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]),
                null), null);
        
        // Make the S3 delete fail by a null aws Params
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto =
            new CompleteUploadSingleParamsDto(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity), EMPTY_S3_BUCKET_NAME,
                MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION, BusinessObjectDataStatusEntity.UPLOADING,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity),
                EMPTY_S3_BUCKET_NAME, MockS3OperationsImpl.MOCK_S3_FILE_NAME_SERVICE_EXCEPTION, BusinessObjectDataStatusEntity.UPLOADING,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, MockS3OperationsImpl.MOCK_KMS_ID, null);

        // Try to delete the source file from S3
        executeWithoutLogging(UploadDownloadHelperServiceImpl.class, () -> {
            uploadDownloadHelperService.deleteSourceFileFromS3(completeUploadSingleParamsDto);
        });
    }
}
