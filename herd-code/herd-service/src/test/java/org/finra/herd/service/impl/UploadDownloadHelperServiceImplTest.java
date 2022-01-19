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
package org.finra.herd.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.persistence.OptimisticLockException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.CompleteUploadSingleParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageFileDaoHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;

/**
 * This class tests functionality within the upload download helper service implementation.
 */
public class UploadDownloadHelperServiceImplTest extends AbstractServiceTest
{
    @Mock
    private AwsHelper awsHelper;

    @Mock
    private BusinessObjectDataDao businessObjectDataDao;

    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private NotificationEventService notificationEventService;

    @Mock
    private S3Dao s3Dao;

    @Mock
    private StorageDaoHelper storageDaoHelper;

    @Mock
    private StorageFileDaoHelper storageFileDaoHelper;

    @Mock
    private StorageHelper storageHelper;

    @Mock
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @InjectMocks
    private UploadDownloadHelperServiceImpl uploadDownloadHelperService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testPrepareForFileMove()
    {
        // Create an object key.
        String objectKey = UUID_VALUE;

        // Create a complete upload single parameters DTO.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto = new CompleteUploadSingleParamsDto();

        // Create a business object data status entity.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.UPLOADING);

        // Create a source storage entity.
        StorageEntity sourceStorageEntity = new StorageEntity();

        // Create a source business object data key.
        BusinessObjectDataKey sourceBusinessObjectDataKey = new BusinessObjectDataKey();
        sourceBusinessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);

        // Create a source business object data entity.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = new BusinessObjectDataEntity();
        sourceBusinessObjectDataEntity.setId(ID);
        sourceBusinessObjectDataEntity.setPartitionValue(objectKey);
        sourceBusinessObjectDataEntity.setStatus(businessObjectDataStatusEntity);

        // Create a list of source storage files.
        List<StorageFileEntity> sourceStorageFileEntities = new ArrayList<>();

        // Create a source storage unit.
        StorageUnitEntity sourceStorageUnitEntity = new StorageUnitEntity();
        sourceStorageUnitEntity.setBusinessObjectData(sourceBusinessObjectDataEntity);
        sourceStorageUnitEntity.setStorage(sourceStorageEntity);
        sourceStorageUnitEntity.setStorageFiles(sourceStorageFileEntities);

        // Create a source storage file entity.
        StorageFileEntity sourceStorageFileEntity = new StorageFileEntity();
        sourceStorageFileEntities.add(sourceStorageFileEntity);
        sourceStorageFileEntity.setStorageUnit(sourceStorageUnitEntity);
        sourceStorageFileEntity.setPath(S3_KEY);
        sourceStorageFileEntity.setFileSizeBytes(FILE_SIZE);

        // Create a target storage entity.
        StorageEntity targetStorageEntity = new StorageEntity();

        // Create a target business object data key.
        BusinessObjectDataKey targetBusinessObjectDataKey = new BusinessObjectDataKey();
        targetBusinessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE_2);

        // Create a list of source storage files.
        List<StorageFileEntity> targetStorageFileEntities = new ArrayList<>();

        // Create a target storage unit.
        StorageUnitEntity targetStorageUnitEntity = new StorageUnitEntity();
        targetStorageUnitEntity.setStorage(targetStorageEntity);
        targetStorageUnitEntity.setStorageFiles(targetStorageFileEntities);

        // Create a source storage file entity.
        StorageFileEntity targetStorageFileEntity = new StorageFileEntity();
        targetStorageFileEntities.add(targetStorageFileEntity);
        targetStorageFileEntity.setPath(S3_KEY_2);

        // Create a target business object data entity.
        BusinessObjectDataEntity targetBusinessObjectDataEntity = new BusinessObjectDataEntity();
        targetBusinessObjectDataEntity.setId(ID_2);
        targetBusinessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        targetBusinessObjectDataEntity.setStorageUnits(Collections.singletonList(targetStorageUnitEntity));

        // Create an AWS parameters DTO.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1);

        // Mock the external calls.
        when(storageFileDaoHelper.getStorageFileEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, objectKey)).thenReturn(sourceStorageFileEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity)).thenReturn(sourceBusinessObjectDataKey);
        when(businessObjectDataDao.getBusinessObjectDataEntitiesByPartitionValue(objectKey))
            .thenReturn(Arrays.asList(sourceBusinessObjectDataEntity, targetBusinessObjectDataEntity));
        when(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity)).thenReturn(targetBusinessObjectDataKey);
        when(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE)).thenReturn(sourceStorageEntity);
        when(storageHelper.getStorageBucketName(sourceStorageEntity)).thenReturn(S3_BUCKET_NAME);
        when(storageUnitDaoHelper.getStorageUnitEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, sourceBusinessObjectDataEntity))
            .thenReturn(sourceStorageUnitEntity);
        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(storageHelper.getStorageBucketName(targetStorageEntity)).thenReturn(S3_BUCKET_NAME_2);
        when(storageHelper.getStorageKmsKeyId(targetStorageEntity)).thenReturn(AWS_KMS_KEY_ID);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT)).thenReturn(S3_ENDPOINT);

        // Call the method under test.
        uploadDownloadHelperService.prepareForFileMoveImpl(objectKey, completeUploadSingleParamsDto);

        // Verify the external calls.
        verify(storageFileDaoHelper).getStorageFileEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, objectKey);
        verify(storageFileDaoHelper).getStorageFileEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, objectKey);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(sourceBusinessObjectDataEntity);
        verify(businessObjectDataDao).getBusinessObjectDataEntitiesByPartitionValue(objectKey);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(targetBusinessObjectDataEntity);
        verify(storageDaoHelper).getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);
        verify(storageHelper).getStorageBucketName(sourceStorageEntity);
        verify(storageUnitDaoHelper).getStorageUnitEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, sourceBusinessObjectDataEntity);
        verify(awsHelper, times(2)).getAwsParamsDto();
        verify(s3Dao).validateS3File(any(S3FileTransferRequestParamsDto.class), eq(FILE_SIZE));
        verify(storageHelper).getStorageBucketName(targetStorageEntity);
        verify(storageHelper).getStorageKmsKeyId(targetStorageEntity);
        verify(s3Dao).s3FileExists(any(S3FileTransferRequestParamsDto.class));
        verify(businessObjectDataDaoHelper).updateBusinessObjectDataStatus(sourceBusinessObjectDataEntity, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        verify(businessObjectDataDaoHelper).updateBusinessObjectDataStatus(targetBusinessObjectDataEntity, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        verify(notificationEventService)
            .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG, sourceBusinessObjectDataKey,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, BusinessObjectDataStatusEntity.UPLOADING);
        verify(notificationEventService)
            .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG, targetBusinessObjectDataKey,
                BusinessObjectDataStatusEntity.RE_ENCRYPTING, BusinessObjectDataStatusEntity.UPLOADING);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(new CompleteUploadSingleParamsDto(sourceBusinessObjectDataKey, S3_BUCKET_NAME, S3_KEY, BusinessObjectDataStatusEntity.UPLOADING,
            BusinessObjectDataStatusEntity.RE_ENCRYPTING, targetBusinessObjectDataKey, S3_BUCKET_NAME_2, S3_KEY_2, BusinessObjectDataStatusEntity.UPLOADING,
            BusinessObjectDataStatusEntity.RE_ENCRYPTING, AWS_KMS_KEY_ID, awsParamsDto, S3_ENDPOINT), completeUploadSingleParamsDto);
    }

    @Test
    public void testPrepareForFileMoveImplOptimisticLockException()
    {
        // Create an object key.
        String objectKey = UUID_VALUE;

        // Create a complete upload single parameters DTO.
        CompleteUploadSingleParamsDto completeUploadSingleParamsDto = new CompleteUploadSingleParamsDto();

        // Create a business object data status entity.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.UPLOADING);

        // Create a source storage entity.
        StorageEntity sourceStorageEntity = new StorageEntity();

        // Create a source business object data key.
        BusinessObjectDataKey sourceBusinessObjectDataKey = new BusinessObjectDataKey();
        sourceBusinessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);

        // Create a source business object data entity.
        BusinessObjectDataEntity sourceBusinessObjectDataEntity = new BusinessObjectDataEntity();
        sourceBusinessObjectDataEntity.setId(ID);
        sourceBusinessObjectDataEntity.setPartitionValue(objectKey);
        sourceBusinessObjectDataEntity.setStatus(businessObjectDataStatusEntity);

        // Create a list of source storage files.
        List<StorageFileEntity> sourceStorageFileEntities = new ArrayList<>();

        // Create a source storage unit.
        StorageUnitEntity sourceStorageUnitEntity = new StorageUnitEntity();
        sourceStorageUnitEntity.setBusinessObjectData(sourceBusinessObjectDataEntity);
        sourceStorageUnitEntity.setStorage(sourceStorageEntity);
        sourceStorageUnitEntity.setStorageFiles(sourceStorageFileEntities);

        // Create a source storage file entity.
        StorageFileEntity sourceStorageFileEntity = new StorageFileEntity();
        sourceStorageFileEntities.add(sourceStorageFileEntity);
        sourceStorageFileEntity.setStorageUnit(sourceStorageUnitEntity);
        sourceStorageFileEntity.setPath(S3_KEY);
        sourceStorageFileEntity.setFileSizeBytes(FILE_SIZE);

        // Create a target storage entity.
        StorageEntity targetStorageEntity = new StorageEntity();

        // Create a target business object data key.
        BusinessObjectDataKey targetBusinessObjectDataKey = new BusinessObjectDataKey();
        targetBusinessObjectDataKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE_2);

        // Create a list of source storage files.
        List<StorageFileEntity> targetStorageFileEntities = new ArrayList<>();

        // Create a target storage unit.
        StorageUnitEntity targetStorageUnitEntity = new StorageUnitEntity();
        targetStorageUnitEntity.setStorage(targetStorageEntity);
        targetStorageUnitEntity.setStorageFiles(targetStorageFileEntities);

        // Create a source storage file entity.
        StorageFileEntity targetStorageFileEntity = new StorageFileEntity();
        targetStorageFileEntities.add(targetStorageFileEntity);
        targetStorageFileEntity.setPath(S3_KEY_2);

        // Create a target business object data entity.
        BusinessObjectDataEntity targetBusinessObjectDataEntity = new BusinessObjectDataEntity();
        targetBusinessObjectDataEntity.setId(ID_2);
        targetBusinessObjectDataEntity.setStatus(businessObjectDataStatusEntity);
        targetBusinessObjectDataEntity.setStorageUnits(Collections.singletonList(targetStorageUnitEntity));

        // Create an AWS parameters DTO.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1);

        // Mock the external calls.
        when(storageFileDaoHelper.getStorageFileEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, objectKey)).thenReturn(sourceStorageFileEntity);
        when(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity)).thenReturn(sourceBusinessObjectDataKey);
        when(businessObjectDataDao.getBusinessObjectDataEntitiesByPartitionValue(objectKey))
            .thenReturn(Arrays.asList(sourceBusinessObjectDataEntity, targetBusinessObjectDataEntity));
        when(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity)).thenReturn(targetBusinessObjectDataKey);
        when(storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE)).thenReturn(sourceStorageEntity);
        when(storageHelper.getStorageBucketName(sourceStorageEntity)).thenReturn(S3_BUCKET_NAME);
        when(storageUnitDaoHelper.getStorageUnitEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, sourceBusinessObjectDataEntity))
            .thenReturn(sourceStorageUnitEntity);
        when(awsHelper.getAwsParamsDto()).thenReturn(awsParamsDto);
        when(storageHelper.getStorageBucketName(targetStorageEntity)).thenReturn(S3_BUCKET_NAME_2);
        when(storageHelper.getStorageKmsKeyId(targetStorageEntity)).thenReturn(AWS_KMS_KEY_ID);
        doThrow(new OptimisticLockException(ERROR_MESSAGE)).when(businessObjectDataDaoHelper)
            .updateBusinessObjectDataStatus(sourceBusinessObjectDataEntity, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        when(jsonHelper.objectToJson(sourceBusinessObjectDataKey)).thenReturn(BUSINESS_OBJECT_DATA_KEY_AS_STRING);
        when(jsonHelper.objectToJson(targetBusinessObjectDataKey)).thenReturn(BUSINESS_OBJECT_DATA_KEY_AS_STRING_2);
        when(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT)).thenReturn(S3_ENDPOINT);

        // Try to call the method under test.
        try
        {
            uploadDownloadHelperService.prepareForFileMoveImpl(objectKey, completeUploadSingleParamsDto);
        }
        catch (OptimisticLockException e)
        {
            assertEquals(String.format("Ignoring S3 notification due to an optimistic lock exception caused by duplicate S3 event notifications. " +
                    "sourceBusinessObjectDataKey=%s targetBusinessObjectDataKey=%s", BUSINESS_OBJECT_DATA_KEY_AS_STRING, BUSINESS_OBJECT_DATA_KEY_AS_STRING_2),
                e.getMessage());
        }

        // Verify the external calls.
        verify(storageFileDaoHelper).getStorageFileEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, objectKey);
        verify(storageFileDaoHelper).getStorageFileEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, objectKey);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(sourceBusinessObjectDataEntity);
        verify(businessObjectDataDao).getBusinessObjectDataEntitiesByPartitionValue(objectKey);
        verify(businessObjectDataHelper).getBusinessObjectDataKey(targetBusinessObjectDataEntity);
        verify(storageDaoHelper).getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);
        verify(storageHelper).getStorageBucketName(sourceStorageEntity);
        verify(storageUnitDaoHelper).getStorageUnitEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, sourceBusinessObjectDataEntity);
        verify(awsHelper, times(2)).getAwsParamsDto();
        verify(s3Dao).validateS3File(any(S3FileTransferRequestParamsDto.class), eq(FILE_SIZE));
        verify(storageHelper).getStorageBucketName(targetStorageEntity);
        verify(storageHelper).getStorageKmsKeyId(targetStorageEntity);
        verify(s3Dao).s3FileExists(any(S3FileTransferRequestParamsDto.class));
        verify(businessObjectDataDaoHelper).updateBusinessObjectDataStatus(sourceBusinessObjectDataEntity, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        verify(jsonHelper).objectToJson(sourceBusinessObjectDataKey);
        verify(jsonHelper).objectToJson(targetBusinessObjectDataKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(
            new CompleteUploadSingleParamsDto(sourceBusinessObjectDataKey, S3_BUCKET_NAME, S3_KEY, BusinessObjectDataStatusEntity.UPLOADING, NO_BDATA_STATUS,
                targetBusinessObjectDataKey, S3_BUCKET_NAME_2, S3_KEY_2, BusinessObjectDataStatusEntity.UPLOADING, NO_BDATA_STATUS, AWS_KMS_KEY_ID,
                awsParamsDto, S3_ENDPOINT), completeUploadSingleParamsDto);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(awsHelper, businessObjectDataDao, businessObjectDataDaoHelper, businessObjectDataHelper, jsonHelper, notificationEventService,
            s3Dao, storageDaoHelper, storageFileDaoHelper, storageHelper, storageUnitDaoHelper);
    }
}
