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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.annotation.PublishJmsMessages;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.CompleteUploadSingleParamsDto;
import org.finra.herd.model.dto.S3FileCopyRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.UploadDownloadHelperService;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageFileDaoHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;

/**
 * A helper service class for UploadDownloadService.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class UploadDownloadHelperServiceImpl implements UploadDownloadHelperService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDownloadHelperServiceImpl.class);

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private StorageFileDaoHelper storageFileDaoHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private JsonHelper jsonHelper;

    /**
     * The @Lazy annotation below is added to address the following BeanCreationException: - Error creating bean with name 'notificationEventServiceImpl': Bean
     * with name 'notificationEventServiceImpl' has been injected into other beans [fileUploadCleanupServiceImpl] in its raw version as part of a circular
     * reference, but has eventually been wrapped. This means that said other beans do not use the final version of the bean. This is often the result of
     * over-eager type matching - consider using 'getBeanNamesOfType' with the 'allowEagerInit' flag turned off, for example.
     */
    @Autowired
    @Lazy
    private NotificationEventService notificationEventService;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @PublishJmsMessages
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void prepareForFileMove(String objectKey, CompleteUploadSingleParamsDto completeUploadSingleParamsDto)
    {
        prepareForFileMoveImpl(objectKey, completeUploadSingleParamsDto);
    }

    /**
     * Prepares to move an S3 file from the source bucket to the target bucket. On success, both the target and source business object data statuses are set to
     * "RE-ENCRYPTING" and the DTO is updated accordingly.
     *
     * @param objectKey the object key (i.e. filename)
     * @param completeUploadSingleParamsDto the DTO to be initialized with parameters required for complete upload single message processing
     */
    protected void prepareForFileMoveImpl(String objectKey, CompleteUploadSingleParamsDto completeUploadSingleParamsDto)
    {
        try
        {
            // Obtain the source business object data entity.
            BusinessObjectDataEntity sourceBusinessObjectDataEntity =
                storageFileDaoHelper.getStorageFileEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, objectKey).getStorageUnit().getBusinessObjectData();

            // Get the status and key of the source business object data entity.
            completeUploadSingleParamsDto.setSourceOldStatus(sourceBusinessObjectDataEntity.getStatus().getCode());
            completeUploadSingleParamsDto.setSourceBusinessObjectDataKey(businessObjectDataHelper.getBusinessObjectDataKey(sourceBusinessObjectDataEntity));

            // Find the target business object data by the source business object data's partition value, which should have been an UUID.
            // This is assuming that the target has the same partition value as the source, and that there exist one and only one target
            // business object data for this UUID.
            BusinessObjectDataEntity targetBusinessObjectDataEntity = getTargetBusinessObjectDataEntity(sourceBusinessObjectDataEntity);

            // Get the status and key of the target business object data entity.
            completeUploadSingleParamsDto.setTargetOldStatus(targetBusinessObjectDataEntity.getStatus().getCode());
            completeUploadSingleParamsDto.setTargetBusinessObjectDataKey(businessObjectDataHelper.getBusinessObjectDataKey(targetBusinessObjectDataEntity));

            // Verify that both source and target business object data have "UPLOADING" status. If not, log a message and exit from the method.
            // This check effectively discards any duplicate SQS messages coming from S3 for the same uploaded file.
            for (BusinessObjectDataEntity businessObjectDataEntity : Arrays.asList(sourceBusinessObjectDataEntity, targetBusinessObjectDataEntity))
            {
                if (!BusinessObjectDataStatusEntity.UPLOADING.equals(businessObjectDataEntity.getStatus().getCode()))
                {
                    LOGGER.info("Ignoring S3 notification since business object data status \"{}\" does not match the expected status \"{}\". " +
                        "businessObjectDataKey={}", businessObjectDataEntity.getStatus().getCode(), BusinessObjectDataStatusEntity.UPLOADING,
                        jsonHelper.objectToJson(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)));

                    // Exit from the method without setting the new status values in the completeUploadSingleParamsDto to "RE-ENCRYPTING".
                    // Please note that not having source and target new status values set to "RE-ENCRYPTING" will make the caller
                    // method skip the rest of the steps required to complete the upload single message processing.
                    return;
                }
            }

            // Get the S3 managed "loading dock" storage entity and make sure it exists.
            StorageEntity s3ManagedLoadingDockStorageEntity = storageDaoHelper.getStorageEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE);

            // Get bucket name for S3 managed "loading dock" storage. Please note that this attribute value is required.
            completeUploadSingleParamsDto.setSourceBucketName(storageHelper.getStorageBucketName(s3ManagedLoadingDockStorageEntity));

            // Get the storage unit entity for this business object data in the S3 managed "loading dock" storage and make sure it exists.
            StorageUnitEntity sourceStorageUnitEntity =
                storageUnitDaoHelper.getStorageUnitEntity(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, sourceBusinessObjectDataEntity);

            // Get the storage file entity.
            StorageFileEntity sourceStorageFileEntity = IterableUtils.get(sourceStorageUnitEntity.getStorageFiles(), 0);

            // Get the source storage file path.
            completeUploadSingleParamsDto.setSourceFilePath(sourceStorageFileEntity.getPath());

            // Get the AWS parameters.
            AwsParamsDto awsParamsDto = awsHelper.getAwsParamsDto();
            completeUploadSingleParamsDto.setAwsParams(awsParamsDto);

            // Validate the source S3 file.
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
                S3FileTransferRequestParamsDto.builder().s3BucketName(completeUploadSingleParamsDto.getSourceBucketName())
                    .s3KeyPrefix(completeUploadSingleParamsDto.getSourceFilePath()).httpProxyHost(awsParamsDto.getHttpProxyHost())
                    .httpProxyPort(awsParamsDto.getHttpProxyPort()).build();
            s3Dao.validateS3File(s3FileTransferRequestParamsDto, sourceStorageFileEntity.getFileSizeBytes());

            // Get the S3 managed "external" storage entity and make sure it exists.
            StorageEntity s3ManagedExternalStorageEntity = getUniqueStorage(targetBusinessObjectDataEntity);

            // Get bucket name for S3 managed "external" storage. Please note that this attribute value is required.
            completeUploadSingleParamsDto.setTargetBucketName(storageHelper.getStorageBucketName(s3ManagedExternalStorageEntity));

            // Get AWS KMS External Key ID.
            completeUploadSingleParamsDto.setKmsKeyId(storageHelper.getStorageKmsKeyId(s3ManagedExternalStorageEntity));

            // Make sure the target does not already contain the file.
            completeUploadSingleParamsDto
                .setTargetFilePath(IterableUtils.get(IterableUtils.get(targetBusinessObjectDataEntity.getStorageUnits(), 0).getStorageFiles(), 0).getPath());
            assertS3ObjectKeyDoesNotExist(completeUploadSingleParamsDto.getTargetBucketName(), completeUploadSingleParamsDto.getTargetFilePath());

            // Change the status of the source business object data to RE-ENCRYPTING.
            businessObjectDataDaoHelper.updateBusinessObjectDataStatus(sourceBusinessObjectDataEntity, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
            completeUploadSingleParamsDto.setSourceNewStatus(BusinessObjectDataStatusEntity.RE_ENCRYPTING);

            // Change the status of the target business object data to RE-ENCRYPTING.
            businessObjectDataDaoHelper.updateBusinessObjectDataStatus(targetBusinessObjectDataEntity, BusinessObjectDataStatusEntity.RE_ENCRYPTING);
            completeUploadSingleParamsDto.setTargetNewStatus(BusinessObjectDataStatusEntity.RE_ENCRYPTING);
        }
        catch (RuntimeException e)
        {
            // Update statuses for both the source and target business object data instances.
            completeUploadSingleParamsDto
                .setSourceNewStatus(setAndReturnNewSourceBusinessObjectDataStatusAfterError(completeUploadSingleParamsDto.getSourceBusinessObjectDataKey()));

            // Update statuses for both the source and target business object data instances.
            completeUploadSingleParamsDto
                .setTargetNewStatus(setAndReturnNewTargetBusinessObjectDataStatusAfterError(completeUploadSingleParamsDto.getTargetBusinessObjectDataKey()));

            // Delete the source S3 file. Please note that the method below only logs runtime exceptions without re-throwing them.
            deleteSourceS3ObjectAfterError(completeUploadSingleParamsDto.getSourceBucketName(), completeUploadSingleParamsDto.getSourceFilePath(),
                completeUploadSingleParamsDto.getSourceBusinessObjectDataKey());

            // Log the error.
            LOGGER.error("Failed to process upload single completion request for file. s3Key=\"{}\"", objectKey, e);
        }

        // If a status update occurred for the source business object data, create a business object data notification for this event.
        if (completeUploadSingleParamsDto.getSourceNewStatus() != null)
        {
            notificationEventService.processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG,
                completeUploadSingleParamsDto.getSourceBusinessObjectDataKey(), completeUploadSingleParamsDto.getSourceNewStatus(),
                completeUploadSingleParamsDto.getSourceOldStatus());
        }

        // If a status update occurred for the target business object data, create a business object data notification for this event.
        if (completeUploadSingleParamsDto.getTargetNewStatus() != null)
        {
            notificationEventService.processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG,
                completeUploadSingleParamsDto.getTargetBusinessObjectDataKey(), completeUploadSingleParamsDto.getTargetNewStatus(),
                completeUploadSingleParamsDto.getTargetOldStatus());
        }
    }

    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void performFileMove(CompleteUploadSingleParamsDto completeUploadSingleParamsDto)
    {
        performFileMoveImpl(completeUploadSingleParamsDto);
    }

    /**
     * Moves an S3 file from the source bucket to the target bucket. Updates the target business object data status in the DTO based on the result of S3 copy
     * operation.
     *
     * @param completeUploadSingleParamsDto the DTO that contains complete upload single message parameters
     */
    protected void performFileMoveImpl(CompleteUploadSingleParamsDto completeUploadSingleParamsDto)
    {
        // Create and initialize an S3 file copy request parameters DTO.
        S3FileCopyRequestParamsDto params = new S3FileCopyRequestParamsDto();
        params.setSourceBucketName(completeUploadSingleParamsDto.getSourceBucketName());
        params.setTargetBucketName(completeUploadSingleParamsDto.getTargetBucketName());
        params.setSourceObjectKey(completeUploadSingleParamsDto.getSourceFilePath());
        params.setTargetObjectKey(completeUploadSingleParamsDto.getTargetFilePath());
        params.setKmsKeyId(completeUploadSingleParamsDto.getKmsKeyId());
        params.setHttpProxyHost(completeUploadSingleParamsDto.getAwsParams().getHttpProxyHost());
        params.setHttpProxyPort(completeUploadSingleParamsDto.getAwsParams().getHttpProxyPort());

        String targetStatus;

        try
        {
            // Copy the file from source S3 bucket to target bucket, and mark the target business object data as VALID.
            s3Dao.copyFile(params);

            // Update the status of the target business object data to "VALID".
            targetStatus = BusinessObjectDataStatusEntity.VALID;
        }
        catch (Exception e)
        {
            // Log the error.
            LOGGER.error("Failed to copy the upload single file. s3Key=\"{}\" sourceS3BucketName=\"{}\" targetS3BucketName=\"{}\" " +
                "sourceBusinessObjectDataKey={} targetBusinessObjectDataKey={}", completeUploadSingleParamsDto.getSourceFilePath(),
                completeUploadSingleParamsDto.getSourceBucketName(), completeUploadSingleParamsDto.getTargetBucketName(),
                jsonHelper.objectToJson(completeUploadSingleParamsDto.getSourceBusinessObjectDataKey()),
                jsonHelper.objectToJson(completeUploadSingleParamsDto.getTargetBusinessObjectDataKey()), e);

            // Update the status of the target business object data to "INVALID".
            targetStatus = BusinessObjectDataStatusEntity.INVALID;
        }

        // Update the DTO.
        completeUploadSingleParamsDto.setTargetOldStatus(completeUploadSingleParamsDto.getTargetNewStatus());
        completeUploadSingleParamsDto.setTargetNewStatus(targetStatus);
    }

    @PublishJmsMessages
    @Override
    public void executeFileMoveAfterSteps(CompleteUploadSingleParamsDto completeUploadSingleParamsDto)
    {
        executeFileMoveAfterStepsImpl(completeUploadSingleParamsDto);
        deleteSourceFileFromS3(completeUploadSingleParamsDto);
    }

    /**
     * Executes the steps required to complete the processing of complete upload single message following a successful S3 file move operation. The method also
     * updates the DTO that contains complete upload single message parameters.
     *
     * @param completeUploadSingleParamsDto the DTO that contains complete upload single message parameters
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void executeFileMoveAfterStepsImpl(CompleteUploadSingleParamsDto completeUploadSingleParamsDto)
    {
        try
        {
            // If the new target business object data status is set to "VALID", check again to ensure that the actual business object data
            // status is still "RE-ENCRYPTING", which is the expected value for the old target business object data status. Otherwise leave it alone.
            BusinessObjectDataEntity targetBusinessObjectDataEntity =
                businessObjectDataDaoHelper.getBusinessObjectDataEntity(completeUploadSingleParamsDto.getTargetBusinessObjectDataKey());

            if (completeUploadSingleParamsDto.getTargetNewStatus().equalsIgnoreCase(BusinessObjectDataStatusEntity.VALID) &&
                !targetBusinessObjectDataEntity.getStatus().getCode().equalsIgnoreCase(completeUploadSingleParamsDto.getTargetOldStatus()))
            {
                // Set the new target status to null to avoid the status update.
                completeUploadSingleParamsDto.setTargetNewStatus(null);
            }

            // If specified, update the status of the target business object data.
            if (completeUploadSingleParamsDto.getTargetNewStatus() != null)
            {
                completeUploadSingleParamsDto.setTargetOldStatus(targetBusinessObjectDataEntity.getStatus().getCode());
                businessObjectDataDaoHelper.updateBusinessObjectDataStatus(targetBusinessObjectDataEntity, completeUploadSingleParamsDto.getTargetNewStatus());
            }
        }
        catch (Exception e)
        {
            // Log the error if failed to update the business object data status.
            LOGGER.error("Failed to update target business object data status. newBusinessObjectDataStatus=\"{}\" s3Key=\"{}\" targetBusinessObjectDataKey={}",
                completeUploadSingleParamsDto.getTargetNewStatus(), completeUploadSingleParamsDto.getSourceFilePath(),
                jsonHelper.objectToJson(completeUploadSingleParamsDto.getTargetBusinessObjectDataKey()), e);

            // Could not update target business object data status, so set the new target status to null to reflect that no change happened.
            completeUploadSingleParamsDto.setTargetNewStatus(null);
        }
        
        try
        {
            // Update the status of the source business object data to deleted.
            completeUploadSingleParamsDto.setSourceOldStatus(completeUploadSingleParamsDto.getSourceNewStatus());
            completeUploadSingleParamsDto.setSourceNewStatus(BusinessObjectDataStatusEntity.DELETED);
            businessObjectDataDaoHelper.updateBusinessObjectDataStatus(
                businessObjectDataDaoHelper.getBusinessObjectDataEntity(completeUploadSingleParamsDto.getSourceBusinessObjectDataKey()),
                completeUploadSingleParamsDto.getSourceNewStatus());
        }
        catch (Exception e)
        {
            // Log the error.
            LOGGER.error("Failed to update source business object data status. newBusinessObjectDataStatus=\"{}\" s3Key=\"{}\" sourceBusinessObjectDataKey={}",
                BusinessObjectDataStatusEntity.DELETED, completeUploadSingleParamsDto.getSourceFilePath(),
                jsonHelper.objectToJson(completeUploadSingleParamsDto.getSourceBusinessObjectDataKey()), e);

            // Could not update source business object data status, so set the new source status to null to reflect that no change happened.
            completeUploadSingleParamsDto.setSourceNewStatus(null);
        }

        // If a status update occurred for the source business object data, create a business object data notification for this event.
        if (completeUploadSingleParamsDto.getSourceNewStatus() != null)
        {
            notificationEventService.processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG,
                completeUploadSingleParamsDto.getSourceBusinessObjectDataKey(), completeUploadSingleParamsDto.getSourceNewStatus(),
                completeUploadSingleParamsDto.getSourceOldStatus());
        }

        // If a status update occurred for the target business object data, create a business object data notification for this event.
        if (completeUploadSingleParamsDto.getTargetNewStatus() != null)
        {
            notificationEventService.processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG,
                completeUploadSingleParamsDto.getTargetBusinessObjectDataKey(), completeUploadSingleParamsDto.getTargetNewStatus(),
                completeUploadSingleParamsDto.getTargetOldStatus());
        }
    }

    /**
     * delete the source file from S3
     * 
     * @param completeUploadSingleParamsDto  completeUploadSingleParamsDto
     */
    protected void deleteSourceFileFromS3(CompleteUploadSingleParamsDto completeUploadSingleParamsDto)
    {
        try
        {
            // Delete the source file from S3.
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
                S3FileTransferRequestParamsDto.builder().s3BucketName(completeUploadSingleParamsDto.getSourceBucketName())
                    .s3KeyPrefix(completeUploadSingleParamsDto.getSourceFilePath())
                    .httpProxyHost(completeUploadSingleParamsDto.getAwsParams().getHttpProxyHost())
                    .httpProxyPort(completeUploadSingleParamsDto.getAwsParams().getHttpProxyPort()).build();

            s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
        }
        catch (Exception e)
        {
            // Log the error if failed to delete the file from source S3 bucket.
            LOGGER.error("Failed to delete the upload single file. s3Key=\"{}\" sourceS3BucketName=\"{}\" sourceBusinessObjectDataKey={}",
                completeUploadSingleParamsDto.getSourceFilePath(), completeUploadSingleParamsDto.getSourceBucketName(),
                jsonHelper.objectToJson(completeUploadSingleParamsDto.getSourceBusinessObjectDataKey()), e);
        }
    }

    @PublishJmsMessages
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
    {
        updateBusinessObjectDataStatusImpl(businessObjectDataKey, businessObjectDataStatus);
    }

    /**
     * Implementation of the update business object data status.
     */
    protected void updateBusinessObjectDataStatusImpl(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
    {
        businessObjectDataDaoHelper
            .updateBusinessObjectDataStatus(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey), businessObjectDataStatus);
    }

    @Override
    public void assertS3ObjectKeyDoesNotExist(String bucketName, String key)
    {
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = new S3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(bucketName);
        s3FileTransferRequestParamsDto.setS3KeyPrefix(key);
        AwsParamsDto awsParamsDto = awsHelper.getAwsParamsDto();
        String httpProxyHost = awsParamsDto.getHttpProxyHost();
        s3FileTransferRequestParamsDto.setHttpProxyHost(httpProxyHost);
        Integer httpProxyPort = awsParamsDto.getHttpProxyPort();
        s3FileTransferRequestParamsDto.setHttpProxyPort(httpProxyPort);
        Assert.isTrue(!s3Dao.s3FileExists(s3FileTransferRequestParamsDto),
            String.format("A S3 object already exists in bucket \"%s\" and key \"%s\".", bucketName, key));
    }

    /**
     * Sets and returns the new source business object data status after an error.
     *
     * @param sourceBusinessObjectDataKey the source business object data key
     *
     * @return the new status
     */
    private String setAndReturnNewSourceBusinessObjectDataStatusAfterError(BusinessObjectDataKey sourceBusinessObjectDataKey)
    {
        String newStatus = null;

        if (sourceBusinessObjectDataKey != null)
        {
            // Update the source business object data status to DELETED.
            try
            {
                // Set the source business object data status to DELETED in new transaction as we want
                // to throw the original exception which will mark the transaction to rollback.
                updateBusinessObjectDataStatus(sourceBusinessObjectDataKey, BusinessObjectDataStatusEntity.DELETED);
                newStatus = BusinessObjectDataStatusEntity.DELETED;
            }
            catch (Exception e)
            {
                LOGGER.error("Failed to update source business object data status. newBusinessObjectDataStatus=\"{}\" sourceBusinessObjectDataKey={}",
                    BusinessObjectDataStatusEntity.DELETED, jsonHelper.objectToJson(sourceBusinessObjectDataKey), e);
            }
        }

        return newStatus;
    }

    /**
     * Sets and returns the new target business object data status after an error.
     *
     * @param targetBusinessObjectDataKey the target business object data key
     *
     * @return the new status
     */
    private String setAndReturnNewTargetBusinessObjectDataStatusAfterError(BusinessObjectDataKey targetBusinessObjectDataKey)
    {
        String newStatus = null;

        if (targetBusinessObjectDataKey != null)
        {
            // Update the target business object data status to INVALID.
            try
            {
                // Set the target business object data status to INVALID in new transaction as we want
                // to throw the original exception which will mark the transaction to rollback.
                updateBusinessObjectDataStatus(targetBusinessObjectDataKey, BusinessObjectDataStatusEntity.INVALID);
                newStatus = BusinessObjectDataStatusEntity.INVALID;
            }
            catch (Exception e)
            {
                LOGGER.error("Failed to update target business object data status. newBusinessObjectDataStatus=\"{}\" targetBusinessObjectDataKey={}",
                    BusinessObjectDataStatusEntity.INVALID, jsonHelper.objectToJson(targetBusinessObjectDataKey), e);
            }
        }

        return newStatus;
    }

    /**
     * Deletes a source S3 object based on the given bucket name and file path.
     *
     * @param s3BucketName the S3 bucket name
     * @param storageFilePath the storage file path
     * @param businessObjectDataKey the business object key
     */
    private void deleteSourceS3ObjectAfterError(String s3BucketName, String storageFilePath, BusinessObjectDataKey businessObjectDataKey)
    {
        // Delete the file from S3 if storage file information exists.
        if (!StringUtils.isEmpty(storageFilePath))
        {
            try
            {
                // Delete the source file from S3.
                AwsParamsDto awsParams = awsHelper.getAwsParamsDto();

                S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
                    S3FileTransferRequestParamsDto.builder().s3BucketName(s3BucketName).s3KeyPrefix(storageFilePath).httpProxyHost(awsParams.getHttpProxyHost())
                        .httpProxyPort(awsParams.getHttpProxyPort()).build();

                s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
            }
            catch (Exception e)
            {
                LOGGER.error("Failed to delete source business object data file. s3Key=\"{}\" sourceS3BucketName=\"{}\" sourceBusinessObjectDataKey={}",
                    storageFilePath, s3BucketName, jsonHelper.objectToJson(businessObjectDataKey), e);
            }
        }
    }

    /**
     * Gets a unique storage from the given business object data. The given business object data must have one and only one storage unit with a storage,
     * otherwise this method throws an exception.
     *
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the unique storage entity
     */
    private StorageEntity getUniqueStorage(BusinessObjectDataEntity businessObjectDataEntity)
    {
        Collection<StorageUnitEntity> targetStorageUnits = businessObjectDataEntity.getStorageUnits();
        Assert.notEmpty(targetStorageUnits, "No storage units found for business object data ID \"" + businessObjectDataEntity.getId() + "\".");
        Assert.isTrue(targetStorageUnits.size() == 1,
            "More than 1 storage units found for business object data ID \"" + businessObjectDataEntity.getId() + "\".");
        return IterableUtils.get(targetStorageUnits, 0).getStorage();
    }

    /**
     * Gets the target business object data entity with the given source business object data entity. The target is a business object data which has the same
     * partition value as the source, and is not the source itself. Throws an error if no target is found, or more than 1 business object data other than the
     * source is found with the partition value.
     *
     * @param sourceBusinessObjectDataEntity the source business object data entity
     *
     * @return the target business object data entity
     */
    private BusinessObjectDataEntity getTargetBusinessObjectDataEntity(BusinessObjectDataEntity sourceBusinessObjectDataEntity)
    {
        String uuidPartitionValue = sourceBusinessObjectDataEntity.getPartitionValue();
        List<BusinessObjectDataEntity> targetBusinessObjectDataEntities =
            businessObjectDataDao.getBusinessObjectDataEntitiesByPartitionValue(uuidPartitionValue);

        Assert.notEmpty(targetBusinessObjectDataEntities, "No target business object data found with partition value \"" + uuidPartitionValue + "\".");

        // we check for size 2 because one is the source bdata, the other is the target
        Assert.isTrue(targetBusinessObjectDataEntities.size() == 2,
            "More than 1 target business object data found with partition value \"" + uuidPartitionValue + "\".");

        // Find the bdata which is NOT the source bdata. It should be the target bdata.
        BusinessObjectDataEntity targetBusinessObjectDataEntity = null;
        for (BusinessObjectDataEntity businessObjectDataEntity : targetBusinessObjectDataEntities)
        {
            if (!Objects.equals(businessObjectDataEntity.getId(), sourceBusinessObjectDataEntity.getId()))
            {
                targetBusinessObjectDataEntity = businessObjectDataEntity;
            }
        }

        Assert.notNull(targetBusinessObjectDataEntity, "No target business object data found with partition value \"" + uuidPartitionValue + "\".");

        return targetBusinessObjectDataEntity;
    }
}
