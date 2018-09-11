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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.FileUploadCleanupService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.UploadDownloadHelperService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;

/**
 * The file upload cleanup service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class FileUploadCleanupServiceImpl implements FileUploadCleanupService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadCleanupServiceImpl.class);

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Autowired
    private UploadDownloadHelperService uploadDownloadHelperService;

    /**
     * The @Lazy annotation below is added to address the following BeanCreationException: - Error creating bean with name 'notificationEventServiceImpl': Bean
     * with name 'notificationEventServiceImpl' has been injected into other beans [...] in its raw version as part of a circular reference, but has eventually
     * been wrapped. This means that said other beans do not use the final version of the bean. This is often the result of over-eager type matching - consider
     * using 'getBeanNamesOfType' with the 'allowEagerInit' flag turned off, for example.
     */
    @Autowired
    @Lazy
    private NotificationEventService notificationEventService;

    @Override
    public List<BusinessObjectDataKey> deleteBusinessObjectData(String storageName, int thresholdMinutes)
    {
        LOGGER.info("Deleting dangling business object data from the storage that is older than the threshold... storageName=\"{}\" thresholdMinutes={}",
            storageName, thresholdMinutes);

        // Get the storage entity and make sure it exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageName);

        // Returns a new instance of S3FileTransferRequestParamsDto populated with all parameters, required to access the S3 bucket.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = storageHelper.getS3BucketAccessParams(storageEntity);

        // Get dangling business object data records having storage files associated with the specified storage.
        List<BusinessObjectDataEntity> businessObjectDataEntities = businessObjectDataDao
            .getBusinessObjectDataFromStorageOlderThan(storageEntity, thresholdMinutes,
                Arrays.asList(BusinessObjectDataStatusEntity.UPLOADING, BusinessObjectDataStatusEntity.RE_ENCRYPTING));

        // Build a list of keys for business object data that got marked as DELETED.
        List<BusinessObjectDataKey> resultBusinessObjectDataKeys = new ArrayList<>();

        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities)
        {
            try
            {
                // Get the storage unit. Please note that the storage unit entity must exist,
                // since we selected business object data entities associated with the storage.
                StorageUnitEntity storageUnitEntity =
                    storageUnitDaoHelper.getStorageUnitEntityByBusinessObjectDataAndStorage(businessObjectDataEntity, storageEntity);

                // Validate that none of the storage files (if any) exist in the relative S3 bucket.
                boolean foundExistingS3File = false;
                for (StorageFileEntity storageFileEntity : storageUnitEntity.getStorageFiles())
                {
                    // Check if the actual S3 file does not exist.
                    s3FileTransferRequestParamsDto.setS3KeyPrefix(storageFileEntity.getPath());
                    if (s3Dao.getObjectMetadata(s3FileTransferRequestParamsDto) != null)
                    {
                        foundExistingS3File = true;
                        break;
                    }
                }

                // If not S3 files exist, mark the business object data as DELETED.
                if (!foundExistingS3File)
                {
                    // Update the business object data status.
                    BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);
                    String originalBusinessObjectStatus = businessObjectDataEntity.getStatus().getCode();
                    uploadDownloadHelperService.updateBusinessObjectDataStatus(businessObjectDataKey, BusinessObjectDataStatusEntity.DELETED);

                    // Create business object data notification.
                    notificationEventService
                        .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG,
                            businessObjectDataKey, BusinessObjectDataStatusEntity.DELETED, originalBusinessObjectStatus);

                    // Add the business object data key to the result list.
                    resultBusinessObjectDataKeys.add(businessObjectDataKey);

                    // Log the business object data status change.
                    LOGGER.info("Changed business object data status. " +
                            "oldBusinessObjectDataStatus=\"{}\" newBusinessObjectDataStatus=\"{}\" businessObjectDataKey={}", originalBusinessObjectStatus,
                        BusinessObjectDataStatusEntity.DELETED, jsonHelper.objectToJson(businessObjectDataKey));
                }
            }
            catch (RuntimeException e)
            {
                // Log the exception and continue the processing.
                LOGGER.error("Failed to delete business object data. businessObjectDataKey={}",
                    jsonHelper.objectToJson(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)), e);
            }
        }

        return resultBusinessObjectDataKeys;
    }
}
