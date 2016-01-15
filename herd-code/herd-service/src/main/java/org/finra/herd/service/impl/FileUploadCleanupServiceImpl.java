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
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
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
import org.finra.herd.service.helper.HerdDaoHelper;
import org.finra.herd.service.helper.HerdHelper;
import org.finra.herd.service.helper.StorageDaoHelper;

/**
 * The file upload cleanup service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class FileUploadCleanupServiceImpl implements FileUploadCleanupService
{
    private static final Logger LOGGER = Logger.getLogger(FileUploadCleanupServiceImpl.class);

    @Autowired
    private HerdHelper herdHelper;

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private HerdDaoHelper herdDaoHelper;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private UploadDownloadHelperService uploadDownloadHelperService;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    /**
     * The @Lazy annotation below is added to address the following BeanCreationException:
     * - Error creating bean with name 'notificationEventServiceImpl': Bean with name 'notificationEventServiceImpl' has been injected
     *   into other beans [fileUploadCleanupServiceImpl] in its raw version as part of a circular reference, but has eventually been wrapped.
     *   This means that said other beans do not use the final version of the bean. This is often the result of over-eager
     *   type matching - consider using 'getBeanNamesOfType' with the 'allowEagerInit' flag turned off, for example.
     */
    @Autowired
    @Lazy
    private NotificationEventService notificationEventService;

    /**
     * {@inheritDoc}
     */
    @Override
    public List<BusinessObjectDataKey> deleteBusinessObjectData(String storageName, int thresholdMinutes)
    {
        LOGGER.info(String.format("Deleting dangling business object data in \"%s\" storage that is older than %d minutes...", storageName, thresholdMinutes));

        // Get the storage entity and make sure it exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageName);

        // Returns a new instance of S3FileTransferRequestParamsDto populated with all parameters, required to access the S3 bucket.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = storageDaoHelper.getS3BucketAccessParams(storageEntity);

        // Get dangling business object data records having storage files associated with the specified storage.
        List<BusinessObjectDataEntity> businessObjectDataEntities =
            herdDao.getBusinessObjectDataFromStorageOlderThan(storageName, thresholdMinutes, Arrays.asList(BusinessObjectDataStatusEntity.DELETED));

        // Build a list of keys for business object data that got marked as DELETED.
        List<BusinessObjectDataKey> resultBusinessObjectDataKeys = new ArrayList<>();

        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities)
        {
            try
            {
                // Get the storage unit. Please note that the storage unit entity must exist,
                // since we selected business object data entities associated with the storage.
                StorageUnitEntity storageUnitEntity = storageDaoHelper.getStorageUnitEntity(businessObjectDataEntity, storageName);

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
                    BusinessObjectDataKey businessObjectDataKey = herdDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity);
                    String originalBusinessObjectStatus = businessObjectDataEntity.getStatus().getCode();
                    uploadDownloadHelperService.updateBusinessObjectDataStatus(businessObjectDataKey, BusinessObjectDataStatusEntity.DELETED);

                    // Create business object data notification.
                    notificationEventService
                        .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG,
                            businessObjectDataKey, BusinessObjectDataStatusEntity.DELETED, originalBusinessObjectStatus);

                    // Add the business object data key to the result list.
                    resultBusinessObjectDataKeys.add(businessObjectDataKey);

                    // Log the business object data status change.
                    LOGGER.info(String
                        .format("Changed business object data status from \"%s\" to \"%s\" for business object data {%s}", originalBusinessObjectStatus,
                            BusinessObjectDataStatusEntity.DELETED, herdHelper.businessObjectDataKeyToString(businessObjectDataKey)));
                }
            }
            catch (Exception e)
            {
                // Log the exception.
                LOGGER.error(String
                    .format("Failed to delete business object data {%s}.", herdDaoHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)), e);
            }
        }

        return resultBusinessObjectDataKeys;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int abortMultipartUploads(String storageName, int thresholdMinutes)
    {
        LOGGER.info(
            String.format("Aborting S3 multipart uploads that were initiated in \"%s\" storage more than %d minutes ago...", storageName, thresholdMinutes));

        // Get the storage entity and make sure it exists.
        StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageName);

        // Returns a new instance of S3FileTransferRequestParamsDto populated with all parameters, required to access the S3 bucket.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = storageDaoHelper.getS3BucketAccessParams(storageEntity);

        // Get the threshold date indicating which multipart uploads should be aborted.
        Date thresholdDate = HerdDateUtils.addMinutes(new Date(), -thresholdMinutes);

        // Call the S3Dao service to perform the cleanup.
        return s3Dao.abortMultipartUploads(s3FileTransferRequestParamsDto, thresholdDate);
    }
}
