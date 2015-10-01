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
package org.finra.dm.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.dm.core.DmDateUtils;
import org.finra.dm.dao.DmDao;
import org.finra.dm.dao.S3Dao;
import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.jpa.StorageFileEntity;
import org.finra.dm.model.jpa.StorageUnitEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.service.FileUploadCleanupService;
import org.finra.dm.service.UploadDownloadHelperService;
import org.finra.dm.service.helper.DmDaoHelper;
import org.finra.dm.service.helper.DmHelper;

/**
 * The file upload cleanup service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class FileUploadCleanupServiceImpl implements FileUploadCleanupService
{
    private static final Logger LOGGER = Logger.getLogger(FileUploadCleanupServiceImpl.class);

    @Autowired
    private DmHelper dmHelper;

    @Autowired
    private DmDao dmDao;

    @Autowired
    private DmDaoHelper dmDaoHelper;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private UploadDownloadHelperService uploadDownloadHelperService;

    /**
     * {@inheritDoc}
     */
    @Override
    public List<BusinessObjectDataKey> deleteBusinessObjectData(String storageName, int thresholdMinutes)
    {
        LOGGER.info(String.format("Deleting dangling business object data in \"%s\" storage that is older than %d minutes...", storageName, thresholdMinutes));

        // Get the storage entity and make sure it exists.
        StorageEntity storageEntity = dmDaoHelper.getStorageEntity(storageName);

        // Returns a new instance of S3FileTransferRequestParamsDto populated with all parameters, required to access the S3 bucket.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = dmDaoHelper.getS3BucketAccessParams(storageEntity);

        // Get dangling business object data records having storage files associated with the specified storage.
        List<BusinessObjectDataEntity> businessObjectDataEntities =
            dmDao.getBusinessObjectDataFromStorageOlderThan(storageName, thresholdMinutes, Arrays.asList(BusinessObjectDataStatusEntity.DELETED));

        // Build a list of keys for business object data that got marked as DELETED.
        List<BusinessObjectDataKey> resultBusinessObjectDataKeys = new ArrayList<>();

        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities)
        {
            try
            {
                // Get the storage unit. Please note that the storage unit entity must exist,
                // since we selected business object data entities associated with the storage.
                StorageUnitEntity storageUnitEntity = dmDaoHelper.getStorageUnitEntity(businessObjectDataEntity, storageName);

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
                    BusinessObjectDataKey businessObjectDataKey = dmDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity);
                    String originalBusinessObjectStatus = businessObjectDataEntity.getStatus().getCode();
                    uploadDownloadHelperService.updateBusinessObjectDataStatus(businessObjectDataKey, BusinessObjectDataStatusEntity.DELETED);

                    // Add the business object data key to the result list.
                    resultBusinessObjectDataKeys.add(businessObjectDataKey);

                    // Log the business object data status change.
                    LOGGER.info(String
                        .format("Changed business object data status from \"%s\" to \"%s\" for business object data {%s}", originalBusinessObjectStatus,
                            BusinessObjectDataStatusEntity.DELETED, dmHelper.businessObjectDataKeyToString(businessObjectDataKey)));
                }
            }
            catch (Exception e)
            {
                // Log the exception.
                LOGGER.error(
                    String.format("Failed to delete business object data {%s}.", dmDaoHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)),
                    e);
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
        StorageEntity storageEntity = dmDaoHelper.getStorageEntity(storageName);

        // Returns a new instance of S3FileTransferRequestParamsDto populated with all parameters, required to access the S3 bucket.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = dmDaoHelper.getS3BucketAccessParams(storageEntity);

        // Get the threshold date indicating which multipart uploads should be aborted.
        Date thresholdDate = DmDateUtils.addMinutes(new Date(), -thresholdMinutes);

        // Call the S3Dao service to perform the cleanup.
        return s3Dao.abortMultipartUploads(s3FileTransferRequestParamsDto, thresholdDate);
    }
}
