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
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.ExpireRestoredBusinessObjectDataHelperService;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageFileDaoHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;

/**
 * An implementation of the helper service class for the business object data finalize restore functionality.
 */
@Service
public class ExpireRestoredBusinessObjectDataHelperServiceImpl implements ExpireRestoredBusinessObjectDataHelperService
{
    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private StorageFileDaoHelper storageFileDaoHelper;

    @Autowired
    private StorageFileHelper storageFileHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StorageUnitDaoHelper storageUnitDaoHelper;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @PublishNotificationMessages
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void completeStorageUnitExpiration(BusinessObjectDataRestoreDto businessObjectDataRestoreDto)
    {
        completeStorageUnitExpirationImpl(businessObjectDataRestoreDto);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation executes non-transactionally, suspends the current transaction if one exists.
     */
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void executeS3SpecificSteps(BusinessObjectDataRestoreDto businessObjectDataRestoreDto)
    {
        executeS3SpecificStepsImpl(businessObjectDataRestoreDto);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @PublishNotificationMessages
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataRestoreDto prepareToExpireStorageUnit(BusinessObjectDataStorageUnitKey storageUnitKey)
    {
        return prepareToExpireStorageUnitImpl(storageUnitKey);
    }

    /**
     * Completes the expiration of a storage unit.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to expire business object data
     */
    protected void completeStorageUnitExpirationImpl(BusinessObjectDataRestoreDto businessObjectDataRestoreDto)
    {
        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataRestoreDto.getBusinessObjectDataKey());

        // Retrieve the storage unit and ensure it exists.
        StorageUnitEntity storageUnitEntity =
            storageUnitDaoHelper.getStorageUnitEntity(businessObjectDataRestoreDto.getStorageName(), businessObjectDataEntity);

        // Update the storage unit status.
        String oldStorageUnitStatus = storageUnitEntity.getStatus().getCode();
        String reason = StorageUnitStatusEntity.ARCHIVED;
        storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.ARCHIVED, reason);

        // Update the new and old storage unit status values in the DTO.
        businessObjectDataRestoreDto.setNewStorageUnitStatus(storageUnitEntity.getStatus().getCode());
        businessObjectDataRestoreDto.setOldStorageUnitStatus(oldStorageUnitStatus);
    }

    /**
     * Executes S3 specific steps required to expire business object data.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to expire business object data
     */
    protected void executeS3SpecificStepsImpl(BusinessObjectDataRestoreDto businessObjectDataRestoreDto)
    {
        // Create an S3 file transfer parameters DTO to access the S3 bucket.
        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = storageHelper.getS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3Endpoint(businessObjectDataRestoreDto.getS3Endpoint());
        s3FileTransferRequestParamsDto.setS3BucketName(businessObjectDataRestoreDto.getS3BucketName());
        s3FileTransferRequestParamsDto.setS3KeyPrefix(StringUtils.appendIfMissing(businessObjectDataRestoreDto.getS3KeyPrefix(), "/"));

        // Get a list of S3 files matching the S3 key prefix. When listing S3 files, we ignore 0 byte objects that represent S3 directories.
        List<S3ObjectSummary> actualS3Files = s3Service.listDirectory(s3FileTransferRequestParamsDto, true);

        // Validate existence and file size of the S3 files.
        storageFileHelper
            .validateRegisteredS3Files(businessObjectDataRestoreDto.getStorageFiles(), actualS3Files, businessObjectDataRestoreDto.getStorageName(),
                businessObjectDataRestoreDto.getBusinessObjectDataKey());

        // Build a list of files to expire by selection only objects that have Glacier storage class.
        List<S3ObjectSummary> s3Files = new ArrayList<>();
        for (S3ObjectSummary s3ObjectSummary : actualS3Files)
        {
            if (StorageClass.Glacier.toString().equals(s3ObjectSummary.getStorageClass()) ||
                StorageClass.DeepArchive.toString().equals(s3ObjectSummary.getStorageClass()))
            {
                s3Files.add(s3ObjectSummary);
            }
        }

        // Set a list of files to expire.
        s3FileTransferRequestParamsDto.setFiles(storageFileHelper.getFiles(storageFileHelper.createStorageFilesFromS3ObjectSummaries(s3Files)));

        // To expire the restored S3 objects, initiate restore requests with expiration set to 1 day.
        s3Service.restoreObjects(s3FileTransferRequestParamsDto, 1, null);
    }

    /**
     * Retrieves a storage unit for the business object data in the specified storage and validates it.
     *
     * @param storageName the storage name
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the storage unit entity
     */
    protected StorageUnitEntity getStorageUnit(String storageName, BusinessObjectDataEntity businessObjectDataEntity)
    {
        // Get the storage unit and make sure it exists.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(storageName, businessObjectDataEntity);

        // Get the storage unit status.
        String storageUnitStatus = storageUnitEntity.getStatus().getCode();

        // Validate that S3 storage unit is in RESTORED state.
        if (!StorageUnitStatusEntity.RESTORED.equals(storageUnitStatus))
        {
            throw new IllegalArgumentException(String
                .format("S3 storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. Business object data: {%s}", storageName,
                    StorageUnitStatusEntity.RESTORED, storageUnitStatus,
                    businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        return storageUnitEntity;
    }

    /**
     * Prepares for the business object data expiration by validating the S3 storage unit along with other related database entities. The method also creates
     * and returns a DTO that contains parameters needed to expire business object data.
     *
     * @param storageUnitKey the storage unit key
     *
     * @return the DTO that holds various parameters required to expire business object data
     */
    protected BusinessObjectDataRestoreDto prepareToExpireStorageUnitImpl(BusinessObjectDataStorageUnitKey storageUnitKey)
    {
        // Get the storage unit name.
        String storageName = storageUnitKey.getStorageName();

        // Get a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.createBusinessObjectDataKeyFromStorageUnitKey(storageUnitKey);

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Retrieve the storage unit and validate it.
        StorageUnitEntity storageUnitEntity = getStorageUnit(storageName, businessObjectDataEntity);

        // Validate that S3 storage has S3 bucket name configured. Please note that since S3 bucket name attribute value is required we pass a "true" flag.
        String s3BucketName = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageUnitEntity.getStorage(),
                true);

        // Get storage specific S3 key prefix for this business object data.
        String s3KeyPrefix =
            s3KeyPrefixHelper.buildS3KeyPrefix(storageUnitEntity.getStorage(), businessObjectDataEntity.getBusinessObjectFormat(), businessObjectDataKey);

        // Retrieve and validate storage files registered with the storage unit.
        List<StorageFile> storageFiles = storageFileHelper.getAndValidateStorageFiles(storageUnitEntity, s3KeyPrefix, storageName, businessObjectDataKey);

        // Validate that this storage does not have any other registered storage files that
        // start with the S3 key prefix, but belong to other business object data instances.
        storageFileDaoHelper.validateStorageFilesCount(storageName, businessObjectDataKey, s3KeyPrefix, storageFiles.size());

        // Update the storage unit status.
        String oldStorageUnitStatus = storageUnitEntity.getStatus().getCode();
        String reason = StorageUnitStatusEntity.EXPIRING;
        storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.EXPIRING, reason);

        // Build a DTO with parameters required to expire business object data.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto = new BusinessObjectDataRestoreDto();
        businessObjectDataRestoreDto.setBusinessObjectDataKey(businessObjectDataKey);
        businessObjectDataRestoreDto.setNewStorageUnitStatus(storageUnitEntity.getStatus().getCode());
        businessObjectDataRestoreDto.setOldStorageUnitStatus(oldStorageUnitStatus);
        businessObjectDataRestoreDto.setS3BucketName(s3BucketName);
        businessObjectDataRestoreDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
        businessObjectDataRestoreDto.setS3KeyPrefix(s3KeyPrefix);
        businessObjectDataRestoreDto.setStorageFiles(storageFiles);
        businessObjectDataRestoreDto.setStorageName(storageName);

        // Return the business object data restore parameters DTO.
        return businessObjectDataRestoreDto;
    }
}
