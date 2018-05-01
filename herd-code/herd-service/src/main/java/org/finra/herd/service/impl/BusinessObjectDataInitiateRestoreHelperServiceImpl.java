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

import java.sql.Timestamp;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.BusinessObjectDataInitiateRestoreHelperService;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageFileDaoHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;
import org.finra.herd.service.helper.StorageUnitStatusDaoHelper;

/**
 * An implementation of the helper service class for the business object data initiate a restore request functionality.
 */
@Service
public class BusinessObjectDataInitiateRestoreHelperServiceImpl implements BusinessObjectDataInitiateRestoreHelperService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessObjectDataInitiateRestoreHelperServiceImpl.class);

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private JsonHelper jsonHelper;

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
    private StorageUnitDao storageUnitDao;

    @Autowired
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Autowired
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @PublishNotificationMessages
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectData executeInitiateRestoreAfterStep(BusinessObjectDataRestoreDto businessObjectDataRestoreDto)
    {
        return executeInitiateRestoreAfterStepImpl(businessObjectDataRestoreDto);
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
    public BusinessObjectDataRestoreDto prepareToInitiateRestore(BusinessObjectDataKey businessObjectDataKey, Integer expirationInDays)
    {
        return prepareToInitiateRestoreImpl(businessObjectDataKey, expirationInDays);
    }

    /**
     * Executes an after step for the initiation of a business object data restore request.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to perform a business object data restore
     *
     * @return the business object data information
     */
    protected BusinessObjectData executeInitiateRestoreAfterStepImpl(BusinessObjectDataRestoreDto businessObjectDataRestoreDto)
    {
        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataRestoreDto.getBusinessObjectDataKey());

        // On failure, set the storage unit status back to ARCHIVED.
        if (businessObjectDataRestoreDto.getException() != null)
        {
            // Retrieve the storage unit and ensure it exists.
            StorageUnitEntity storageUnitEntity =
                storageUnitDaoHelper.getStorageUnitEntity(businessObjectDataRestoreDto.getStorageName(), businessObjectDataEntity);

            // Retrieve and ensure the ARCHIVED storage unit status entity exists.
            StorageUnitStatusEntity newStorageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.ARCHIVED);

            // Save the old storage unit status value.
            String oldStorageUnitStatus = storageUnitEntity.getStatus().getCode();

            // Update the S3 storage unit status to ARCHIVED.
            String reason = StorageUnitStatusEntity.ARCHIVED;
            storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, newStorageUnitStatusEntity, reason);

            // Update the new and old storage unit status values for the storage unit in the business object data restore DTO.
            businessObjectDataRestoreDto.setNewStorageUnitStatus(newStorageUnitStatusEntity.getCode());
            businessObjectDataRestoreDto.setOldStorageUnitStatus(oldStorageUnitStatus);
        }

        // Create and return the business object data object from the entity.
        BusinessObjectData businessObjectData = businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);

        // Return the business object data information.
        return businessObjectData;
    }

    /**
     * Executes S3 specific steps for the initiation of a business object data restore request. The method also updates the specified DTO.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to perform a business object data restore
     */
    protected void executeS3SpecificStepsImpl(BusinessObjectDataRestoreDto businessObjectDataRestoreDto)
    {
        try
        {
            // Create an S3 file transfer parameters DTO to access the S3 bucket.
            // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = storageHelper.getS3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3BucketName(businessObjectDataRestoreDto.getS3BucketName());
            s3FileTransferRequestParamsDto.setS3Endpoint(businessObjectDataRestoreDto.getS3Endpoint());
            s3FileTransferRequestParamsDto.setS3KeyPrefix(StringUtils.appendIfMissing(businessObjectDataRestoreDto.getS3KeyPrefix(), "/"));

            // Get a list of S3 files matching the S3 key prefix. When listing S3 files, we ignore 0 byte objects that represent S3 directories.
            List<S3ObjectSummary> actualS3Files = s3Service.listDirectory(s3FileTransferRequestParamsDto, true);

            // Validate existence and file size of the S3 files.
            storageFileHelper
                .validateRegisteredS3Files(businessObjectDataRestoreDto.getStorageFiles(), actualS3Files, businessObjectDataRestoreDto.getStorageName(),
                    businessObjectDataRestoreDto.getBusinessObjectDataKey());

            // Validate that all files to be restored are currently archived in Glacier (have Glacier storage class).
            // Fail on any S3 file that does not have Glacier storage class. This can happen when request to restore business object
            // data is posted after business object data archiving transition is executed (relative S3 objects get tagged),
            // but before AWS actually transitions the S3 files to Glacier (changes S3 object storage class to Glacier).
            for (S3ObjectSummary s3ObjectSummary : actualS3Files)
            {
                if (!StringUtils.equals(s3ObjectSummary.getStorageClass(), StorageClass.Glacier.toString()))
                {
                    throw new IllegalArgumentException(String
                        .format("S3 file \"%s\" is not archived (found %s storage class when expecting %s). S3 Bucket Name: \"%s\"", s3ObjectSummary.getKey(),
                            s3ObjectSummary.getStorageClass(), StorageClass.Glacier.toString(), s3FileTransferRequestParamsDto.getS3BucketName()));
                }
            }

            // Set a list of files to restore.
            s3FileTransferRequestParamsDto.setFiles(storageFileHelper.getFiles(storageFileHelper.createStorageFilesFromS3ObjectSummaries(actualS3Files)));

            // Initiate restore requests for the list of objects in the Glacier bucket.
            // TODO: Make "expirationInDays" value configurable with default value set to 99 years (36135 days).
            s3Service.restoreObjects(s3FileTransferRequestParamsDto, 36135);
        }
        catch (RuntimeException e)
        {
            // Log the exception.
            LOGGER.error("Failed to initiate a restore request for the business object data. businessObjectDataKey={}",
                jsonHelper.objectToJson(businessObjectDataRestoreDto.getBusinessObjectDataKey()), e);

            // Update the DTO with the caught exception.
            businessObjectDataRestoreDto.setException(e);
        }
    }

    /**
     * Retrieves storage unit for the business object data. The method validates that there one and only one storage unit for this business object data in
     * "ARCHIVED" state.
     *
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the archived storage unit entity
     */
    protected StorageUnitEntity getStorageUnit(BusinessObjectDataEntity businessObjectDataEntity)
    {
        // Retrieve all S3 storage units for this business object data.
        List<StorageUnitEntity> s3StorageUnitEntities =
            storageUnitDao.getStorageUnitsByStoragePlatformAndBusinessObjectData(StoragePlatformEntity.S3, businessObjectDataEntity);

        // Validate that business object data has at least one S3 storage unit.
        if (CollectionUtils.isEmpty(s3StorageUnitEntities))
        {
            throw new IllegalArgumentException(String.format("Business object data has no S3 storage unit. Business object data: {%s}",
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        // Validate that this business object data has no multiple S3 storage units.
        if (CollectionUtils.size(s3StorageUnitEntities) > 1)
        {
            throw new IllegalArgumentException(String
                .format("Business object data has multiple (%s) %s storage units. Business object data: {%s}", s3StorageUnitEntities.size(),
                    StoragePlatformEntity.S3, businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        // Get the S3 storage unit.
        StorageUnitEntity storageUnitEntity = s3StorageUnitEntities.get(0);

        // Get the storage unit status code.
        String storageUnitStatus = storageUnitEntity.getStatus().getCode();

        // Validate that this business object data has its S3 storage unit in "ARCHIVED" state.
        if (!StorageUnitStatusEntity.ARCHIVED.equals(storageUnitStatus))
        {
            // Get the storage name.
            String storageName = storageUnitEntity.getStorage().getName();

            // Fail with a custom error message if the S3 storage unit is already enabled.
            if (StorageUnitStatusEntity.ENABLED.equals(storageUnitStatus))
            {
                throw new IllegalArgumentException(String
                    .format("Business object data is already available in \"%s\" S3 storage. Business object data: {%s}", storageName,
                        businessObjectDataHelper.businessObjectDataEntityAltKeyToString(storageUnitEntity.getBusinessObjectData())));
            }
            // Fail with a custom error message if this business object data is already marked as being restored.
            else if (StorageUnitStatusEntity.RESTORING.equals(storageUnitStatus))
            {
                throw new IllegalArgumentException(String
                    .format("Business object data is already being restored in \"%s\" S3 storage. Business object data: {%s}", storageName,
                        businessObjectDataHelper.businessObjectDataEntityAltKeyToString(storageUnitEntity.getBusinessObjectData())));
            }
            // Else, fail and report the actual S3 storage unit status.
            else
            {
                throw new IllegalArgumentException(String.format("Business object data is not archived. " +
                        "S3 storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. Business object data: {%s}", storageName,
                    StorageUnitStatusEntity.ARCHIVED, storageUnitStatus,
                    businessObjectDataHelper.businessObjectDataEntityAltKeyToString(storageUnitEntity.getBusinessObjectData())));
            }
        }

        return storageUnitEntity;
    }

    /**
     * Prepares for the business object data initiate a restore request by validating the business object data along with other related database entities. The
     * method also creates and returns a business object data restore DTO.
     *
     * @param businessObjectDataKey the business object data key
     * @param expirationInDays the the time, in days, between when the business object data is restored to the S3 bucket and when it expires
     *
     * @return the DTO that holds various parameters needed to perform a business object data restore
     */
    protected BusinessObjectDataRestoreDto prepareToInitiateRestoreImpl(BusinessObjectDataKey businessObjectDataKey, Integer expirationInDays)
    {
        // Validate and trim the business object data key.
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // If expiration time is not specified, use the configured default value.
        int localExpirationInDays = expirationInDays != null ? expirationInDays :
            herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.BDATA_RESTORE_EXPIRATION_IN_DAYS_DEFAULT);

        // Validate the expiration time.
        Assert.isTrue(localExpirationInDays > 0, "Expiration in days value must be a positive integer.");

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Retrieve and validate a Glacier storage unit for this business object data.
        StorageUnitEntity storageUnitEntity = getStorageUnit(businessObjectDataEntity);

        // Get the storage name.
        String storageName = storageUnitEntity.getStorage().getName();

        // Validate that S3 storage has S3 bucket name configured.
        // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
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

        // Set the expiration time for the restored storage unit.
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        storageUnitEntity.setRestoreExpirationOn(HerdDateUtils.addDays(currentTime, localExpirationInDays));

        // Retrieve and ensure the RESTORING storage unit status entity exists.
        StorageUnitStatusEntity newStorageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.RESTORING);

        // Save the old storage unit status value.
        String oldOriginStorageUnitStatus = storageUnitEntity.getStatus().getCode();

        // Update the S3 storage unit status to RESTORING.
        storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, newStorageUnitStatusEntity, StorageUnitStatusEntity.RESTORING);

        // Build the business object data restore parameters DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto = new BusinessObjectDataRestoreDto();
        businessObjectDataRestoreDto.setBusinessObjectDataKey(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity));
        businessObjectDataRestoreDto.setStorageName(storageName);
        businessObjectDataRestoreDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
        businessObjectDataRestoreDto.setS3BucketName(s3BucketName);
        businessObjectDataRestoreDto.setS3KeyPrefix(s3KeyPrefix);
        businessObjectDataRestoreDto.setStorageFiles(storageFiles);
        businessObjectDataRestoreDto.setNewStorageUnitStatus(newStorageUnitStatusEntity.getCode());
        businessObjectDataRestoreDto.setOldStorageUnitStatus(oldOriginStorageUnitStatus);

        // Return the parameters DTO.
        return businessObjectDataRestoreDto;
    }
}
