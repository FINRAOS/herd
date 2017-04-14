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

import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.StorageFileDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.dto.StoragePolicyTransitionParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.StoragePolicyProcessorHelperService;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StoragePolicyDaoHelper;
import org.finra.herd.service.helper.StoragePolicyHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;

/**
 * An implementation of the helper service class for the storage policy processor service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class StoragePolicyProcessorHelperServiceImpl implements StoragePolicyProcessorHelperService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StoragePolicyProcessorHelperServiceImpl.class);

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
    private StorageFileDao storageFileDao;

    @Autowired
    private StorageFileHelper storageFileHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StoragePolicyDaoHelper storagePolicyDaoHelper;

    @Autowired
    private StoragePolicyHelper storagePolicyHelper;

    @Autowired
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public StoragePolicyTransitionParamsDto initiateStoragePolicyTransition(StoragePolicySelection storagePolicySelection)
    {
        return initiateStoragePolicyTransitionImpl(storagePolicySelection);
    }

    /**
     * Initiates a storage policy transition as per specified storage policy selection.
     *
     * @param storagePolicySelection the storage policy selection message
     *
     * @return the storage policy transition DTO that contains parameters needed to perform a storage policy transition
     */
    protected StoragePolicyTransitionParamsDto initiateStoragePolicyTransitionImpl(StoragePolicySelection storagePolicySelection)
    {
        // Validate and trim the storage policy selection message content.
        validateStoragePolicySelection(storagePolicySelection);

        // Get the business object data and storage policy keys from the storage policy selection message.
        BusinessObjectDataKey businessObjectDataKey = storagePolicySelection.getBusinessObjectDataKey();
        StoragePolicyKey storagePolicyKey = storagePolicySelection.getStoragePolicyKey();
        Integer storagePolicyVersion = storagePolicySelection.getStoragePolicyVersion();

        // Retrieve the business object data entity and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Validate the business object data.
        validateBusinessObjectData(businessObjectDataEntity, businessObjectDataKey);

        // Retrieve the storage policy and ensure it exists.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoHelper.getStoragePolicyEntityByKeyAndVersion(storagePolicyKey, storagePolicyVersion);

        // Get the storage name.
        String storageName = storagePolicyEntity.getStorage().getName();

        // Validate the storage.
        validateStorage(storagePolicyEntity.getStorage(), storagePolicyKey, storagePolicyVersion);

        // Validate that storage policy filter storage has S3 bucket name configured.
        // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
        String s3BucketName = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storagePolicyEntity.getStorage(),
                true);

        // Validate that storage policy transition type is GLACIER.
        Assert.isTrue(StoragePolicyTransitionTypeEntity.GLACIER.equals(storagePolicyEntity.getStoragePolicyTransitionType().getCode()), String
            .format("Storage policy transition type \"%s\" is not supported. Storage policy: {%s}",
                storagePolicyEntity.getStoragePolicyTransitionType().getCode(),
                storagePolicyHelper.storagePolicyKeyAndVersionToString(storagePolicyKey, storagePolicyVersion)));

        // Get the S3 object tag key to be used to tag the objects for archiving.
        String s3ObjectTagKey = configurationHelper.getRequiredProperty(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_TAG_KEY);

        // Get the S3 object tag value to be used to tag S3 objects for archiving to Glacier.
        String s3ObjectTagValue = configurationHelper.getRequiredProperty(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_TAG_VALUE);

        // Get the ARN of the role to assume to tag S3 objects for archiving to Glacier.
        String s3ObjectTaggerRoleArn = configurationHelper.getRequiredProperty(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN);

        // Get the session identifier for the assumed role to be used to tag S3 objects for archiving to Glacier.
        String s3ObjectTaggerRoleSessionName = configurationHelper.getRequiredProperty(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME);

        // Retrieve the storage unit and ensure it exists.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(storageName, businessObjectDataEntity);

        // Validate the storage unit.
        validateStorageUnit(storageUnitEntity, storageName, businessObjectDataKey);

        // Get S3 key prefix for this business object data.
        String s3KeyPrefix = s3KeyPrefixHelper
            .buildS3KeyPrefix(storagePolicyEntity.getStorage(), storageUnitEntity.getBusinessObjectData().getBusinessObjectFormat(), businessObjectDataKey);

        // Retrieve storage files registered with this business object data in the specified storage.
        List<StorageFile> storageFiles = storageFileHelper.createStorageFilesFromEntities(storageUnitEntity.getStorageFiles());

        // Validate that we have storage files registered in the storage.
        Assert.isTrue(!CollectionUtils.isEmpty(storageFiles), String
            .format("Business object data has no storage files registered in \"%s\" storage. Business object data: {%s}", storageName,
                businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Validate storage file paths registered with this business object data in the specified storage.
        storageFileHelper
            .validateStorageFiles(storageFileHelper.getFilePathsFromStorageFiles(storageFiles), s3KeyPrefix, storageUnitEntity.getBusinessObjectData(),
                storageUnitEntity.getStorage().getName());

        // Validate that this storage does not have any other registered storage files that
        // start with the S3 key prefix, but belong to other business object data instances.
        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        Long registeredStorageFileCount = storageFileDao.getStorageFileCount(storageName, StringUtils.appendIfMissing(s3KeyPrefix, "/"));
        if (registeredStorageFileCount != storageFiles.size())
        {
            throw new IllegalStateException(String
                .format("Found %d registered storage file(s) matching business object data S3 key prefix in the storage that is not equal to the number " +
                    "of storage files (%d) registered with the business object data in that storage. " +
                    "Storage: {%s}, s3KeyPrefix {%s}, business object data: {%s}", registeredStorageFileCount, storageFiles.size(), storageName, s3KeyPrefix,
                    businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        // Update the storage unit status.
        String reason = StorageUnitStatusEntity.ARCHIVING;
        String oldStorageUnitStatus = storageUnitEntity.getStatus().getCode();
        storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.ARCHIVING, reason);

        // Build the storage policy transition parameters DTO.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto = new StoragePolicyTransitionParamsDto();
        storagePolicyTransitionParamsDto.setBusinessObjectDataKey(businessObjectDataKey);
        storagePolicyTransitionParamsDto.setStorageName(storageName);
        storagePolicyTransitionParamsDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
        storagePolicyTransitionParamsDto.setS3BucketName(s3BucketName);
        storagePolicyTransitionParamsDto.setS3KeyPrefix(s3KeyPrefix);
        storagePolicyTransitionParamsDto.setNewStorageUnitStatus(storageUnitEntity.getStatus().getCode());
        storagePolicyTransitionParamsDto.setOldStorageUnitStatus(oldStorageUnitStatus);
        storagePolicyTransitionParamsDto.setStorageFiles(storageFiles);
        storagePolicyTransitionParamsDto.setS3ObjectTagKey(s3ObjectTagKey);
        storagePolicyTransitionParamsDto.setS3ObjectTagValue(s3ObjectTagValue);
        storagePolicyTransitionParamsDto.setS3ObjectTaggerRoleArn(s3ObjectTaggerRoleArn);
        storagePolicyTransitionParamsDto.setS3ObjectTaggerRoleSessionName(s3ObjectTaggerRoleSessionName);

        return storagePolicyTransitionParamsDto;
    }

    /**
     * Validates the storage policy selection. This method also trims the request parameters.
     *
     * @param storagePolicySelection the storage policy selection
     */
    private void validateStoragePolicySelection(StoragePolicySelection storagePolicySelection)
    {
        Assert.notNull(storagePolicySelection, "A storage policy selection must be specified.");
        businessObjectDataHelper.validateBusinessObjectDataKey(storagePolicySelection.getBusinessObjectDataKey(), true, true);
        storagePolicyHelper.validateStoragePolicyKey(storagePolicySelection.getStoragePolicyKey());
        Assert.notNull(storagePolicySelection.getStoragePolicyVersion(), "A storage policy version must be specified.");
    }

    /**
     * Validate that business object data status is supported by the storage policy feature.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param businessObjectDataKey the business object data key
     */
    private void validateBusinessObjectData(BusinessObjectDataEntity businessObjectDataEntity, BusinessObjectDataKey businessObjectDataKey)
    {
        Assert.isTrue(StoragePolicySelectorServiceImpl.SUPPORTED_BUSINESS_OBJECT_DATA_STATUSES.contains(businessObjectDataEntity.getStatus().getCode()), String
            .format("Business object data status \"%s\" is not supported by the storage policy feature. Business object data: {%s}",
                businessObjectDataEntity.getStatus().getCode(), businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
    }

    /**
     * Validates the storage.
     *
     * @param storageEntity the storage entity
     * @param storagePolicyKey the storage policy key
     * @param storagePolicyVersion the storage policy version
     */
    private void validateStorage(StorageEntity storageEntity, StoragePolicyKey storagePolicyKey, Integer storagePolicyVersion)
    {
        // Validate that storage platform is S3 for the storage policy filter storage.
        Assert.isTrue(StoragePlatformEntity.S3.equals(storageEntity.getStoragePlatform().getName()), String
            .format("Storage platform for storage policy filter storage with name \"%s\" is not \"%s\". Storage policy: {%s}", storageEntity.getName(),
                StoragePlatformEntity.S3, storagePolicyHelper.storagePolicyKeyAndVersionToString(storagePolicyKey, storagePolicyVersion)));

        // Validate that storage policy filter storage has the S3 path prefix validation enabled.
        if (!storageHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true))
        {
            throw new IllegalStateException(String
                .format("Path prefix validation must be enabled on \"%s\" storage. Storage policy: {%s}", storageEntity.getName(),
                    storagePolicyHelper.storagePolicyKeyAndVersionToString(storagePolicyKey, storagePolicyVersion)));
        }

        // Validate that storage policy filter storage has the S3 file existence validation enabled.
        if (!storageHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), storageEntity,
                false, true))
        {
            throw new IllegalStateException(String
                .format("File existence validation must be enabled on \"%s\" storage. Storage policy: {%s}", storageEntity.getName(),
                    storagePolicyHelper.storagePolicyKeyAndVersionToString(storagePolicyKey, storagePolicyVersion)));
        }
    }

    /**
     * Validates the storage unit.
     *
     * @param storageUnitEntity the storage unit entity, not null
     * @param storageName the storage name
     * @param businessObjectDataKey the business object data key
     */
    private void validateStorageUnit(StorageUnitEntity storageUnitEntity, String storageName, BusinessObjectDataKey businessObjectDataKey)
    {
        Assert.isTrue(StorageUnitStatusEntity.ENABLED.equals(storageUnitEntity.getStatus().getCode()) ||
            StorageUnitStatusEntity.ARCHIVING.equals(storageUnitEntity.getStatus().getCode()), String.format(
            "Storage unit status is \"%s\", but must be \"%s\" or \"%s\" for storage policy transition to proceed. Storage: {%s}, business object data: {%s}",
            storageUnitEntity.getStatus().getCode(), StorageUnitStatusEntity.ENABLED, StorageUnitStatusEntity.ARCHIVING, storageName,
            businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
    }

    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void executeStoragePolicyTransition(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        executeStoragePolicyTransitionImpl(storagePolicyTransitionParamsDto);
    }

    /**
     * Executes a storage policy transition as per specified storage policy selection.
     *
     * @param storagePolicyTransitionParamsDto the storage policy transition DTO that contains parameters needed to perform a storage policy transition
     */
    protected void executeStoragePolicyTransitionImpl(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        // Create an S3 file transfer parameters DTO to access the S3 bucket.
        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = storageHelper.getS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(storagePolicyTransitionParamsDto.getS3BucketName());
        s3FileTransferRequestParamsDto.setS3Endpoint(storagePolicyTransitionParamsDto.getS3Endpoint());
        s3FileTransferRequestParamsDto.setS3KeyPrefix(StringUtils.appendIfMissing(storagePolicyTransitionParamsDto.getS3KeyPrefix(), "/"));

        // Create an S3 file transfer parameters DTO to be used for S3 object tagging operation.
        S3FileTransferRequestParamsDto s3ObjectTaggerParamsDto = storageHelper
            .getS3FileTransferRequestParamsDtoByRole(storagePolicyTransitionParamsDto.getS3ObjectTaggerRoleArn(),
                storagePolicyTransitionParamsDto.getS3ObjectTaggerRoleSessionName());
        s3ObjectTaggerParamsDto.setS3Endpoint(storagePolicyTransitionParamsDto.getS3Endpoint());

        // Get actual S3 files by selecting all S3 keys matching the S3 key prefix form the S3 bucket.
        // When listing S3 files, we ignore 0 byte objects that represent S3 directories.
        List<S3ObjectSummary> actualS3FilesWithoutZeroByteDirectoryMarkers = s3Service.listDirectory(s3FileTransferRequestParamsDto, true);

        // Validate existence of the S3 files.
        storageFileHelper.validateRegisteredS3Files(storagePolicyTransitionParamsDto.getStorageFiles(), actualS3FilesWithoutZeroByteDirectoryMarkers,
            storagePolicyTransitionParamsDto.getStorageName(), storagePolicyTransitionParamsDto.getBusinessObjectDataKey());

        // Get actual S3 files by selecting all S3 keys matching the S3 key prefix form the S3 bucket.
        // This time, we do not ignore 0 byte objects that represent S3 directories.
        List<S3ObjectSummary> actualS3Files = s3Service.listDirectory(s3FileTransferRequestParamsDto, false);

        // Set the list of files to be tagged for archiving.
        s3FileTransferRequestParamsDto.setFiles(storageFileHelper.getFiles(storageFileHelper.createStorageFilesFromS3ObjectSummaries(actualS3Files)));

        // Tag the S3 objects to initiate the archiving.
        s3Service.tagObjects(s3FileTransferRequestParamsDto, s3ObjectTaggerParamsDto,
            new Tag(storagePolicyTransitionParamsDto.getS3ObjectTagKey(), storagePolicyTransitionParamsDto.getS3ObjectTagValue()));

        // Log a list of files tagged in the S3 bucket.
        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("Successfully tagged files in S3 bucket. s3BucketName=\"{}\" s3KeyCount={} s3ObjectTagKey=\"{}\" s3ObjectTagValue=\"{}\"",
                s3FileTransferRequestParamsDto.getS3BucketName(), actualS3Files.size(), storagePolicyTransitionParamsDto.getS3ObjectTagKey(),
                storagePolicyTransitionParamsDto.getS3ObjectTagValue());

            for (S3ObjectSummary s3File : actualS3FilesWithoutZeroByteDirectoryMarkers)
            {
                LOGGER.info("s3Key=\"{}\"", s3File.getKey());
            }
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void completeStoragePolicyTransition(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        completeStoragePolicyTransitionImpl(storagePolicyTransitionParamsDto);
    }

    /**
     * Completes a storage policy transition as per specified storage policy selection.
     *
     * @param storagePolicyTransitionParamsDto the storage policy transition DTO that contains parameters needed to complete a storage policy transition
     */
    protected void completeStoragePolicyTransitionImpl(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        // Get the business object data key.
        BusinessObjectDataKey businessObjectDataKey = storagePolicyTransitionParamsDto.getBusinessObjectDataKey();

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Validate that business object data status is supported by the storage policy feature.
        String businessObjectDataStatus = businessObjectDataEntity.getStatus().getCode();
        Assert.isTrue(StoragePolicySelectorServiceImpl.SUPPORTED_BUSINESS_OBJECT_DATA_STATUSES.contains(businessObjectDataStatus), String
            .format("Business object data status \"%s\" is not supported by the storage policy feature. Business object data: {%s}", businessObjectDataStatus,
                businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Retrieve storage unit and ensure it exists.
        StorageUnitEntity storageUnitEntity =
            storageUnitDaoHelper.getStorageUnitEntity(storagePolicyTransitionParamsDto.getStorageName(), businessObjectDataEntity);

        // Validate that storage unit status is ARCHIVING.
        Assert.isTrue(StorageUnitStatusEntity.ARCHIVING.equals(storageUnitEntity.getStatus().getCode()), String
            .format("Storage unit status is \"%s\", but must be \"%s\" for storage policy transition to proceed. Storage: {%s}, business object data: {%s}",
                storageUnitEntity.getStatus().getCode(), StorageUnitStatusEntity.ARCHIVING, storagePolicyTransitionParamsDto.getStorageName(),
                businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Change the storage unit status to ARCHIVED.
        String reason = StorageUnitStatusEntity.ARCHIVED;
        storagePolicyTransitionParamsDto.setOldStorageUnitStatus(storageUnitEntity.getStatus().getCode());
        storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.ARCHIVED, reason);
        storagePolicyTransitionParamsDto.setNewStorageUnitStatus(storageUnitEntity.getStatus().getCode());
    }
}
