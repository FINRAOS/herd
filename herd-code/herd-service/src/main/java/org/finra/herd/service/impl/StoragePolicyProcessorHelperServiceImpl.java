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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.HerdFileUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.StorageFileDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileCopyRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.dto.StoragePolicyTransitionParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
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
import org.finra.herd.service.helper.StorageUnitStatusDaoHelper;

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
    private S3Dao s3Dao;

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
    private StorageUnitDao storageUnitDao;

    @Autowired
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Autowired
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

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

        // Get the source storage name.
        String sourceStorageName = storagePolicyEntity.getStorage().getName();

        // Validate the source storage.
        validateSourceStorage(storagePolicyEntity.getStorage(), storagePolicyKey, storagePolicyVersion);

        // Validate that storage policy filter storage has S3 bucket name configured.
        // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
        String sourceBucketName = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storagePolicyEntity.getStorage(),
                true);

        // Get the destination storage name.
        String destinationStorageName = storagePolicyEntity.getDestinationStorage().getName();

        // Validate the destination storage.
        validateDestinationStorage(storagePolicyEntity.getDestinationStorage(), storagePolicyKey, storagePolicyVersion);

        // Validate that storage policy transition destination storage has S3 bucket name configured - this is a bucket name for the "archive" S3 bucket.
        // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
        String destinationBucketName = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                storagePolicyEntity.getDestinationStorage(), true);

        // Retrieve the source storage unit and ensure it exists.
        StorageUnitEntity sourceStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(sourceStorageName, businessObjectDataEntity);

        // Validate the source storage unit.
        validateSourceStorageUnit(sourceStorageUnitEntity, sourceStorageName, businessObjectDataKey);

        // Try to retrieve the destination storage unit.
        StorageUnitEntity destinationStorageUnitEntity =
            storageUnitDao.getStorageUnitByBusinessObjectDataAndStorageName(businessObjectDataEntity, destinationStorageName);

        // Validate the destination storage unit, if one exists.
        validateDestinationStorageUnit(destinationStorageUnitEntity, destinationStorageName, businessObjectDataKey);

        // Get S3 key prefix for this business object data.
        String sourceS3KeyPrefix = s3KeyPrefixHelper
            .buildS3KeyPrefix(storagePolicyEntity.getStorage(), sourceStorageUnitEntity.getBusinessObjectData().getBusinessObjectFormat(),
                businessObjectDataKey);

        // Retrieve storage files registered with this business object data in the source storage.
        List<StorageFile> storageFiles = storageFileHelper.createStorageFilesFromEntities(sourceStorageUnitEntity.getStorageFiles());

        // Validate that we have storage files registered in the source storage.
        Assert.isTrue(!CollectionUtils.isEmpty(storageFiles), String
            .format("Business object data has no storage files registered in \"%s\" storage. Business object data: {%s}", sourceStorageName,
                businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Validate that the total size of storage files for the business object data is not greater than the threshold value configured in the system.
        // Get the total size in byte of all storage files.
        validateTotalStorageFilesSize(sourceStorageName, businessObjectDataKey, storageFileHelper.getStorageFilesSizeBytes(storageFiles));

        // Validate storage file paths registered with this business object data in the specified storage.
        storageFileHelper.validateStorageFiles(storageFileHelper.getFilePathsFromStorageFiles(storageFiles), sourceS3KeyPrefix,
            sourceStorageUnitEntity.getBusinessObjectData(), sourceStorageUnitEntity.getStorage().getName());

        // Validate that this storage does not have any other registered storage files that
        // start with the S3 key prefix, but belong to other business object data instances.
        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        Long registeredStorageFileCount = storageFileDao.getStorageFileCount(sourceStorageName, StringUtils.appendIfMissing(sourceS3KeyPrefix, "/"));
        if (registeredStorageFileCount != storageFiles.size())
        {
            throw new IllegalStateException(String
                .format("Found %d registered storage file(s) matching business object data S3 key prefix in the storage that is not equal to the number " +
                    "of storage files (%d) registered with the business object data in that storage. " +
                    "Storage: {%s}, s3KeyPrefix {%s}, business object data: {%s}", registeredStorageFileCount, storageFiles.size(), sourceStorageName,
                    sourceS3KeyPrefix, businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        // Retrieve and ensure the ARCHIVING storage unit status entity exists.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.ARCHIVING);

        // If not exists, create the destination storage unit.
        String oldDestinationStorageUnitStatus = null;
        if (destinationStorageUnitEntity == null)
        {
            // Create the destination storage unit.
            destinationStorageUnitEntity = new StorageUnitEntity();
            businessObjectDataEntity.getStorageUnits().add(destinationStorageUnitEntity);
            destinationStorageUnitEntity.setStorage(storagePolicyEntity.getDestinationStorage());
            destinationStorageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
            destinationStorageUnitEntity.setStatus(storageUnitStatusEntity);
            storageUnitDao.saveAndRefresh(destinationStorageUnitEntity);
        }
        else
        {
            oldDestinationStorageUnitStatus = destinationStorageUnitEntity.getStatus().getCode();
        }

        // Set the storage directory path for the destination storage unit.
        // The directory path is constructed  by concatenating the source S3 bucket name with the source S3 key prefix.
        String destinationS3KeyBasePrefix = sourceBucketName;
        destinationStorageUnitEntity.setDirectoryPath(String.format("%s/%s", destinationS3KeyBasePrefix, sourceS3KeyPrefix));

        // Set the source storage unit as a parent for the destination storage unit.
        destinationStorageUnitEntity.setParentStorageUnit(sourceStorageUnitEntity);

        // Update the destination storage unit status. We make this call even for the newly created storage unit,
        // since this call also adds an entry to the storage unit status history table.
        storageUnitDaoHelper.updateStorageUnitStatus(destinationStorageUnitEntity, storageUnitStatusEntity, StorageUnitStatusEntity.ARCHIVING);
        String newDestinationStorageUnitStatus = destinationStorageUnitEntity.getStatus().getCode();

        // Build the storage policy transition parameters DTO.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto = new StoragePolicyTransitionParamsDto();
        storagePolicyTransitionParamsDto.setBusinessObjectDataKey(businessObjectDataKey);
        storagePolicyTransitionParamsDto.setSourceStorageName(sourceStorageName);
        storagePolicyTransitionParamsDto.setSourceBucketName(sourceBucketName);
        storagePolicyTransitionParamsDto.setSourceS3KeyPrefix(sourceS3KeyPrefix);
        storagePolicyTransitionParamsDto.setOldSourceStorageUnitStatus(sourceStorageUnitEntity.getStatus().getCode());
        storagePolicyTransitionParamsDto.setNewSourceStorageUnitStatus(sourceStorageUnitEntity.getStatus().getCode());
        storagePolicyTransitionParamsDto.setSourceStorageFiles(storageFiles);
        storagePolicyTransitionParamsDto.setDestinationStorageName(destinationStorageName);
        storagePolicyTransitionParamsDto.setDestinationBucketName(destinationBucketName);
        storagePolicyTransitionParamsDto.setDestinationS3KeyBasePrefix(destinationS3KeyBasePrefix);
        storagePolicyTransitionParamsDto.setOldDestinationStorageUnitStatus(oldDestinationStorageUnitStatus);
        storagePolicyTransitionParamsDto.setNewDestinationStorageUnitStatus(newDestinationStorageUnitStatus);

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
     * Validates the source storage.
     *
     * @param storageEntity the storage entity
     * @param storagePolicyKey the storage policy key
     * @param storagePolicyVersion the storage policy version
     */
    private void validateSourceStorage(StorageEntity storageEntity, StoragePolicyKey storagePolicyKey, Integer storagePolicyVersion)
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
     * Validates the destination storage.
     *
     * @param storageEntity the destination storage entity
     * @param storagePolicyKey the storage policy key
     * @param storagePolicyVersion the storage policy version
     */
    private void validateDestinationStorage(StorageEntity storageEntity, StoragePolicyKey storagePolicyKey, Integer storagePolicyVersion)
    {
        // Validate that storage platform is GLACIER for the destination storage.
        Assert.isTrue(StoragePlatformEntity.GLACIER.equals(storageEntity.getStoragePlatform().getName()), String
            .format("Storage platform for storage policy transition destination storage with name \"%s\" is not \"%s\". Storage policy: {%s}",
                storageEntity.getName(), StoragePlatformEntity.GLACIER,
                storagePolicyHelper.storagePolicyKeyAndVersionToString(storagePolicyKey, storagePolicyVersion)));
    }

    /**
     * Validates that source storage unit status is ENABLED.
     *
     * @param storageUnitEntity the source storage unit entity, not null
     * @param storageName the source storage name
     * @param businessObjectDataKey the business object data key
     */
    private void validateSourceStorageUnit(StorageUnitEntity storageUnitEntity, String storageName, BusinessObjectDataKey businessObjectDataKey)
    {
        Assert.isTrue(StorageUnitStatusEntity.ENABLED.equals(storageUnitEntity.getStatus().getCode()), String.format(
            "Source storage unit status is \"%s\", but must be \"%s\" for storage policy transition to proceed. Storage: {%s}, business object data: {%s}",
            storageUnitEntity.getStatus().getCode(), StorageUnitStatusEntity.ENABLED, storageName,
            businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
    }

    /**
     * Validate that if destination storage unit exists it is in DISABLED state and has no storage files.
     *
     * @param storageUnitEntity the destination storage unit entity, may be null
     * @param storageName the destination storage name
     * @param businessObjectDataKey the business object data key
     */
    private void validateDestinationStorageUnit(StorageUnitEntity storageUnitEntity, String storageName, BusinessObjectDataKey businessObjectDataKey)
    {
        if (storageUnitEntity != null)
        {
            // Validate that destination storage unit is in DISABLED state.
            if (!StorageUnitStatusEntity.DISABLED.equals(storageUnitEntity.getStatus().getCode()))
            {
                throw new AlreadyExistsException(String
                    .format("Destination storage unit already exists and has \"%s\" status. Storage: {%s}, business object data: {%s}",
                        storageUnitEntity.getStatus().getCode(), storageName, businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
            }

            // Throw an exception if destination storage unit has any storage files.
            if (!CollectionUtils.isEmpty(storageUnitEntity.getStorageFiles()))
            {
                throw new IllegalStateException(String.format(
                    "Destination storage unit already exists and has %d storage file(s), but must have no storage files. " +
                        "Storage: {%s}, business object data: {%s}", storageUnitEntity.getStorageFiles().size(), storageName,
                    businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
            }
        }
    }

    /**
     * Validates that the total size of storage files for the business object data is not greater than the threshold value configured in the system.
     *
     * @param storageName the storage name
     * @param businessObjectDataKey the business object data key
     * @param storageFilesSizeBytes the total size of the storage files in bytes
     */
    private void validateTotalStorageFilesSize(String storageName, BusinessObjectDataKey businessObjectDataKey, long storageFilesSizeBytes)
    {
        // Get the threshold value configured in the system.
        Integer storageFilesSizeThresholdGb =
            configurationHelper.getProperty(ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_SIZE_THRESHOLD_GB, Integer.class);

        // Perform the check.
        if (storageFilesSizeThresholdGb != null)
        {
            long storageFilesSizeThresholdBytes = HerdFileUtils.BYTES_PER_GB * storageFilesSizeThresholdGb;
            Assert.isTrue(storageFilesSizeBytes <= storageFilesSizeThresholdBytes, String.format(
                "Total size of storage files (%d bytes) for business object data in \"%s\" storage is greater " +
                    "than the configured threshold of %d GB (%d bytes) as per \"%s\" configuration entry. Business object data: {%s}", storageFilesSizeBytes,
                storageName, storageFilesSizeThresholdGb, storageFilesSizeThresholdBytes,
                ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_SIZE_THRESHOLD_GB.getKey(),
                businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }
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
        boolean cleanUpDestinationS3BucketOnFailure = false;
        S3FileTransferRequestParamsDto destinationS3FileTransferRequestParamsDto = null;

        try
        {
            // Create an S3 file transfer parameters DTO to access the source S3 bucket.
            // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
            S3FileTransferRequestParamsDto sourceS3FileTransferRequestParamsDto = storageHelper.getS3FileTransferRequestParamsDto();
            sourceS3FileTransferRequestParamsDto.setS3BucketName(storagePolicyTransitionParamsDto.getSourceBucketName());
            sourceS3FileTransferRequestParamsDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
            sourceS3FileTransferRequestParamsDto.setS3KeyPrefix(StringUtils.appendIfMissing(storagePolicyTransitionParamsDto.getSourceS3KeyPrefix(), "/"));

            // Get actual source S3 files by selecting all S3 keys matching the source S3 key prefix form the source S3 bucket.
            // When listing S3 files, we ignore 0 byte objects that represent S3 directories.
            List<S3ObjectSummary> actualSourceS3Files = s3Service.listDirectory(sourceS3FileTransferRequestParamsDto, true);

            // Validate existence and file size of the source S3 files.
            storageFileHelper.validateSourceS3Files(storagePolicyTransitionParamsDto.getSourceStorageFiles(), actualSourceS3Files,
                storagePolicyTransitionParamsDto.getSourceStorageName(), storagePolicyTransitionParamsDto.getBusinessObjectDataKey());

            // Create an S3 file transfer parameters DTO to access the destination S3 bucket.
            // Create the destination S3 key prefix by concatenating the destination S3 key base prefix with the source S3 key prefix.
            // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
            destinationS3FileTransferRequestParamsDto = storageHelper.getS3FileTransferRequestParamsDto();
            destinationS3FileTransferRequestParamsDto.setS3BucketName(storagePolicyTransitionParamsDto.getDestinationBucketName());
            destinationS3FileTransferRequestParamsDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
            destinationS3FileTransferRequestParamsDto.setS3KeyPrefix(String.format("%s/%s", storagePolicyTransitionParamsDto.getDestinationS3KeyBasePrefix(),
                StringUtils.appendIfMissing(storagePolicyTransitionParamsDto.getSourceS3KeyPrefix(), "/")));

            // Check that the destination S3 key prefix is empty.
            // When listing S3 files, by default, we do not ignore 0 byte objects that represent S3 directories.
            if (s3Service.listDirectory(destinationS3FileTransferRequestParamsDto).isEmpty())
            {
                cleanUpDestinationS3BucketOnFailure = true;
            }
            else
            {
                throw new IllegalStateException(String.format("The destination S3 key prefix is not empty. S3 bucket name: {%s}, S3 key prefix: {%s}",
                    destinationS3FileTransferRequestParamsDto.getS3BucketName(), destinationS3FileTransferRequestParamsDto.getS3KeyPrefix()));
            }

            // Copy source S3 files to the destination S3 bucket.
            S3FileCopyRequestParamsDto s3FileCopyRequestParamsDto = storageHelper.getS3FileCopyRequestParamsDto();
            s3FileCopyRequestParamsDto.setSourceBucketName(storagePolicyTransitionParamsDto.getSourceBucketName());
            s3FileCopyRequestParamsDto.setTargetBucketName(storagePolicyTransitionParamsDto.getDestinationBucketName());
            List<StorageFile> expectedDestinationS3Files = new ArrayList<>();
            for (S3ObjectSummary sourceS3File : actualSourceS3Files)
            {
                // Create the destination S3 key by concatenating the destination S3 key base prefix with the source S3 key prefix.
                String destinationS3FilePath = String.format("%s/%s", storagePolicyTransitionParamsDto.getDestinationS3KeyBasePrefix(), sourceS3File.getKey());

                // Update the list of expected destination S3 files. Please note that we do not validate row count information.
                expectedDestinationS3Files.add(new StorageFile(destinationS3FilePath, sourceS3File.getSize(), null));

                // Copy the file from source S3 bucket to target bucket.
                s3FileCopyRequestParamsDto.setSourceObjectKey(sourceS3File.getKey());
                s3FileCopyRequestParamsDto.setTargetObjectKey(destinationS3FilePath);
                try
                {
                    s3Dao.copyFile(s3FileCopyRequestParamsDto);
                }
                catch (Exception e)
                {
                    throw new IllegalStateException(
                        String.format("Failed to copy S3 file. Source storage: {%s}, source S3 bucket name: {%s}, source S3 object key: {%s}, " +
                            "target storage: {%s}, target S3 bucket name: {%s}, target S3 object key: {%s}, " +
                            "business object data: {%s}", storagePolicyTransitionParamsDto.getSourceStorageName(),
                            s3FileCopyRequestParamsDto.getSourceBucketName(), s3FileCopyRequestParamsDto.getSourceObjectKey(),
                            storagePolicyTransitionParamsDto.getDestinationStorageName(), s3FileCopyRequestParamsDto.getTargetBucketName(),
                            s3FileCopyRequestParamsDto.getTargetObjectKey(),
                            businessObjectDataHelper.businessObjectDataKeyToString(storagePolicyTransitionParamsDto.getBusinessObjectDataKey())), e);
                }
            }

            // Get the list of S3 files matching the destination S3 key prefix. When listing S3 files, we ignore 0 byte objects that represent S3 directories.
            List<S3ObjectSummary> actualDestinationS3Files = s3Service.listDirectory(destinationS3FileTransferRequestParamsDto, true);

            // Validate existence and file size of the copied S3 files
            storageFileHelper
                .validateCopiedS3Files(expectedDestinationS3Files, actualDestinationS3Files, storagePolicyTransitionParamsDto.getDestinationStorageName(),
                    storagePolicyTransitionParamsDto.getBusinessObjectDataKey());

            // Log a list of files copied to the destination S3 bucket.
            if (LOGGER.isInfoEnabled())
            {
                LOGGER.info("Copied S3 files to the destination S3 bucket. s3KeyCount={} s3BucketName=\"{}\"", actualDestinationS3Files.size(),
                    destinationS3FileTransferRequestParamsDto.getS3BucketName());

                for (S3ObjectSummary s3File : actualDestinationS3Files)
                {
                    LOGGER.info("s3Key=\"{}\"", s3File.getKey());
                }
            }
        }
        catch (RuntimeException e)
        {
            // Check if we need to rollback the S3 copy operation by deleting all keys matching destination S3 key prefix in the destination S3 bucket.
            if (cleanUpDestinationS3BucketOnFailure)
            {
                LOGGER.info("Rolling back the S3 copy operation by deleting all keys matching the S3 key prefix... s3KeyPrefix=\"{}\" s3BucketName=\"{}\"",
                    destinationS3FileTransferRequestParamsDto.getS3KeyPrefix(), destinationS3FileTransferRequestParamsDto.getS3BucketName());

                // Delete all object keys from the destination S3 bucket matching the expected S3 key prefix.
                // Please note that when deleting S3 files, by default, we also delete all 0 byte objects that represent S3 directories.
                s3Service.deleteDirectoryIgnoreException(destinationS3FileTransferRequestParamsDto);
            }

            // Rethrow the original exception.
            throw e;
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

        // Retrieve the source storage unit and ensure it exists.
        StorageUnitEntity sourceStorageUnitEntity =
            storageUnitDaoHelper.getStorageUnitEntity(storagePolicyTransitionParamsDto.getSourceStorageName(), businessObjectDataEntity);

        // Validate that source storage unit status is ENABLED.
        Assert.isTrue(StorageUnitStatusEntity.ENABLED.equals(sourceStorageUnitEntity.getStatus().getCode()), String.format(
            "Source storage unit status is \"%s\", but must be \"%s\" for storage policy transition to proceed. Storage: {%s}, business object data: {%s}",
            sourceStorageUnitEntity.getStatus().getCode(), StorageUnitStatusEntity.ENABLED, storagePolicyTransitionParamsDto.getSourceStorageName(),
            businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Retrieve the destination storage unit and ensure it exists.
        StorageUnitEntity destinationStorageUnitEntity =
            storageUnitDaoHelper.getStorageUnitEntity(storagePolicyTransitionParamsDto.getDestinationStorageName(), businessObjectDataEntity);

        // Validate that destination storage unit status is ARCHIVING.
        Assert.isTrue(StorageUnitStatusEntity.ARCHIVING.equals(destinationStorageUnitEntity.getStatus().getCode()), String.format(
            "Destination storage unit status is \"%s\", but must be \"%s\" for storage policy transition to proceed. Storage: {%s}, business object data: {%s}",
            destinationStorageUnitEntity.getStatus().getCode(), StorageUnitStatusEntity.ARCHIVING, storagePolicyTransitionParamsDto.getDestinationStorageName(),
            businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Change the destination storage unit status to ENABLED.
        String reason = StorageUnitStatusEntity.ARCHIVING;
        storagePolicyTransitionParamsDto.setOldDestinationStorageUnitStatus(destinationStorageUnitEntity.getStatus().getCode());
        storageUnitDaoHelper.updateStorageUnitStatus(destinationStorageUnitEntity, StorageUnitStatusEntity.ENABLED, reason);
        storagePolicyTransitionParamsDto.setNewDestinationStorageUnitStatus(destinationStorageUnitEntity.getStatus().getCode());

        // Change the source storage unit status to DISABLED.
        storagePolicyTransitionParamsDto.setOldSourceStorageUnitStatus(sourceStorageUnitEntity.getStatus().getCode());
        storageUnitDaoHelper.updateStorageUnitStatus(sourceStorageUnitEntity, StorageUnitStatusEntity.DISABLED, reason);
        storagePolicyTransitionParamsDto.setNewSourceStorageUnitStatus(sourceStorageUnitEntity.getStatus().getCode());
    }

    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void executeStoragePolicyTransitionAfterStep(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        executeStoragePolicyTransitionAfterStepImpl(storagePolicyTransitionParamsDto);
    }

    /**
     * Executes a step after the storage policy transition is completed.
     *
     * @param storagePolicyTransitionParamsDto the storage policy transition DTO that contains parameters needed for the storage policy transition after step
     */
    protected void executeStoragePolicyTransitionAfterStepImpl(StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto)
    {
        // Create an S3 file transfer parameters DTO to access the source S3 bucket.
        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = storageHelper.getS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3BucketName(storagePolicyTransitionParamsDto.getSourceBucketName());
        s3FileTransferRequestParamsDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
        s3FileTransferRequestParamsDto.setS3KeyPrefix(StringUtils.appendIfMissing(storagePolicyTransitionParamsDto.getSourceS3KeyPrefix(), "/"));

        // Delete the source S3 data by deleting all keys/objects from the source S3 bucket matching the source S3 key prefix.
        // Please note that when deleting S3 files, by default, we also delete all 0 byte objects that represent S3 directories.
        s3Service.deleteDirectoryIgnoreException(s3FileTransferRequestParamsDto);
    }
}
