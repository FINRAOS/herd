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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileCopyRequestParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StorageUnitAlternateKeyDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.BusinessObjectDataFinalizeRestoreHelperService;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;
import org.finra.herd.service.helper.StorageUnitStatusDaoHelper;

/**
 * An implementation of the helper service class for the business object data finalize restore functionality.
 */
@Service
public class BusinessObjectDataFinalizeRestoreHelperServiceImpl implements BusinessObjectDataFinalizeRestoreHelperService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessObjectDataFinalizeRestoreHelperServiceImpl.class);

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private StorageFileHelper storageFileHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Autowired
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataRestoreDto prepareToFinalizeRestore(StorageUnitAlternateKeyDto glacierStorageUnitKey)
    {
        return prepareToFinalizeRestoreImpl(glacierStorageUnitKey);
    }

    /**
     * Prepares for the business object data finalize restore by validating the Glacier storage unit along with other related database entities. The method also
     * creates and returns a business object data restore DTO.
     *
     * @param glacierStorageUnitKey the Glacier storage unit key
     *
     * @return the DTO that holds various parameters needed to perform a business object data restore
     */
    protected BusinessObjectDataRestoreDto prepareToFinalizeRestoreImpl(StorageUnitAlternateKeyDto glacierStorageUnitKey)
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.createBusinessObjectDataKeyFromStorageUnitKey(glacierStorageUnitKey);

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Retrieve and validate a Glacier storage unit.
        StorageUnitEntity glacierStorageUnitEntity = getGlacierStorageUnit(businessObjectDataEntity, glacierStorageUnitKey.getStorageName());

        // Validate that Glacier storage has S3 bucket name configured.
        // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
        String glacierBucketName = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                glacierStorageUnitEntity.getStorage(), true);

        // Retrieve and validate an origin storage unit.
        StorageUnitEntity originStorageUnitEntity = getOriginStorageUnit(glacierStorageUnitEntity);

        // Validate that origin S3 storage has S3 bucket name configured.
        // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
        String originBucketName = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                originStorageUnitEntity.getStorage(), true);

        // Validate that Glacier storage unit storage directory path starts with the origin S3 bucket name.
        Assert.isTrue(glacierStorageUnitEntity.getDirectoryPath().startsWith(originBucketName + "/"), String.format(
            "Storage directory path \"%s\" for business object data in \"%s\" %s storage does not start with the origin S3 bucket name. " +
                "Origin S3 bucket name: {%s}, origin storage: {%s}, business object data: {%s}", glacierStorageUnitEntity.getDirectoryPath(),
            glacierStorageUnitEntity.getStorage().getName(), StoragePlatformEntity.GLACIER, originBucketName, originStorageUnitEntity.getStorage().getName(),
            businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

        // Construct the origin S3 key prefix by removing the origin S3 bucket name from the beginning of the Glacier storage unit storage directory path.
        String originS3KeyPrefix = StringUtils.removeStart(glacierStorageUnitEntity.getDirectoryPath(), originBucketName + "/");

        // Retrieve storage files registered with this business object data in the origin storage.
        List<StorageFile> originStorageFiles = storageFileHelper.createStorageFilesFromEntities(originStorageUnitEntity.getStorageFiles());

        // Validate that we have storage files registered in the origin storage.
        Assert.isTrue(CollectionUtils.isNotEmpty(originStorageFiles), String
            .format("Business object data has no storage files registered in \"%s\" origin storage. Business object data: {%s}",
                originStorageUnitEntity.getStorage().getName(), businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Build the storage policy transition parameters DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto = new BusinessObjectDataRestoreDto();
        businessObjectDataRestoreDto.setBusinessObjectDataKey(businessObjectDataKey);
        businessObjectDataRestoreDto.setOriginStorageName(originStorageUnitEntity.getStorage().getName());
        businessObjectDataRestoreDto.setOriginBucketName(originBucketName);
        businessObjectDataRestoreDto.setOriginS3KeyPrefix(originS3KeyPrefix);
        businessObjectDataRestoreDto.setOriginStorageFiles(originStorageFiles);
        businessObjectDataRestoreDto.setGlacierStorageName(glacierStorageUnitEntity.getStorage().getName());
        businessObjectDataRestoreDto.setGlacierBucketName(glacierBucketName);
        businessObjectDataRestoreDto.setGlacierS3KeyBasePrefix(originBucketName);
        businessObjectDataRestoreDto.setGlacierS3KeyPrefix(glacierStorageUnitEntity.getDirectoryPath());

        // Return the parameters DTO.
        return businessObjectDataRestoreDto;
    }

    /**
     * Retrieves a Glacier storage unit per specified parameters. The method validates that the storage unit belongs to a Glacier storage and that the storage
     * unit has "ENABLED" status and a non-blank storage directory path.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param storageName the storage name (case-insensitive)
     *
     * @return the Glacier storage unit entity
     */
    protected StorageUnitEntity getGlacierStorageUnit(BusinessObjectDataEntity businessObjectDataEntity, String storageName)
    {
        // Retrieve and validate glacier storage unit for the business object data.
        StorageUnitEntity glacierStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(storageName, businessObjectDataEntity);

        // Validate that we have an "ENABLED" Glacier storage unit.
        Assert.isTrue(StorageUnitStatusEntity.ENABLED.equals(glacierStorageUnitEntity.getStatus().getCode()), String
            .format("Business object data is not archived. Business object data: {%s}",
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

        // Validate that Glacier storage unit has a non-blank storage directory path.
        Assert.isTrue(StringUtils.isNotBlank(glacierStorageUnitEntity.getDirectoryPath()), String
            .format("Business object data has no storage directory path specified in \"%s\" %s storage. Business object data: {%s}",
                glacierStorageUnitEntity.getStorage().getName(), StoragePlatformEntity.GLACIER,
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

        return glacierStorageUnitEntity;
    }

    /**
     * Retrieves an origin storage unit for the specified Glacier storage unit and validates it.
     *
     * @param glacierStorageUnitEntity the Glacier storage unit
     *
     * @return the origin storage unit entity
     */
    protected StorageUnitEntity getOriginStorageUnit(StorageUnitEntity glacierStorageUnitEntity)
    {
        StorageUnitEntity originStorageUnitEntity = glacierStorageUnitEntity.getParentStorageUnit();

        // Validate that the specified Glacier storage unit has a parent S3 storage unit.
        if (originStorageUnitEntity == null || !StoragePlatformEntity.S3.equals(originStorageUnitEntity.getStorage().getStoragePlatform().getName()))
        {
            throw new IllegalArgumentException(String.format("Glacier storage unit in \"%s\" storage has no origin S3 storage unit. Business object data: {%s}",
                glacierStorageUnitEntity.getStorage().getName(),
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(glacierStorageUnitEntity.getBusinessObjectData())));
        }

        // Validate that S3 storage unit has RESTORING status.
        if (!StorageUnitStatusEntity.RESTORING.equals(originStorageUnitEntity.getStatus().getCode()))
        {
            // Fail with a custom error message if the origin S3 storage unit is already enabled.
            if (StorageUnitStatusEntity.ENABLED.equals(originStorageUnitEntity.getStatus().getCode()))
            {
                throw new IllegalArgumentException(String.format("Business object data is already available in \"%s\" S3 storage. Business object data: {%s}",
                    originStorageUnitEntity.getStorage().getName(),
                    businessObjectDataHelper.businessObjectDataEntityAltKeyToString(glacierStorageUnitEntity.getBusinessObjectData())));
            }
            // Else, fail and report the actual origin S3 storage unit status.
            else
            {
                throw new IllegalArgumentException(String
                    .format("Origin S3 storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. Business object data: {%s}",
                        originStorageUnitEntity.getStorage().getName(), StorageUnitStatusEntity.RESTORING, originStorageUnitEntity.getStatus().getCode(),
                        businessObjectDataHelper.businessObjectDataEntityAltKeyToString(glacierStorageUnitEntity.getBusinessObjectData())));
            }
        }

        return originStorageUnitEntity;
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
     * Executes S3 specific steps for the business object data finalize restore.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to perform a business object data restore
     */
    protected void executeS3SpecificStepsImpl(BusinessObjectDataRestoreDto businessObjectDataRestoreDto)
    {
        boolean cleanUpOriginS3BucketLocationOnFailure = false;
        S3FileTransferRequestParamsDto originS3FileTransferRequestParamsDto = null;

        try
        {
            // Create an S3 file transfer parameters DTO to access the Glacier S3 bucket location.
            // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
            S3FileTransferRequestParamsDto glacierS3FileTransferRequestParamsDto = storageHelper.getS3FileTransferRequestParamsDto();
            glacierS3FileTransferRequestParamsDto.setS3BucketName(businessObjectDataRestoreDto.getGlacierBucketName());
            glacierS3FileTransferRequestParamsDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
            glacierS3FileTransferRequestParamsDto.setS3KeyPrefix(StringUtils.appendIfMissing(businessObjectDataRestoreDto.getGlacierS3KeyPrefix(), "/"));

            // Get actual Glacier S3 files by selecting all S3 keys matching the Glacier S3 key prefix form the Glacier S3 bucket.
            // When listing S3 files, we ignore 0 byte objects that represent S3 directories.
            List<S3ObjectSummary> actualGlacierS3Files = s3Service.listDirectory(glacierS3FileTransferRequestParamsDto, true);

            // Build a list of S3 files expected to be located at the Glacier destination.
            List<StorageFile> expectedGlacierS3Files = new ArrayList<>();
            for (StorageFile originS3File : businessObjectDataRestoreDto.getOriginStorageFiles())
            {
                // Create the Glacier S3 key by concatenating Glacier S3 key base prefix with the relative origin S3 key.
                String glacierS3FilePath = String.format("%s/%s", businessObjectDataRestoreDto.getGlacierS3KeyBasePrefix(), originS3File.getFilePath());

                // Update the list of expected destination S3 files. Please note that we do not validate row count information.
                expectedGlacierS3Files.add(new StorageFile(glacierS3FilePath, originS3File.getFileSizeBytes(), null));
            }

            // Validate existence and file size of the Glacier S3 files.
            storageFileHelper.validateArchivedS3Files(expectedGlacierS3Files, actualGlacierS3Files, businessObjectDataRestoreDto.getGlacierStorageName(),
                businessObjectDataRestoreDto.getBusinessObjectDataKey());

            // Build a list of files to check for restore status by selection only objects
            // that are currently archived in Glacier (have Glacier storage class).
            List<S3ObjectSummary> archivedGlacierS3Files = new ArrayList<>();
            for (S3ObjectSummary s3ObjectSummary : actualGlacierS3Files)
            {
                if (StorageClass.Glacier.toString().equals(s3ObjectSummary.getStorageClass()))
                {
                    archivedGlacierS3Files.add(s3ObjectSummary);
                }
            }

            // Validate that all Glacier storage class S3 files are now restored.
            glacierS3FileTransferRequestParamsDto
                .setFiles(storageFileHelper.getFiles(storageFileHelper.createStorageFilesFromS3ObjectSummaries(archivedGlacierS3Files)));
            s3Service.validateGlacierS3FilesRestored(glacierS3FileTransferRequestParamsDto);

            // Create an S3 file transfer parameters DTO to access the origin S3 bucket.
            // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
            originS3FileTransferRequestParamsDto = storageHelper.getS3FileTransferRequestParamsDto();
            originS3FileTransferRequestParamsDto.setS3BucketName(businessObjectDataRestoreDto.getOriginBucketName());
            originS3FileTransferRequestParamsDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
            originS3FileTransferRequestParamsDto.setS3KeyPrefix(StringUtils.appendIfMissing(businessObjectDataRestoreDto.getOriginS3KeyPrefix(), "/"));

            // Check that the origin S3 key prefix is empty.
            // When listing S3 files, by default, we do not ignore 0 byte objects that represent S3 directories.
            if (s3Service.listDirectory(originS3FileTransferRequestParamsDto).isEmpty())
            {
                cleanUpOriginS3BucketLocationOnFailure = true;
            }
            else
            {
                throw new IllegalStateException(String.format("The origin S3 key prefix is not empty. S3 bucket name: {%s}, S3 key prefix: {%s}",
                    originS3FileTransferRequestParamsDto.getS3BucketName(), originS3FileTransferRequestParamsDto.getS3KeyPrefix()));
            }

            // Copy Glacier S3 files to the origin S3 bucket.
            S3FileCopyRequestParamsDto s3FileCopyRequestParamsDto = storageHelper.getS3FileCopyRequestParamsDto();
            s3FileCopyRequestParamsDto.setSourceBucketName(businessObjectDataRestoreDto.getGlacierBucketName());
            s3FileCopyRequestParamsDto.setTargetBucketName(businessObjectDataRestoreDto.getOriginBucketName());
            for (StorageFile originS3File : businessObjectDataRestoreDto.getOriginStorageFiles())
            {
                // Create the Glacier S3 key by concatenating Glacier S3 key base prefix with the relative origin S3 key.
                String glacierS3FilePath = String.format("%s/%s", businessObjectDataRestoreDto.getGlacierS3KeyBasePrefix(), originS3File.getFilePath());

                // Copy the file from the Glacier S3 bucket to the origin S3 bucket.
                s3FileCopyRequestParamsDto.setSourceObjectKey(glacierS3FilePath);
                s3FileCopyRequestParamsDto.setTargetObjectKey(originS3File.getFilePath());
                try
                {
                    s3Dao.copyFile(s3FileCopyRequestParamsDto);
                }
                catch (Exception e)
                {
                    throw new IllegalStateException(
                        String.format("Failed to copy S3 file. Source storage: {%s}, source S3 bucket name: {%s}, source S3 object key: {%s}, " +
                            "target storage: {%s}, target S3 bucket name: {%s}, target S3 object key: {%s}, " +
                            "business object data: {%s}", businessObjectDataRestoreDto.getGlacierStorageName(),
                            s3FileCopyRequestParamsDto.getSourceBucketName(), s3FileCopyRequestParamsDto.getSourceObjectKey(),
                            businessObjectDataRestoreDto.getOriginStorageName(), s3FileCopyRequestParamsDto.getTargetBucketName(),
                            s3FileCopyRequestParamsDto.getTargetObjectKey(),
                            businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataRestoreDto.getBusinessObjectDataKey())), e);
                }
            }

            // Get the list of S3 files matching the destination S3 key prefix. When listing S3 files, we ignore 0 byte objects that represent S3 directories.
            List<S3ObjectSummary> actualOriginS3Files = s3Service.listDirectory(originS3FileTransferRequestParamsDto, true);

            // Validate existence and file size of the restored S3 files.
            storageFileHelper.validateRestoredS3Files(businessObjectDataRestoreDto.getOriginStorageFiles(), actualOriginS3Files,
                businessObjectDataRestoreDto.getOriginStorageName(), businessObjectDataRestoreDto.getBusinessObjectDataKey());

            // Log a list of files copied to the origin S3 bucket.
            if (LOGGER.isInfoEnabled())
            {
                LOGGER.info("Copied S3 files to the origin S3 bucket. s3KeyCount={} s3BucketName=\"{}\"", actualOriginS3Files.size(),
                    originS3FileTransferRequestParamsDto.getS3BucketName());

                for (S3ObjectSummary s3File : actualOriginS3Files)
                {
                    LOGGER.info("s3Key=\"{}\"", s3File.getKey());
                }
            }
        }
        catch (RuntimeException e)
        {
            // Check if we need to rollback the S3 copy operation by deleting all keys matching origin S3 key prefix in the origin S3 bucket.
            if (cleanUpOriginS3BucketLocationOnFailure)
            {
                LOGGER.info("Rolling back the S3 copy operation by deleting all keys matching the S3 key prefix... s3KeyPrefix=\"{}\" s3BucketName=\"{}\"",
                    originS3FileTransferRequestParamsDto.getS3KeyPrefix(), originS3FileTransferRequestParamsDto.getS3BucketName());

                // Delete all object keys from the origin S3 bucket matching the origin S3 key prefix.
                // Please note that when deleting S3 files, by default, we also delete all 0 byte objects that represent S3 directories.
                s3Service.deleteDirectoryIgnoreException(originS3FileTransferRequestParamsDto);
            }

            // Rethrow the original exception.
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void enableOriginStorageUnit(BusinessObjectDataRestoreDto businessObjectDataRestoreDto)
    {
        enableOriginStorageUnitImpl(businessObjectDataRestoreDto);
    }

    /**
     * Updates the origin S3 storage unit status to ENABLED.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to perform a business object data restore
     */
    protected void enableOriginStorageUnitImpl(BusinessObjectDataRestoreDto businessObjectDataRestoreDto)
    {
        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataRestoreDto.getBusinessObjectDataKey());

        // Retrieve the origin storage unit and ensure it exists.
        StorageUnitEntity originStorageUnitEntity =
            storageUnitDaoHelper.getStorageUnitEntity(businessObjectDataRestoreDto.getOriginStorageName(), businessObjectDataEntity);

        // Retrieve and ensure the ENABLED storage unit status entity exists.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.ENABLED);

        // Update the origin S3 storage unit status to ENABLED.
        businessObjectDataRestoreDto.setOldOriginStorageUnitStatus(originStorageUnitEntity.getStatus().getCode());
        storageUnitDaoHelper.updateStorageUnitStatus(originStorageUnitEntity, storageUnitStatusEntity, StorageUnitStatusEntity.ENABLED);
        businessObjectDataRestoreDto.setNewOriginStorageUnitStatus(originStorageUnitEntity.getStatus().getCode());
    }
}
