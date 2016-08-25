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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.helper.JsonHelper;
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
    private JsonHelper jsonHelper;

    @Autowired
    private S3Service s3Service;

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
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataRestoreDto prepareToInitiateRestore(BusinessObjectDataKey businessObjectDataKey)
    {
        return prepareToInitiateRestoreImpl(businessObjectDataKey);
    }

    /**
     * Prepares for the business object data initiate a restore request by validating the business object data along with other related database entities. The
     * method also creates and returns a business object data restore DTO.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the DTO that holds various parameters needed to perform a business object data restore
     */
    protected BusinessObjectDataRestoreDto prepareToInitiateRestoreImpl(BusinessObjectDataKey businessObjectDataKey)
    {
        // Validate and trim the business object data key.
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Retrieve and validate a Glacier storage unit for this business object data.
        StorageUnitEntity glacierStorageUnitEntity = getGlacierStorageUnit(businessObjectDataEntity);

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
        String originS3KeyPrefix = StringUtils.removeStart(originBucketName + "/", glacierStorageUnitEntity.getDirectoryPath());

        // Retrieve storage files registered with this business object data in the origin storage.
        List<StorageFile> originStorageFiles = storageFileHelper.createStorageFilesFromEntities(originStorageUnitEntity.getStorageFiles());

        // Validate that we have storage files registered in the origin storage.
        Assert.isTrue(!CollectionUtils.isEmpty(originStorageFiles), String
            .format("Business object data has no storage files registered in \"%s\" origin storage. Business object data: {%s}",
                originStorageUnitEntity.getStorage().getName(), businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Retrieve and ensure the RESTORING storage unit status entity exists.
        StorageUnitStatusEntity newStorageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.RESTORING);

        // Get the old storage unit status value for the origin storage unit.
        String oldOriginStorageUnitStatus = originStorageUnitEntity.getStatus().getCode();

        // Update the origin S3 storage unit status to RESTORING.
        storageUnitDaoHelper.updateStorageUnitStatus(originStorageUnitEntity, newStorageUnitStatusEntity, StorageUnitStatusEntity.RESTORING);

        // Build the storage policy transition parameters DTO.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto = new BusinessObjectDataRestoreDto();
        businessObjectDataRestoreDto.setBusinessObjectDataKey(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity));
        businessObjectDataRestoreDto.setOriginStorageName(originStorageUnitEntity.getStorage().getName());
        businessObjectDataRestoreDto.setOriginBucketName(originBucketName);
        businessObjectDataRestoreDto.setOriginS3KeyPrefix(originS3KeyPrefix);
        businessObjectDataRestoreDto.setOriginStorageFiles(originStorageFiles);
        businessObjectDataRestoreDto.setNewOriginStorageUnitStatus(newStorageUnitStatusEntity.getCode());
        businessObjectDataRestoreDto.setOldOriginStorageUnitStatus(oldOriginStorageUnitStatus);
        businessObjectDataRestoreDto.setGlacierStorageName(glacierStorageUnitEntity.getStorage().getName());
        businessObjectDataRestoreDto.setGlacierBucketName(glacierBucketName);
        businessObjectDataRestoreDto.setGlacierS3KeyBasePrefix(originBucketName);
        businessObjectDataRestoreDto.setGlacierS3KeyPrefix(glacierStorageUnitEntity.getDirectoryPath());

        // Return the parameters DTO.
        return businessObjectDataRestoreDto;
    }

    /**
     * Retrieves a Glacier storage unit for the specified business object data. The method validates that there one and only one Glacier storage unit for the
     * business object data and that the storage unit has "ENABLED" status and a non-blank storage directory path.
     *
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the Glacier storage unit entity
     */
    protected StorageUnitEntity getGlacierStorageUnit(BusinessObjectDataEntity businessObjectDataEntity)
    {
        // Retrieve and validate glacier storage unit for the business object data.
        List<StorageUnitEntity> glacierStorageUnitEntities =
            storageUnitDao.getStorageUnitsByStoragePlatformAndBusinessObjectData(StoragePlatformEntity.GLACIER, businessObjectDataEntity);

        // Validate that business object data has no multiple Glacier storage units.
        Assert.isTrue(CollectionUtils.isEmpty(glacierStorageUnitEntities) || glacierStorageUnitEntities.size() < 2, String
            .format("Business object data has multiple (%s) %s storage units. Business object data: {%s}", glacierStorageUnitEntities.size(),
                StoragePlatformEntity.GLACIER, businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

        // Validate that we have a single "ENABLED" Glacier storage unit for the business object data.
        Assert.isTrue(!CollectionUtils.isEmpty(glacierStorageUnitEntities) &&
            StorageUnitStatusEntity.ENABLED.equals(glacierStorageUnitEntities.get(0).getStatus().getCode()), String
            .format("Business object data is not archived. Business object data: {%s}",
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

        // Validate that Glacier storage unit has a non-blank storage directory path.
        Assert.isTrue(StringUtils.isNotBlank(glacierStorageUnitEntities.get(0).getDirectoryPath()), String
            .format("Business object data has no storage directory path specified in \"%s\" %s storage. Business object data: {%s}",
                glacierStorageUnitEntities.get(0).getStorage().getName(), StoragePlatformEntity.GLACIER,
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

        return glacierStorageUnitEntities.get(0);
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

        // Validate that S3 storage unit is DISABLED.
        if (!StorageUnitStatusEntity.DISABLED.equals(originStorageUnitEntity.getStatus().getCode()))
        {
            // Fail with a custom error message if the origin S3 storage unit is already enabled.
            if (StorageUnitStatusEntity.ENABLED.equals(originStorageUnitEntity.getStatus().getCode()))
            {
                throw new IllegalArgumentException(String.format("Business object data is already available in \"%s\" S3 storage. Business object data: {%s}",
                    originStorageUnitEntity.getStorage().getName(),
                    businessObjectDataHelper.businessObjectDataEntityAltKeyToString(glacierStorageUnitEntity.getBusinessObjectData())));
            }
            // Fail with a custom error message if business object data is already marked as being restored.
            else if (StorageUnitStatusEntity.RESTORING.equals(originStorageUnitEntity.getStatus().getCode()))
            {
                throw new IllegalArgumentException(String
                    .format("Business object data is already being restored to \"%s\" S3 storage. Business object data: {%s}",
                        originStorageUnitEntity.getStorage().getName(),
                        businessObjectDataHelper.businessObjectDataEntityAltKeyToString(glacierStorageUnitEntity.getBusinessObjectData())));
            }
            // Else, fail and report the actual origin S3 storage unit status.
            else
            {
                throw new IllegalArgumentException(String
                    .format("Origin S3 storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. Business object data: {%s}",
                        originStorageUnitEntity.getStorage().getName(), StorageUnitStatusEntity.DISABLED, originStorageUnitEntity.getStatus().getCode(),
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
     * Executes S3 specific steps for the initiation of a business object data restore request. The method also updates the specified DTO.
     *
     * @param businessObjectDataRestoreDto the DTO that holds various parameters needed to perform a business object data restore
     */
    protected void executeS3SpecificStepsImpl(BusinessObjectDataRestoreDto businessObjectDataRestoreDto)
    {
        try
        {
            // Create an S3 file transfer parameters DTO to access the Glacier S3 bucket.
            // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
            S3FileTransferRequestParamsDto glacierS3FileTransferRequestParamsDto = storageHelper.getS3FileTransferRequestParamsDto();
            glacierS3FileTransferRequestParamsDto.setS3BucketName(businessObjectDataRestoreDto.getGlacierBucketName());
            glacierS3FileTransferRequestParamsDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
            glacierS3FileTransferRequestParamsDto.setS3KeyPrefix(StringUtils.appendIfMissing(businessObjectDataRestoreDto.getGlacierS3KeyPrefix(), "/"));

            // Build a list of S3 files expected to be located at the Glacier destination.
            List<StorageFile> expectedGlacierS3Files = new ArrayList<>();
            for (StorageFile originS3File : businessObjectDataRestoreDto.getOriginStorageFiles())
            {
                // Create the Glacier S3 key by concatenating Glacier S3 key base prefix with the relative origin S3 key.
                String destinationS3FilePath = String.format("%s/%s", businessObjectDataRestoreDto.getGlacierS3KeyBasePrefix(), originS3File.getFilePath());

                // Update the list of expected destination S3 files. Please note that we do not validate row count information.
                expectedGlacierS3Files.add(new StorageFile(destinationS3FilePath, originS3File.getFileSizeBytes(), null));
            }

            // Get the list of S3 files matching the Glacier S3 key prefix. When listing S3 files, we ignore 0 byte objects that represent S3 directories.
            List<S3ObjectSummary> actualGlacierS3Files = s3Service.listDirectory(glacierS3FileTransferRequestParamsDto, true);

            // Validate existence and file size of the Glacier S3 files.
            storageFileHelper.validateArchivedS3Files(expectedGlacierS3Files, actualGlacierS3Files, businessObjectDataRestoreDto.getGlacierStorageName(),
                businessObjectDataRestoreDto.getBusinessObjectDataKey());

            // Build a list of files to restore by selection only objects that are currently archived in Glacier (have Glacier storage class).
            List<S3ObjectSummary> archivedGlacierS3Files = new ArrayList<>();
            for (S3ObjectSummary s3ObjectSummary : actualGlacierS3Files)
            {
                if (StorageClass.Glacier.toString().equals(s3ObjectSummary.getStorageClass()))
                {
                    archivedGlacierS3Files.add(s3ObjectSummary);
                }
            }

            // Set the list of files to restore.
            glacierS3FileTransferRequestParamsDto
                .setFiles(storageFileHelper.getFiles(storageFileHelper.createStorageFilesFromS3ObjectSummaries(archivedGlacierS3Files)));

            // Initiate restore requests for the list of objects in the Glacier bucket.
            // TODO: Make "expirationInDays" value configurable with default value set to 7 days
            s3Service.restoreObjects(glacierS3FileTransferRequestParamsDto, 7);
        }
        catch (Exception e)
        {
            // Log the exception.
            LOGGER.error("Failed to initiate a restore request for the business object data. businessObjectDataKey={}",
                jsonHelper.objectToJson(businessObjectDataRestoreDto.getBusinessObjectDataKey()), businessObjectDataRestoreDto.getException());

            // Update the DTO with the caught exception.
            businessObjectDataRestoreDto.setException(e);
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectData executeInitiateRestoreAfterStep(BusinessObjectDataRestoreDto businessObjectDataRestoreDto)
    {
        return executeInitiateRestoreAfterStepImpl(businessObjectDataRestoreDto);
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

        // On failure, set the origin storage unit status back to DISABLED.
        if (businessObjectDataRestoreDto.getException() != null)
        {
            // Retrieve the origin storage unit and ensure it exists.
            StorageUnitEntity originStorageUnitEntity =
                storageUnitDaoHelper.getStorageUnitEntity(businessObjectDataRestoreDto.getOriginStorageName(), businessObjectDataEntity);

            // Retrieve and ensure the DISABLED storage unit status entity exists.
            StorageUnitStatusEntity newStorageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.DISABLED);

            // Get the old storage unit status value for the origin storage unit.
            String oldStorageUnitStatus = originStorageUnitEntity.getStatus().getCode();

            // Update the origin S3 storage unit status to DISABLED.
            storageUnitDaoHelper.updateStorageUnitStatus(originStorageUnitEntity, newStorageUnitStatusEntity, StorageUnitStatusEntity.DISABLED);

            // Update the new and old storage unit status values for the origin storage unit in the business object data restore DTO.
            businessObjectDataRestoreDto.setNewOriginStorageUnitStatus(newStorageUnitStatusEntity.getCode());
            businessObjectDataRestoreDto.setOldOriginStorageUnitStatus(oldStorageUnitStatus);
        }

        // Create and return the business object data object from the entity.
        BusinessObjectData businessObjectData = businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);

        // Return the business object data information.
        return businessObjectData;
    }
}
