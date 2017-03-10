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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.SqsDao;
import org.finra.herd.dao.StorageFileDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataRetryStoragePolicyTransitionRequest;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.BusinessObjectDataRetryStoragePolicyTransitionDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.BusinessObjectDataRetryStoragePolicyTransitionHelperService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StoragePolicyDaoHelper;
import org.finra.herd.service.helper.StoragePolicyHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;
import org.finra.herd.service.helper.StorageUnitStatusDaoHelper;

/**
 * An implementation of the helper service class for the business object data retry storage policy transition functionality.
 */
@Service
public class BusinessObjectDataRetryStoragePolicyTransitionHelperServiceImpl implements BusinessObjectDataRetryStoragePolicyTransitionHelperService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessObjectDataRetryStoragePolicyTransitionHelperServiceImpl.class);

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private NotificationEventService notificationEventService;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private SqsDao sqsDao;

    @Autowired
    private StorageFileDao storageFileDao;

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

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectData executeRetryStoragePolicyTransitionAfterStep(
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto)
    {
        return executeRetryStoragePolicyTransitionAfterStepImpl(businessObjectDataRetryStoragePolicyTransitionDto);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation executes non-transactionally, suspends the current transaction if one exists.
     */
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void executeS3SpecificSteps(BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto)
    {
        executeS3SpecificStepsImpl(businessObjectDataRetryStoragePolicyTransitionDto);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataRetryStoragePolicyTransitionDto prepareToRetryStoragePolicyTransition(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataRetryStoragePolicyTransitionRequest request)
    {
        return prepareToRetryStoragePolicyTransitionImpl(businessObjectDataKey, request);
    }

    /**
     * Executes the after step for the retry a storage policy transition and return the business object data information.
     *
     * @param businessObjectDataRetryStoragePolicyTransitionDto the DTO that holds various parameters needed to retry a storage policy transition
     *
     * @return the business object data information
     */
    protected BusinessObjectData executeRetryStoragePolicyTransitionAfterStepImpl(
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto)
    {
        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataRetryStoragePolicyTransitionDto.getBusinessObjectDataKey());

        // Retrieve the Glacier storage unit and ensure it exists.
        StorageUnitEntity glacierStorageUnitEntity =
            storageUnitDaoHelper.getStorageUnitEntity(businessObjectDataRetryStoragePolicyTransitionDto.getGlacierStorageName(), businessObjectDataEntity);

        // Retrieve and ensure the DISABLED storage unit status entity exists.
        StorageUnitStatusEntity newGlacierStorageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(StorageUnitStatusEntity.DISABLED);

        // Get the new storage unit status value for the Glacier storage unit.
        String newGlacierOriginStorageUnitStatus = newGlacierStorageUnitStatusEntity.getCode();

        // Get the old storage unit status value from the Glacier storage unit.
        String oldGlacierOriginStorageUnitStatus = glacierStorageUnitEntity.getStatus().getCode();

        // Update the Glacier S3 storage unit status to DISABLED.
        storageUnitDaoHelper.updateStorageUnitStatus(glacierStorageUnitEntity, newGlacierStorageUnitStatusEntity, StorageUnitStatusEntity.DISABLED);

        // Create storage unit notification for the Glacier storage unit.
        notificationEventService.processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
            businessObjectDataRetryStoragePolicyTransitionDto.getBusinessObjectDataKey(),
            businessObjectDataRetryStoragePolicyTransitionDto.getGlacierStorageName(), newGlacierOriginStorageUnitStatus, oldGlacierOriginStorageUnitStatus);

        // Create a storage policy selection.
        StoragePolicySelection storagePolicySelection = new StoragePolicySelection(businessObjectDataRetryStoragePolicyTransitionDto.getBusinessObjectDataKey(),
            businessObjectDataRetryStoragePolicyTransitionDto.getStoragePolicyKey(),
            businessObjectDataRetryStoragePolicyTransitionDto.getStoragePolicyVersion());

        // Executes SQS specific steps needed to retry a storage policy transition.
        sendStoragePolicySelectionSqsMessage(businessObjectDataRetryStoragePolicyTransitionDto.getSqsQueueName(), storagePolicySelection);

        // Create and return the business object data object from the entity.
        return businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);
    }

    /**
     * Executes S3 specific steps needed to retry a storage policy transition.
     *
     * @param businessObjectDataRetryStoragePolicyTransitionDto the DTO that holds various parameters needed to retry a storage policy transition
     */
    protected void executeS3SpecificStepsImpl(BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto)
    {
        try
        {
            // Create an S3 file transfer parameters DTO to access the Glacier S3 bucket.
            // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
            S3FileTransferRequestParamsDto glacierS3FileTransferRequestParamsDto = storageHelper.getS3FileTransferRequestParamsDto();
            glacierS3FileTransferRequestParamsDto.setS3BucketName(businessObjectDataRetryStoragePolicyTransitionDto.getGlacierBucketName());
            glacierS3FileTransferRequestParamsDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
            glacierS3FileTransferRequestParamsDto
                .setS3KeyPrefix(StringUtils.appendIfMissing(businessObjectDataRetryStoragePolicyTransitionDto.getGlacierS3KeyPrefix(), "/"));

            // If Glacier S3 key prefix is not empty, delete all discovered S3 objects from the Glacier S3 bucket matching the S3 key prefix.
            // Please note that when deleting S3 files, by default, we also delete all 0 byte objects that represent S3 directories.
            s3Service.deleteDirectory(glacierS3FileTransferRequestParamsDto);
        }
        catch (RuntimeException e)
        {
            // Log the exception.
            LOGGER.error("Failed to execute S3 specific steps needed to retry a storage policy transition for the business object data. " +
                " businessObjectDataKey={}, storagePolicyKey={}, storagePolicyVersion={}",
                jsonHelper.objectToJson(businessObjectDataRetryStoragePolicyTransitionDto.getBusinessObjectDataKey()),
                jsonHelper.objectToJson(businessObjectDataRetryStoragePolicyTransitionDto.getStoragePolicyKey()),
                businessObjectDataRetryStoragePolicyTransitionDto.getStoragePolicyVersion(), e);

            // Rethrow the original exception.
            throw e;
        }
    }

    /**
     * Prepares for the business object data retry storage policy transition by validating the input parameters along with the related database entities. The
     * method also creates and returns a business object data retry storage policy transition DTO.
     *
     * @param businessObjectDataKey the business object data key
     * @param request the information needed to retry a storage policy transition
     *
     * @return the DTO that holds various parameters needed to retry a storage policy transition
     */
    protected BusinessObjectDataRetryStoragePolicyTransitionDto prepareToRetryStoragePolicyTransitionImpl(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataRetryStoragePolicyTransitionRequest request)
    {
        // Validate and trim the business object data key.
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // Validate and trim the request.
        validateBusinessObjectDataRetryStoragePolicyTransitionRequest(request);

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Retrieve and ensure that a storage policy exists with the specified key.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoHelper.getStoragePolicyEntityByKey(request.getStoragePolicyKey());

        // Validate that storage policy filter matches this business object data, except for the source storages.
        Assert.isTrue((storagePolicyEntity.getBusinessObjectDefinition() == null ||
            storagePolicyEntity.getBusinessObjectDefinition().equals(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition())) &&
            (StringUtils.isBlank(storagePolicyEntity.getUsage()) ||
                storagePolicyEntity.getUsage().equalsIgnoreCase(businessObjectDataEntity.getBusinessObjectFormat().getUsage())) &&
            (storagePolicyEntity.getFileType() == null ||
                storagePolicyEntity.getFileType().equals(businessObjectDataEntity.getBusinessObjectFormat().getFileType())), String
            .format("Business object data does not match storage policy filter. " + "Storage policy: {%s}, business object data: {%s}",
                storagePolicyHelper.storagePolicyKeyAndVersionToString(request.getStoragePolicyKey(), storagePolicyEntity.getVersion()),
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

        // Validate the source storage.
        storagePolicyDaoHelper.validateSourceStorage(storagePolicyEntity.getStorage());

        // Validate the destination storage.
        storagePolicyDaoHelper.validateDestinationStorage(storagePolicyEntity.getDestinationStorage());

        // Retrieve and validate a Glacier storage unit for this business object data.
        StorageUnitEntity glacierStorageUnitEntity = getGlacierStorageUnit(businessObjectDataEntity, storagePolicyEntity.getDestinationStorage());

        // Retrieve and validate an origin storage unit.
        StorageUnitEntity originStorageUnitEntity = getOriginStorageUnit(glacierStorageUnitEntity);

        // Validate that source storage from the storage policy matches to the Glacier origin storage.
        Assert.isTrue(originStorageUnitEntity.getStorage().equals(storagePolicyEntity.getStorage()), String.format(
            "Origin storage unit for the business object data Glacier storage unit does not belong to the storage policy storage. " +
                "Glacier storage unit origin storage: {%s}, storage policy storage: {%s}, business object data: {%s}",
            originStorageUnitEntity.getStorage().getName(), storagePolicyEntity.getStorage().getName(),
            businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

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
        int originStorageFilesCount = originStorageUnitEntity.getStorageFiles().size();

        // Validate that we have storage files registered in the origin storage.
        Assert.isTrue(originStorageFilesCount > 0, String
            .format("Business object data has no storage files registered in \"%s\" origin storage. Business object data: {%s}",
                originStorageUnitEntity.getStorage().getName(), businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Retrieve all registered storage files from the origin storage that start with the origin S3 key prefix.
        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        String originS3KeyPrefixWithTrailingSlash = StringUtils.appendIfMissing(originS3KeyPrefix, "/");
        Long registeredStorageFilesMatchingS3KeyPrefixCount =
            storageFileDao.getStorageFileCount(originStorageUnitEntity.getStorage().getName(), originS3KeyPrefixWithTrailingSlash);

        // Sanity check for the origin S3 key prefix.
        if (registeredStorageFilesMatchingS3KeyPrefixCount.intValue() != originStorageFilesCount)
        {
            throw new IllegalArgumentException(String.format(
                "Number of storage files (%d) registered for the business object data in \"%s\" storage is not equal to " +
                    "the number of registered storage files (%d) matching \"%s\" S3 key prefix in the same storage. Business object data: {%s}",
                originStorageFilesCount, originStorageUnitEntity.getStorage().getName(), registeredStorageFilesMatchingS3KeyPrefixCount,
                originS3KeyPrefixWithTrailingSlash, businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        // Validate that Glacier S3 storage has S3 bucket name configured.
        // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
        String glacierBucketName = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                glacierStorageUnitEntity.getStorage(), true);

        // Get the SQS queue name from the system configuration.
        String sqsQueueName = configurationHelper.getProperty(ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME);

        // Throw IllegalStateException if SQS queue name is undefined.
        if (StringUtils.isBlank(sqsQueueName))
        {
            throw new IllegalStateException(String.format("SQS queue name not found. Ensure the \"%s\" configuration entry is configured.",
                ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME.getKey()));
        }

        // Build the storage policy transition parameters DTO.
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto =
            new BusinessObjectDataRetryStoragePolicyTransitionDto();
        businessObjectDataRetryStoragePolicyTransitionDto.setBusinessObjectDataKey(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity));
        businessObjectDataRetryStoragePolicyTransitionDto.setStoragePolicyKey(storagePolicyHelper.getStoragePolicyKey(storagePolicyEntity));
        businessObjectDataRetryStoragePolicyTransitionDto.setStoragePolicyVersion(storagePolicyEntity.getVersion());
        businessObjectDataRetryStoragePolicyTransitionDto.setGlacierStorageName(glacierStorageUnitEntity.getStorage().getName());
        businessObjectDataRetryStoragePolicyTransitionDto.setGlacierBucketName(glacierBucketName);
        businessObjectDataRetryStoragePolicyTransitionDto.setGlacierS3KeyPrefix(glacierStorageUnitEntity.getDirectoryPath());
        businessObjectDataRetryStoragePolicyTransitionDto.setSqsQueueName(sqsQueueName);

        // Return the parameters DTO.
        return businessObjectDataRetryStoragePolicyTransitionDto;
    }

    /**
     * Retrieves and validates the Glacier storage unit for the specified business object data.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param glacierStorageEntity the Glacier storage entity
     *
     * @return the Glacier storage unit entity
     */
    private StorageUnitEntity getGlacierStorageUnit(BusinessObjectDataEntity businessObjectDataEntity, StorageEntity glacierStorageEntity)
    {
        // Retrieve and validate glacier storage unit for the business object data.
        StorageUnitEntity glacierStorageUnitEntity =
            storageUnitDao.getStorageUnitByBusinessObjectDataAndStorage(businessObjectDataEntity, glacierStorageEntity);

        // Validate that business object data has no multiple Glacier storage units.
        if (glacierStorageUnitEntity == null)
        {
            throw new IllegalArgumentException(String
                .format("Business object data has no storage unit in \"%s\" storage policy destination storage. Business object data: {%s}",
                    glacierStorageEntity.getName(), businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }
        else
        {
            // Validate that Glacier storage unit is in "ARCHIVING" state.
            Assert.isTrue(StorageUnitStatusEntity.ARCHIVING.equals(glacierStorageUnitEntity.getStatus().getCode()), String
                .format("Business object data is not currently being archived to \"%s\" storage policy destination storage. Business object data: {%s}",
                    glacierStorageEntity.getName(), businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

            // Validate that Glacier storage unit has a non-blank storage directory path.
            Assert.isTrue(StringUtils.isNotBlank(glacierStorageUnitEntity.getDirectoryPath()), String
                .format("Business object data has no storage directory path specified in \"%s\" %s storage. Business object data: {%s}",
                    glacierStorageUnitEntity.getStorage().getName(), StoragePlatformEntity.GLACIER,
                    businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        return glacierStorageUnitEntity;
    }

    /**
     * Retrieves the origin storage unit for the specified Glacier storage unit and validates it.
     *
     * @param glacierStorageUnitEntity the Glacier storage unit
     *
     * @return the origin storage unit entity
     */
    private StorageUnitEntity getOriginStorageUnit(StorageUnitEntity glacierStorageUnitEntity)
    {
        StorageUnitEntity originStorageUnitEntity = glacierStorageUnitEntity.getParentStorageUnit();

        // Validate that the specified Glacier storage unit has a parent S3 storage unit.
        if (originStorageUnitEntity == null || !StoragePlatformEntity.S3.equals(originStorageUnitEntity.getStorage().getStoragePlatform().getName()))
        {
            throw new IllegalArgumentException(String.format("Glacier storage unit in \"%s\" storage has no origin S3 storage unit. Business object data: {%s}",
                glacierStorageUnitEntity.getStorage().getName(),
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(glacierStorageUnitEntity.getBusinessObjectData())));
        }

        // Validate that S3 storage unit is ENABLED.
        if (!StorageUnitStatusEntity.ENABLED.equals(originStorageUnitEntity.getStatus().getCode()))
        {
            throw new IllegalArgumentException(String
                .format("Origin S3 storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. Business object data: {%s}",
                    originStorageUnitEntity.getStorage().getName(), StorageUnitStatusEntity.ENABLED, originStorageUnitEntity.getStatus().getCode(),
                    businessObjectDataHelper.businessObjectDataEntityAltKeyToString(glacierStorageUnitEntity.getBusinessObjectData())));
        }

        return originStorageUnitEntity;
    }

    /**
     * Sends out an SQS message that contains the specified storage policy selection to the SQS queue name.
     *
     * @param sqsQueueName the SQS queue name
     * @param storagePolicySelection the storage policy selection
     */
    private void sendStoragePolicySelectionSqsMessage(String sqsQueueName, StoragePolicySelection storagePolicySelection)
    {
        // Send the storage policy selection to the relative AWS SQS queue.
        AwsParamsDto awsParamsDto = awsHelper.getAwsParamsDto();
        String messageText = null;
        try
        {
            messageText = jsonHelper.objectToJson(storagePolicySelection);
            sqsDao.sendSqsTextMessage(awsParamsDto, sqsQueueName, messageText);
        }
        catch (RuntimeException e)
        {
            LOGGER.error("Failed to publish message to the JMS queue. jmsQueueName=\"{}\" jmsMessagePayload={}", sqsQueueName, messageText);

            // Rethrow the original exception.
            throw e;
        }
    }

    /**
     * Validates a business object data retry storage policy transition request. This method also trims the request parameters.
     *
     * @param request the business object data retry storage policy transition request
     */
    private void validateBusinessObjectDataRetryStoragePolicyTransitionRequest(BusinessObjectDataRetryStoragePolicyTransitionRequest request)
    {
        Assert.notNull(request, "A business object data retry storage policy transition request must be specified.");
        storagePolicyHelper.validateStoragePolicyKey(request.getStoragePolicyKey());
    }
}
