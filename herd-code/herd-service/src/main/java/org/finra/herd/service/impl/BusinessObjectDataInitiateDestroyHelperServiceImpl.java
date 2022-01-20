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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.Tag;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.S3Dao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.dto.BatchJobConfigDto;
import org.finra.herd.model.dto.BusinessObjectDataBatchDestroyDto;
import org.finra.herd.model.dto.BusinessObjectDataBatchRestoreDto;
import org.finra.herd.model.dto.BusinessObjectDataDestroyDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.S3ObjectTaggerRoleParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.RetentionTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.BusinessObjectDataInitiateDestroyHelperService;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageFileHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;

@Service
public class BusinessObjectDataInitiateDestroyHelperServiceImpl implements BusinessObjectDataInitiateDestroyHelperService
{
    /**
     * List of storage unit statuses that are supported by business object data destroy feature.
     */
    private static final List<String> SUPPORTED_STORAGE_UNIT_STATUSES = Collections.unmodifiableList(Arrays
        .asList(StorageUnitStatusEntity.ENABLED, StorageUnitStatusEntity.ARCHIVED, StorageUnitStatusEntity.RESTORED, StorageUnitStatusEntity.DISABLING,
            StorageUnitStatusEntity.DISABLED));

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private S3Dao s3Dao;

    @Autowired
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Autowired
    private StorageFileHelper storageFileHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StorageUnitDao storageUnitDao;

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
    public BusinessObjectData executeInitiateDestroyAfterStep(BusinessObjectDataDestroyDto businessObjectDataDestroyDto)
    {
        return executeInitiateDestroyAfterStepImpl(businessObjectDataDestroyDto);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation executes non-transactionally, suspends the current transaction if one exists.
     */
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void executeS3SpecificSteps(BusinessObjectDataDestroyDto businessObjectDataDestroyDto)
    {
        executeS3SpecificStepsImpl(businessObjectDataDestroyDto);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @PublishNotificationMessages
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void prepareToInitiateDestroy(BusinessObjectDataDestroyDto businessObjectDataDestroyDto, BusinessObjectDataKey businessObjectDataKey)
    {
        prepareToInitiateDestroyImpl(businessObjectDataDestroyDto, businessObjectDataKey);
    }

    /**
     * Executes an after step for initiation of a business object data destroy. This method also updates business object data destroy DTO passed as a
     * parameter.
     *
     * @param businessObjectDataDestroyDto the DTO that holds various parameters needed to initiate a business object data destroy
     *
     * @return the business object data information
     */
    BusinessObjectData executeInitiateDestroyAfterStepImpl(BusinessObjectDataDestroyDto businessObjectDataDestroyDto)
    {
        // Get the business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataDestroyDto.getBusinessObjectDataKey();

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Retrieve storage unit and ensure it exists.
        StorageUnitEntity storageUnitEntity =
            storageUnitDaoHelper.getStorageUnitEntity(businessObjectDataDestroyDto.getStorageName(), businessObjectDataEntity);

        // Validate that storage unit status is DISABLING.
        if (!StorageUnitStatusEntity.DISABLING.equals(storageUnitEntity.getStatus().getCode()))
        {
            throw new IllegalArgumentException(String
                .format("Storage unit status is \"%s\", but must be \"%s\". Storage: {%s}, business object data: {%s}", storageUnitEntity.getStatus().getCode(),
                    StorageUnitStatusEntity.DISABLING, businessObjectDataDestroyDto.getStorageName(),
                    businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        // Set timestamp of when it is OK to finalize deletion of the business object data.
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        storageUnitEntity.setFinalDestroyOn(HerdDateUtils.addDays(currentTime, businessObjectDataDestroyDto.getFinalDestroyInDays()));

        // Change the storage unit status to DISABLED and update the DTO.
        String reason = StorageUnitStatusEntity.DISABLED;
        businessObjectDataDestroyDto.setOldStorageUnitStatus(storageUnitEntity.getStatus().getCode());
        storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.DISABLED, reason);
        businessObjectDataDestroyDto.setNewStorageUnitStatus(storageUnitEntity.getStatus().getCode());

        // Create business object data from the entity and return it.
        return businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);
    }

    /**
     * Executes S3 specific steps required for initiation of a business object data destroy.
     *
     * @param businessObjectDataDestroyDto the DTO that holds various parameters needed to initiate a business object data destroy
     */
    void executeS3SpecificStepsImpl(BusinessObjectDataDestroyDto businessObjectDataDestroyDto)
    {
        // Create an S3 file transfer parameters DTO to access the S3 bucket.
        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = storageHelper.getS3FileTransferRequestParamsDto();
        s3FileTransferRequestParamsDto.setS3Endpoint(businessObjectDataDestroyDto.getS3Endpoint());
        s3FileTransferRequestParamsDto.setS3BucketName(businessObjectDataDestroyDto.getS3BucketName());
        s3FileTransferRequestParamsDto.setS3KeyPrefix(StringUtils.appendIfMissing(businessObjectDataDestroyDto.getS3KeyPrefix(), "/"));

        // Get all S3 objects matching the S3 key prefix from the S3 bucket.
        List<S3VersionSummary> s3VersionSummaries = s3Dao.listVersions(s3FileTransferRequestParamsDto);

        // Check if the operation needs to be executed in batch mode.
        if (businessObjectDataDestroyDto instanceof BusinessObjectDataBatchDestroyDto)
        {
            BatchJobConfigDto jobConfig = ((BusinessObjectDataBatchDestroyDto) businessObjectDataDestroyDto).getJobConfig();
            // Create and execute s3 batch job to tag the S3 objects to initiate deletion
            s3Dao.batchTagVersions(s3FileTransferRequestParamsDto, jobConfig, businessObjectDataDestroyDto.getS3ObjectTaggerRoleParamsDto(), s3VersionSummaries,
                new Tag(businessObjectDataDestroyDto.getS3ObjectTagKey(), businessObjectDataDestroyDto.getS3ObjectTagValue()));
        }
        else
        {
            // Tag the S3 objects to initiate the deletion.
            s3Dao.tagVersions(s3FileTransferRequestParamsDto, businessObjectDataDestroyDto.getS3ObjectTaggerRoleParamsDto(), s3VersionSummaries,
                new Tag(businessObjectDataDestroyDto.getS3ObjectTagKey(), businessObjectDataDestroyDto.getS3ObjectTagValue()));
        }
    }

    /**
     * Get and validates configuration value for the delay in days to complete the business object data destroy operation.
     *
     * @return the delay in days to complete the business object data destroy operation
     */
    int getAndValidateFinalDestroyInDays()
    {
        // Get the configured delay (in days) for business object data finalize destroy.
        int finalDestroyInDays = herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.BDATA_FINAL_DESTROY_DELAY_IN_DAYS);

        // Validate the finalize delay configuration value.
        if (finalDestroyInDays <= 0)
        {
            throw new IllegalStateException(
                String.format("Configuration \"%s\" must be a positive integer.", ConfigurationValue.BDATA_FINAL_DESTROY_DELAY_IN_DAYS.getKey()));
        }

        return finalDestroyInDays;
    }

    /**
     * Retrieves and validates storage unit for the specified business object data. The method makes sure that there is one and only one S3 storage unit.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param businessObjectDataKey the business object data key
     *
     * @return the storage unit entity
     */
    StorageUnitEntity getAndValidateStorageUnit(BusinessObjectDataEntity businessObjectDataEntity, BusinessObjectDataKey businessObjectDataKey)
    {
        // Retrieve all S3 storage units for this business object data.
        List<StorageUnitEntity> s3StorageUnitEntities =
            storageUnitDao.getStorageUnitsByStoragePlatformAndBusinessObjectData(StoragePlatformEntity.S3, businessObjectDataEntity);

        // Validate that business object data has at least one S3 storage unit.
        if (CollectionUtils.isEmpty(s3StorageUnitEntities))
        {
            throw new IllegalArgumentException(String.format("Business object data has no S3 storage unit. Business object data: {%s}",
                businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        // Validate that this business object data has no multiple S3 storage units.
        if (CollectionUtils.size(s3StorageUnitEntities) > 1)
        {
            throw new IllegalArgumentException(String
                .format("Business object data has multiple (%s) %s storage units. Business object data: {%s}", s3StorageUnitEntities.size(),
                    StoragePlatformEntity.S3, businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        // Get the S3 storage unit.
        StorageUnitEntity storageUnitEntity = s3StorageUnitEntities.get(0);

        // Get the storage unit status code.
        String storageUnitStatus = storageUnitEntity.getStatus().getCode();

        // Validate storage unit status.
        if (!BusinessObjectDataInitiateDestroyHelperServiceImpl.SUPPORTED_STORAGE_UNIT_STATUSES.contains(storageUnitStatus))
        {
            throw new IllegalArgumentException(String
                .format("Storage unit status \"%s\" is not supported by the business object data destroy feature. Storage: {%s}, business object data: {%s}",
                    storageUnitStatus, storageUnitEntity.getStorage().getName(),
                    businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        return storageUnitEntity;
    }

    /**
     * Prepares to initiate a business object data destroy process by validating specified business object data along with other related database entities. The
     * method also initializes business object data destroy DTO passed as a parameter.
     *
     * @param businessObjectDataDestroyDto the DTO that holds various parameters needed to initiate a business object data destroy
     * @param businessObjectDataKey the business object data key
     */
    void prepareToInitiateDestroyImpl(BusinessObjectDataDestroyDto businessObjectDataDestroyDto, BusinessObjectDataKey businessObjectDataKey)
    {
        // Validate and trim the business object data key.
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // Get the S3 object tag key to be used to tag the objects for archiving.
        String s3ObjectTagKey = configurationHelper.getRequiredProperty(ConfigurationValue.S3_OBJECT_DELETE_TAG_KEY);

        // Get the S3 object tag value to be used to tag S3 objects for archiving to Glacier.
        String s3ObjectTagValue = configurationHelper.getRequiredProperty(ConfigurationValue.S3_OBJECT_DELETE_TAG_VALUE);

        // Get the ARN of the role to assume to tag S3 objects for archiving to Glacier.
        String s3ObjectTaggerRoleArn = configurationHelper.getRequiredProperty(ConfigurationValue.S3_OBJECT_DELETE_ROLE_ARN);

        // Get the session identifier for the assumed role to be used to tag S3 objects for archiving to Glacier.
        String s3ObjectTaggerRoleSessionName = configurationHelper.getRequiredProperty(ConfigurationValue.S3_OBJECT_DELETE_ROLE_SESSION_NAME);

        // Get duration, in seconds, of the assumed role session.
        Integer s3ObjectTaggerRoleSessionDurationSeconds =
            configurationHelper.getProperty(ConfigurationValue.AWS_ASSUME_S3_TAGGING_ROLE_DURATION_SECS, Integer.class);

        // Get the configured delay (in days) for business object data finalize destroy.
        int finalDestroyInDays = getAndValidateFinalDestroyInDays();

        // Retrieve business object data entity and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Validate business object data including the retention information.
        validateBusinessObjectData(businessObjectDataEntity, businessObjectDataKey);

        // Retrieve and validate a storage unit entity for this business object data.
        StorageUnitEntity storageUnitEntity = getAndValidateStorageUnit(businessObjectDataEntity, businessObjectDataKey);

        // Get the storage entity.
        StorageEntity storageEntity = storageUnitEntity.getStorage();

        // Validate the storage.
        validateStorage(storageUnitEntity.getStorage());

        // Validate that S3 storage has S3 bucket name configured.
        // Please note that since S3 bucket name attribute value is required we pass a "true" flag.
        String s3BucketName = storageHelper
            .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageEntity, true);

        // Get storage specific S3 key prefix for this business object data.
        String s3KeyPrefix = s3KeyPrefixHelper.buildS3KeyPrefix(storageEntity, businessObjectDataEntity.getBusinessObjectFormat(), businessObjectDataKey);

        // Get the storage name.
        String storageName = storageEntity.getName();

        // Validate storage files registered with the storage unit, if they exist.
        storageFileHelper.getAndValidateStorageFilesIfPresent(storageUnitEntity, s3KeyPrefix, storageName, businessObjectDataKey);

        // Validate that this storage does not have any other registered storage files that
        // start with the S3 key prefix, but belong to other business object data instances.
        storageUnitDaoHelper.validateNoExplicitlyRegisteredSubPartitionInStorageForBusinessObjectData(storageUnitEntity.getStorage(),
            businessObjectDataEntity.getBusinessObjectFormat(), businessObjectDataKey, s3KeyPrefix);

        // Change the storage unit status to DISABLING and update the DTO.
        String reason = StorageUnitStatusEntity.DISABLING;
        businessObjectDataDestroyDto.setOldStorageUnitStatus(storageUnitEntity.getStatus().getCode());
        storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, StorageUnitStatusEntity.DISABLING, reason);
        businessObjectDataDestroyDto.setNewStorageUnitStatus(storageUnitEntity.getStatus().getCode());

        // Change the business object data status to DELETED and update the DTO.
        businessObjectDataDestroyDto.setOldBusinessObjectDataStatus(businessObjectDataEntity.getStatus().getCode());
        businessObjectDataDaoHelper.updateBusinessObjectDataStatus(businessObjectDataEntity, BusinessObjectDataStatusEntity.DELETED);
        businessObjectDataDestroyDto.setNewBusinessObjectDataStatus(businessObjectDataEntity.getStatus().getCode());

        // Create and initialize the S3 object tagger role parameters DTO.
        S3ObjectTaggerRoleParamsDto s3ObjectTaggerRoleParamsDto = new S3ObjectTaggerRoleParamsDto();
        s3ObjectTaggerRoleParamsDto.setS3ObjectTaggerRoleArn(s3ObjectTaggerRoleArn);
        s3ObjectTaggerRoleParamsDto.setS3ObjectTaggerRoleSessionName(s3ObjectTaggerRoleSessionName);
        s3ObjectTaggerRoleParamsDto.setS3ObjectTaggerRoleSessionDurationSeconds(s3ObjectTaggerRoleSessionDurationSeconds);

        // Initialize other parameters in the business object data destroy parameters DTO.
        businessObjectDataDestroyDto.setBusinessObjectDataKey(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity));
        businessObjectDataDestroyDto.setStorageName(storageName);
        businessObjectDataDestroyDto.setS3Endpoint(configurationHelper.getProperty(ConfigurationValue.S3_ENDPOINT));
        businessObjectDataDestroyDto.setS3BucketName(s3BucketName);
        businessObjectDataDestroyDto.setS3KeyPrefix(s3KeyPrefix);
        businessObjectDataDestroyDto.setS3ObjectTagKey(s3ObjectTagKey);
        businessObjectDataDestroyDto.setS3ObjectTagValue(s3ObjectTagValue);
        businessObjectDataDestroyDto.setS3ObjectTaggerRoleParamsDto(s3ObjectTaggerRoleParamsDto);
        businessObjectDataDestroyDto.setFinalDestroyInDays(finalDestroyInDays);

        if (businessObjectDataDestroyDto instanceof BusinessObjectDataBatchDestroyDto)
        {
            BatchJobConfigDto batchJobConfig = new BatchJobConfigDto();
            batchJobConfig.setAwsAccountId(configurationHelper.getRequiredProperty(ConfigurationValue.AWS_ACCOUNT_ID));
            batchJobConfig.setS3BatchRoleArn(configurationHelper.getRequiredProperty(ConfigurationValue.S3_BATCH_DESTROY_ROLE_ARN));
            batchJobConfig.setManifestS3BucketName(configurationHelper.getRequiredProperty(ConfigurationValue.S3_BATCH_MANIFEST_BUCKET_NAME));
            batchJobConfig.setManifestS3Prefix(configurationHelper.getRequiredProperty(ConfigurationValue.S3_BATCH_MANIFEST_LOCATION_PREFIX));
            batchJobConfig.setBackoffPeriod(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_BACKOFF_PERIOD, Integer.class));
            batchJobConfig.setMaxAttempts(configurationHelper.getProperty(ConfigurationValue.S3_BATCH_RESTORE_MAX_ATTEMPTS, Integer.class));

            ((BusinessObjectDataBatchDestroyDto) businessObjectDataDestroyDto).setJobConfig(batchJobConfig);
        }
    }

    /**
     * Validate that business object data is supported by the business object data destroy feature.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param businessObjectDataKey the business object data key
     */
    void validateBusinessObjectData(BusinessObjectDataEntity businessObjectDataEntity, BusinessObjectDataKey businessObjectDataKey)
    {
        // Get business object format for this business object data.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectDataEntity.getBusinessObjectFormat();

        // Create a version-less key for the business object format.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(), null);

        // Get the latest version of the format to retrieve retention information.
        BusinessObjectFormatEntity latestVersionBusinessObjectFormatEntity = businessObjectFormatEntity.getLatestVersion() ? businessObjectFormatEntity :
            businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey);

        // Get retention information.
        String retentionType =
            latestVersionBusinessObjectFormatEntity.getRetentionType() != null ? latestVersionBusinessObjectFormatEntity.getRetentionType().getCode() : null;
        Integer retentionPeriodInDays = latestVersionBusinessObjectFormatEntity.getRetentionPeriodInDays();

        // Get the current timestamp from the database.
        Timestamp currentTimestamp = herdDao.getCurrentTimestamp();

        // Validate that retention information is specified for this business object format.
        if (retentionType != null)
        {
            switch (retentionType)
            {
                case RetentionTypeEntity.PARTITION_VALUE:
                    Assert.notNull(retentionPeriodInDays,
                        String.format("Retention period in days must be specified for %s retention type.", RetentionTypeEntity.PARTITION_VALUE));
                    Assert.isTrue(retentionPeriodInDays > 0,
                        String.format("A positive retention period in days must be specified for %s retention type.", RetentionTypeEntity.PARTITION_VALUE));

                    // Try to convert business object data primary partition value to a timestamp.
                    // If conversion is not successful, the method returns a null value.
                    Date primaryPartitionValue = businessObjectDataHelper.getDateFromString(businessObjectDataEntity.getPartitionValue());

                    // If primary partition values is not a date, this business object data is not supported by the business object data destroy feature.
                    if (primaryPartitionValue == null)
                    {
                        throw new IllegalArgumentException(String
                            .format("Primary partition value \"%s\" cannot get converted to a valid date. Business object data: {%s}",
                                businessObjectDataEntity.getPartitionValue(), businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
                    }

                    // Compute the relative primary partition value threshold date based on the current timestamp and retention period value.
                    Date primaryPartitionValueThreshold = new Date(HerdDateUtils.addDays(currentTimestamp, -retentionPeriodInDays).getTime());

                    // Validate that this business object data has it's primary partition value before or equal to the threshold date.
                    if (primaryPartitionValue.compareTo(primaryPartitionValueThreshold) > 0)
                    {
                        throw new IllegalArgumentException(String.format(
                            "Business object data fails retention threshold check for retention type \"%s\" with retention period of %d days. " +
                                "Business object data: {%s}", retentionType, retentionPeriodInDays,
                            businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
                    }
                    break;
                case RetentionTypeEntity.BDATA_RETENTION_DATE:
                    // Retention period in days value must only be specified for PARTITION_VALUE retention type.
                    Assert.isNull(retentionPeriodInDays, String.format("A retention period in days cannot be specified for %s retention type.", retentionType));

                    // Validate that the retention information is specified for business object data with retention type as BDATA_RETENTION_DATE.
                    Assert.notNull(businessObjectDataEntity.getRetentionExpiration(), String
                        .format("Retention information with retention type %s must be specified for the Business Object Data: {%s}", retentionType,
                            businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));

                    // Validate that the business object data retention expiration date is in the past.
                    if (!(businessObjectDataEntity.getRetentionExpiration().before(currentTimestamp)))
                    {
                        throw new IllegalArgumentException(String.format(
                            "Business object data fails retention threshold check for retention type \"%s\" with retention expiration date %s. " +
                                "Business object data: {%s}", retentionType, businessObjectDataEntity.getRetentionExpiration(),
                            businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
                    }
                    break;
                default:
                    throw new IllegalArgumentException(String
                        .format("Retention type \"%s\" is not supported by the business object data destroy feature. Business object format: {%s}",
                            retentionType, businessObjectFormatHelper.businessObjectFormatKeyToString(businessObjectFormatKey)));
            }
        }
        else
        {
            throw new IllegalArgumentException(String
                .format("Retention information is not configured for the business object format. Business object format: {%s}",
                    businessObjectFormatHelper.businessObjectFormatKeyToString(businessObjectFormatKey)));
        }
    }

    /**
     * Validates the storage.
     *
     * @param storageEntity the storage entity
     */
    void validateStorage(StorageEntity storageEntity)
    {
        // Validate that storage policy filter storage has the S3 path prefix validation enabled.
        if (!storageHelper
            .getBooleanStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), storageEntity,
                false, true))
        {
            throw new IllegalStateException(String.format("Path prefix validation must be enabled on \"%s\" storage.", storageEntity.getName()));
        }
    }
}
