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
package org.finra.herd.service.helper;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
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
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

@Component
public class BusinessObjectDataRetryStoragePolicyTransitionHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessObjectDataRetryStoragePolicyTransitionHelper.class);

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
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Autowired
    private SqsDao sqsDao;

    @Autowired
    private StorageFileDao storageFileDao;

    @Autowired
    private StoragePolicyDaoHelper storagePolicyDaoHelper;

    @Autowired
    private StoragePolicyHelper storagePolicyHelper;

    @Autowired
    private StorageUnitDao storageUnitDao;

    /**
     * Executes a retry of the storage policy transition and return the business object data information.
     *
     * @param businessObjectDataKey the business object data key
     * @param request the information needed to retry a storage policy transition
     *
     * @return the business object data information
     */
    public BusinessObjectData retryStoragePolicyTransition(BusinessObjectDataKey businessObjectDataKey,
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

        // Validate that storage policy filter matches this business object data, except for the storage.
        Assert.isTrue((storagePolicyEntity.getBusinessObjectDefinition() == null ||
            storagePolicyEntity.getBusinessObjectDefinition().equals(businessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectDefinition())) &&
            (StringUtils.isBlank(storagePolicyEntity.getUsage()) ||
                storagePolicyEntity.getUsage().equalsIgnoreCase(businessObjectDataEntity.getBusinessObjectFormat().getUsage())) &&
            (storagePolicyEntity.getFileType() == null ||
                storagePolicyEntity.getFileType().equals(businessObjectDataEntity.getBusinessObjectFormat().getFileType())), String
            .format("Business object data does not match storage policy filter. " + "Storage policy: {%s}, business object data: {%s}",
                storagePolicyHelper.storagePolicyKeyAndVersionToString(request.getStoragePolicyKey(), storagePolicyEntity.getVersion()),
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));

        // Validate the storage policy filter storage.
        storagePolicyDaoHelper.validateStoragePolicyFilterStorage(storagePolicyEntity.getStorage());

        // Retrieve and validate a storage unit for this business object data.
        StorageUnitEntity storageUnitEntity = getStorageUnit(businessObjectDataEntity, storagePolicyEntity.getStorage());

        // Get S3 key prefix for this business object data.
        String s3KeyPrefix = s3KeyPrefixHelper
            .buildS3KeyPrefix(storagePolicyEntity.getStorage(), storageUnitEntity.getBusinessObjectData().getBusinessObjectFormat(), businessObjectDataKey);

        // Retrieve storage files registered with this business object data in the  storage.
        int storageFilesCount = storageUnitEntity.getStorageFiles().size();

        // Validate that we have storage files registered in the storage.
        Assert.isTrue(storageFilesCount > 0, String.format("Business object data has no storage files registered in \"%s\" storage. Business object data: {%s}",
            storageUnitEntity.getStorage().getName(), businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));

        // Retrieve all registered storage files from the storage that start with the S3 key prefix.
        // Since the S3 key prefix represents a directory, we add a trailing '/' character to it.
        String s3KeyPrefixWithTrailingSlash = StringUtils.appendIfMissing(s3KeyPrefix, "/");
        Long registeredStorageFilesMatchingS3KeyPrefixCount =
            storageFileDao.getStorageFileCount(storageUnitEntity.getStorage().getName(), s3KeyPrefixWithTrailingSlash);

        // Sanity check for the S3 key prefix.
        if (registeredStorageFilesMatchingS3KeyPrefixCount.intValue() != storageFilesCount)
        {
            throw new IllegalArgumentException(String.format(
                "Number of storage files (%d) registered for the business object data in \"%s\" storage is not equal to " +
                    "the number of registered storage files (%d) matching \"%s\" S3 key prefix in the same storage. Business object data: {%s}",
                storageFilesCount, storageUnitEntity.getStorage().getName(), registeredStorageFilesMatchingS3KeyPrefixCount, s3KeyPrefixWithTrailingSlash,
                businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
        }

        // Get the SQS queue name from the system configuration.
        String sqsQueueName = configurationHelper.getProperty(ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME);

        // Throw IllegalStateException if SQS queue name is not defined.
        if (StringUtils.isBlank(sqsQueueName))
        {
            throw new IllegalStateException(String.format("SQS queue name not found. Ensure the \"%s\" configuration entry is configured.",
                ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME.getKey()));
        }

        // Create a storage policy selection.
        StoragePolicySelection storagePolicySelection =
            new StoragePolicySelection(businessObjectDataKey, storagePolicyHelper.getStoragePolicyKey(storagePolicyEntity), storagePolicyEntity.getVersion());

        // Executes SQS specific steps needed to retry a storage policy transition.
        sendStoragePolicySelectionSqsMessage(sqsQueueName, storagePolicySelection);

        // Create and return the business object data object from the entity.
        return businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);
    }

    /**
     * Sends out an SQS message that contains the specified storage policy selection to the SQS queue name.
     *
     * @param sqsQueueName the SQS queue name
     * @param storagePolicySelection the storage policy selection
     */
    protected void sendStoragePolicySelectionSqsMessage(String sqsQueueName, StoragePolicySelection storagePolicySelection)
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
     * Retrieves and validates the storage unit for the specified business object data.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param storageEntity the storage entity
     *
     * @return the storage unit entity
     */
    private StorageUnitEntity getStorageUnit(BusinessObjectDataEntity businessObjectDataEntity, StorageEntity storageEntity)
    {
        // Retrieve and validate storage unit for the business object data.
        StorageUnitEntity storageUnitEntity = storageUnitDao.getStorageUnitByBusinessObjectDataAndStorage(businessObjectDataEntity, storageEntity);

        // Validate the storage unit.
        if (storageUnitEntity == null)
        {
            throw new IllegalArgumentException(String
                .format("Business object data has no storage unit in \"%s\" storage. Business object data: {%s}", storageEntity.getName(),
                    businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }
        else
        {
            // Validate that storage unit is in "ARCHIVING" state.
            Assert.isTrue(StorageUnitStatusEntity.ARCHIVING.equals(storageUnitEntity.getStatus().getCode()), String.format(
                "Business object data is not currently being archived. " +
                    "Storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. Business object data: {%s}",
                storageEntity.getName(), StorageUnitStatusEntity.ARCHIVING, storageUnitEntity.getStatus().getCode(),
                businessObjectDataHelper.businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        return storageUnitEntity;
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
