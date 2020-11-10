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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.SqsDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.StoragePolicyPriorityLevel;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.service.StoragePolicySelectorService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;

@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class StoragePolicySelectorServiceImpl implements StoragePolicySelectorService
{
    /**
     * List of storage policy priority levels in order of priorities, highest priority listed first: <p><ul> <li>Storage policy filter has business object
     * definition, business object format usage, and business object format file type specified <li>Storage policy filter has only business object definition
     * specified <li>Storage policy filter has only business object format usage and business object format file type specified <li>Storage policy filter has no
     * fields specified </ul>
     */
    static final List<StoragePolicyPriorityLevel> STORAGE_POLICY_PRIORITY_LEVELS = Collections.unmodifiableList(Arrays
        .asList(new StoragePolicyPriorityLevel(false, false, false), new StoragePolicyPriorityLevel(false, true, true),
            new StoragePolicyPriorityLevel(true, false, false), new StoragePolicyPriorityLevel(true, true, true)));

    /**
     * List of business object data statuses that storage policies apply to.
     */
    static final List<String> SUPPORTED_BUSINESS_OBJECT_DATA_STATUSES = Collections
        .unmodifiableList(Arrays.asList(BusinessObjectDataStatusEntity.VALID, BusinessObjectDataStatusEntity.INVALID, BusinessObjectDataStatusEntity.EXPIRED));

    private static final Logger LOGGER = LoggerFactory.getLogger(StoragePolicySelectorServiceImpl.class);

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private SqsDao sqsDao;

    @Override
    public List<StoragePolicySelection> execute(String sqsQueueName, int maxResult)
    {
        // Create a result map. We use the map here to prevent duplicate selection of business object data entities.
        // Please note that we use linked has map here in order to preserve the order of selections.
        Map<BusinessObjectDataEntity, StoragePolicySelection> storagePolicySelectionMap = new LinkedHashMap<>();

        // Get the current timestamp from the database.
        Timestamp currentTimestamp = herdDao.getCurrentTimestamp();

        // Get the threshold in days since business object data registration update for business object data to be selectable
        // by a storage policy with DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE storage policy rule type.
        int updatedOnThresholdInDays =
            herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_UPDATED_ON_THRESHOLD_DAYS);

        // Get the maximum number of failed storage policy transition attempts before the relative storage unit gets excluded from
        // being selected per storage policies by the storage policy selector system job. 0 means the maximum is not set.
        int maxAllowedTransitionAttempts = herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.STORAGE_POLICY_TRANSITION_MAX_ALLOWED_ATTEMPTS);

        LOGGER.info("{}={} {}={}", ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_UPDATED_ON_THRESHOLD_DAYS.getKey(), updatedOnThresholdInDays,
            ConfigurationValue.STORAGE_POLICY_TRANSITION_MAX_ALLOWED_ATTEMPTS.getKey(), maxAllowedTransitionAttempts);

        // Compute business object data "updated on" threshold timestamp based on
        // the current database timestamp and the threshold value configured in the system.
        Timestamp updatedOnThresholdTimestamp = HerdDateUtils.addDays(currentTimestamp, -updatedOnThresholdInDays);

        // First, we want to process all storage policies level by level that are configured to ignore latest valid versions. This is done, in order for the
        // regular policies, possibly from the same priority level, to still be able to select the latest valid business object data entities that got selected
        // for rule checking by the policies that that are configured to ignore latest valid versions. The second scan is for all policies that are not
        // configured to ignore latest valid versions.
        for (Boolean doNotTransitionLatestValid : new Boolean[] {true, false})
        {
            // Keep track of all business object data entities selected per storage policies. This is need to avoid a lower priority selection policy to be
            // executed ahead of a higher priority one. Please note that this check also implies that, for each of the two scans that we perform, any business
            // object data could be selected for checking applicable rules only by one storage policy regardless of storage policy priority level.
            Set<BusinessObjectDataEntity> businessObjectDataEntitiesSelectedForReview = new HashSet<>();

            // Separately process all possible storage policy priority levels in order of priorities. This is done to assure that higher priority level storage
            // policies will be listed earlier in the final result map.
            for (StoragePolicyPriorityLevel storagePolicyPriorityLevel : STORAGE_POLICY_PRIORITY_LEVELS)
            {
                // Until we reach maximum number of results or run out of entities to select, retrieve and process business object data entities mapped to their
                // corresponding storage policy entities, where the business object data status is supported by the storage policy feature and the business
                // object data alternate key values match storage policy's filter and transition (not taking into account storage policy rules).
                int startPosition = 0;
                while (true)
                {
                    Map<BusinessObjectDataEntity, StoragePolicyEntity> map = businessObjectDataDao
                        .getBusinessObjectDataEntitiesMatchingStoragePolicies(storagePolicyPriorityLevel, doNotTransitionLatestValid,
                            SUPPORTED_BUSINESS_OBJECT_DATA_STATUSES, maxAllowedTransitionAttempts, startPosition, maxResult);

                    // If we are processing storage policies that are configured to ignore latest valid business object data, then find all latest valid
                    // business object data that is present in this chunk of selected business object data matching storage policies.
                    Set<BusinessObjectDataEntity> latestValidBusinessObjectDataEntities =
                        doNotTransitionLatestValid ? businessObjectDataHelper.getLatestValidBusinessObjectDataEntities(new ArrayList<>(map.keySet())) : null;

                    // Process all business object data entities that are matching storage polices.
                    for (Map.Entry<BusinessObjectDataEntity, StoragePolicyEntity> entry : map.entrySet())
                    {
                        // Get business object data entity.
                        BusinessObjectDataEntity businessObjectDataEntity = entry.getKey();

                        // Process this storage policy selection, only if this business object data entity has not been selected for review earlier. This is
                        // needed in order to avoid a lower priority selection policy to be executed ahead of a higher priority one. Please note that we clear
                        // the set that tracks business object data entities selected for review between the scans. This is needed, so storage policies that
                        // are not configured to ignore the latest valid business object data would still be able to select business object data entities that
                        // got selected for rule checking by the policies configured to ignore latest valid versions.  The below check also implies that, for
                        // each of the two scans that we perform, any business object data could be selected for checking applicable rules only by one storage
                        // policy regardless of storage policy priority level. Since two sets of storage policies configured and not configured to ignore
                        // the latest valid business object data select data independent from each other, we also check against all already created storage
                        // policy selections. This is needed in order to avoid selecting the same business object data entity for transition twice.
                        if (!businessObjectDataEntitiesSelectedForReview.contains(businessObjectDataEntity) &&
                            !storagePolicySelectionMap.containsKey(businessObjectDataEntity))
                        {
                            boolean createStoragePolicySelection = false;

                            // Remember that we got this business object data entity as matching to a storage policy.
                            // This is done so we would not try to select this business object data again later by a lower level storage policy.
                            businessObjectDataEntitiesSelectedForReview.add(businessObjectDataEntity);

                            // Get the storage policy entity, so we can validate the storage policy rule against this business object data.
                            StoragePolicyEntity storagePolicyEntity = entry.getValue();

                            // Do not select this business object data if it was identified as a latest valid business object data and the storage policy
                            // is configured to ignore latest valid versions.
                            if (!doNotTransitionLatestValid || !latestValidBusinessObjectDataEntities.contains(businessObjectDataEntity))
                            {
                                // Get a storage policy rule type and value.
                                String storagePolicyRuleType = storagePolicyEntity.getStoragePolicyRuleType().getCode();
                                Integer storagePolicyRuleValue = storagePolicyEntity.getStoragePolicyRuleValue();

                                // For DAYS_SINCE_BDATA_REGISTERED storage policy rule type, select business object data based on it's "created on" timestamp.
                                if (StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED.equals(storagePolicyRuleType))
                                {
                                    // Compute "created on" threshold timestamp based on the current timestamp and storage policy rule value.
                                    Timestamp createdOnThresholdTimestamp = HerdDateUtils.addDays(currentTimestamp, -storagePolicyRuleValue);

                                    // Select this business object data if it has "created on" timestamp before or equal to the threshold timestamp.
                                    createStoragePolicySelection = (businessObjectDataEntity.getCreatedOn().compareTo(createdOnThresholdTimestamp) <= 0);
                                }
                                // For DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE storage policy rule type, select business object data based on both it's
                                // primary partition value compared against storage policy rule value and "updated on" timestamp being below the threshold.
                                else if (StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE.equals(storagePolicyRuleType))
                                {
                                    // For this storage policy rule, we ignore this business object data entity
                                    // if it was updated earlier than the threshold value of days ago.
                                    if (businessObjectDataEntity.getUpdatedOn().compareTo(updatedOnThresholdTimestamp) <= 0)
                                    {
                                        // Try to convert business object data primary partition value to a timestamp.
                                        // If it is not a date, the storage policy rule is not matching this business object data.
                                        Date primaryPartitionValue = businessObjectDataHelper.getDateFromString(businessObjectDataEntity.getPartitionValue());

                                        // For this storage policy rule, we ignore this business data if primary partition value is not a date.
                                        if (primaryPartitionValue != null)
                                        {
                                            // Compute the relative primary partition value threshold date based
                                            // on the current timestamp and storage policy rule value.
                                            Date primaryPartitionValueThreshold =
                                                new Date(HerdDateUtils.addDays(currentTimestamp, -storagePolicyRuleValue).getTime());

                                            // Select this business object data if it has it's primary partition value before or equal to the threshold date.
                                            createStoragePolicySelection = (primaryPartitionValue.compareTo(primaryPartitionValueThreshold) <= 0);
                                        }
                                    }
                                }
                                // Fail on an un-supported storage policy rule type.
                                else
                                {
                                    throw new IllegalStateException(String.format("Storage policy type \"%s\" is not supported.", storagePolicyRuleType));
                                }
                            }

                            // If this business object data got selected, create a storage policy selection and add it to the result list.
                            if (createStoragePolicySelection)
                            {
                                // Create business object data key and storage policy key per selected entities.
                                BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);
                                StoragePolicyKey storagePolicyKey =
                                    new StoragePolicyKey(storagePolicyEntity.getNamespace().getCode(), storagePolicyEntity.getName());

                                // Create and add a storage policy selection to the result map.
                                storagePolicySelectionMap.put(businessObjectDataEntity,
                                    new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, storagePolicyEntity.getVersion()));

                                LOGGER.info("Selected business object data for storage policy processing: " +
                                        "businessObjectDataKey={} storagePolicyKey={} storagePolicyVersion={}", jsonHelper.objectToJson(businessObjectDataKey),
                                    jsonHelper.objectToJson(storagePolicyKey), storagePolicyEntity.getVersion());

                                // Stop adding storage policy selections to the result map if we reached the maximum results limit.
                                if (storagePolicySelectionMap.size() >= maxResult)
                                {
                                    break;
                                }
                            }
                        }
                    }

                    // Stop processing storage policies if we reached the max result limit or there are no more business object data to select.
                    if (storagePolicySelectionMap.size() >= maxResult || map.isEmpty())
                    {
                        break;
                    }

                    // Increment start position for the next select.
                    startPosition += maxResult;
                }

                // Stop processing storage policies if we reached the max result limit.
                if (storagePolicySelectionMap.size() >= maxResult)
                {
                    break;
                }
            }
        }

        // Get the list of all storage policy selections.
        List<StoragePolicySelection> storagePolicySelections = new ArrayList<>(storagePolicySelectionMap.values());

        // Send all storage policy selections to the specified SQS queue.
        sendStoragePolicySelectionToSqsQueue(sqsQueueName, storagePolicySelections);

        // Return the selections.
        return storagePolicySelections;
    }

    /**
     * Sends storage policy selections to the specified AWS SQS queue.
     *
     * @param sqsQueueName the SQS queue name to send storage policy selections to
     * @param storagePolicySelections the list of storage policy selections
     */
    private void sendStoragePolicySelectionToSqsQueue(String sqsQueueName, List<StoragePolicySelection> storagePolicySelections)
    {
        if (CollectionUtils.isNotEmpty(storagePolicySelections))
        {
            AwsParamsDto awsParamsDto = awsHelper.getAwsParamsDto();

            for (StoragePolicySelection storagePolicySelection : storagePolicySelections)
            {
                String messageText = null;

                try
                {
                    messageText = jsonHelper.objectToJson(storagePolicySelection);
                    sqsDao.sendMessage(awsParamsDto, sqsQueueName, messageText, null);
                }
                catch (Exception e)
                {
                    // Log the error and throw the exception up.
                    LOGGER.error("Failed to publish message to the JMS queue. jmsQueueName=\"{}\" jmsMessagePayload={}", sqsQueueName, messageText);
                    throw new IllegalStateException(e.getMessage(), e);
                }
            }
        }
    }
}
