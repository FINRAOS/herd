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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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
import org.finra.herd.dao.impl.AbstractHerdDao;
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

/**
 * The file upload cleanup service implementation.
 */
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
    public static final List<StoragePolicyPriorityLevel> STORAGE_POLICY_PRIORITY_LEVELS = Collections.unmodifiableList(Arrays
        .asList(new StoragePolicyPriorityLevel(false, false, false), new StoragePolicyPriorityLevel(false, true, true),
            new StoragePolicyPriorityLevel(true, false, false), new StoragePolicyPriorityLevel(true, true, true)));

    /**
     * List of business object data statuses that storage policies apply to.
     */
    public static final List<String> SUPPORTED_BUSINESS_OBJECT_DATA_STATUSES = Collections
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
        // Create a result list.
        List<StoragePolicySelection> resultStoragePolicySelections = new ArrayList<>();

        // Get the current timestamp from the database.
        Timestamp currentTimestamp = herdDao.getCurrentTimestamp();

        // Get the threshold in days since business object data registration update for business object data to be selectable
        // by a storage policy with DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE storage policy rule type.
        int updatedOnThresholdInDays =
            herdStringHelper.getConfigurationValueAsInteger(ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_UPDATED_ON_THRESHOLD_DAYS);

        // Compute business object data "updated on" threshold timestamp based on
        // the current database timestamp and the threshold value configured in the system.
        Timestamp updatedOnThresholdTimestamp = HerdDateUtils.addDays(currentTimestamp, -updatedOnThresholdInDays);

        // Keep track of all business object data entities selected per storage policies. This is need to avoid a lower priority selection policy
        // to be executed ahead of a higher priority one.
        Set<BusinessObjectDataEntity> selectedBusinessObjectDataEntities = new LinkedHashSet<>();

        // Separately process all possible storage policy priority levels in order of priorities. This is done to assure that higher priority level storage
        // policies will be listed earlier in the final result map.
        for (StoragePolicyPriorityLevel storagePolicyPriorityLevel : STORAGE_POLICY_PRIORITY_LEVELS)
        {
            // Until we reach maximum number of results or run out of entities to select, retrieve and process business object data entities mapped to their
            // corresponding storage policy entities, where the business object data status is supported by the storage policy feature and the business object
            // data alternate key values match storage policy's filter and transition (not taking into account storage policy rules).
            int startPosition = 0;
            while (true)
            {
                Map<BusinessObjectDataEntity, StoragePolicyEntity> map = businessObjectDataDao
                    .getBusinessObjectDataEntitiesMatchingStoragePolicies(storagePolicyPriorityLevel, SUPPORTED_BUSINESS_OBJECT_DATA_STATUSES, startPosition,
                        maxResult);

                for (Map.Entry<BusinessObjectDataEntity, StoragePolicyEntity> entry : map.entrySet())
                {
                    BusinessObjectDataEntity businessObjectDataEntity = entry.getKey();

                    // Process this storage policy selection, only if this business object data has not been selected earlier.
                    if (!selectedBusinessObjectDataEntities.contains(businessObjectDataEntity))
                    {
                        // Initialize a flag.
                        boolean createStoragePolicySelection = false;

                        // Remember that we got this business object data entity as matching to a storage policy.
                        // This is done so we would not try to select this business object data again later by a lower level storage policy.
                        selectedBusinessObjectDataEntities.add(businessObjectDataEntity);

                        // Get the storage policy entity, so we can validate the storage policy rule against this business object data.
                        StoragePolicyEntity storagePolicyEntity = entry.getValue();

                        // Check if business object data matches the storage policy rule.
                        if (StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED.equals(storagePolicyEntity.getStoragePolicyRuleType().getCode()))
                        {
                            // For DAYS_SINCE_BDATA_REGISTERED storage policy rule type, select business object data based on it's "created on" timestamp.

                            // Compute the business object data "created on " threshold timestamp
                            // based on the current database timestamp and storage policy rule value.
                            Timestamp createdOnThresholdTimestamp = HerdDateUtils.addDays(currentTimestamp, -storagePolicyEntity.getStoragePolicyRuleValue());

                            // Select this business object data per this storage policy if it has
                            // "created on" timestamp before or equal to the threshold timestamp.
                            createStoragePolicySelection = (businessObjectDataEntity.getCreatedOn().compareTo(createdOnThresholdTimestamp) <= 0);
                        }
                        else if (StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE
                            .equals(storagePolicyEntity.getStoragePolicyRuleType().getCode()))
                        {
                            // For DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE storage policy rule type, select business object
                            // data based on both it's primary partition value compared against storage policy rule value
                            // and "updated on" timestamp being below the threshold configured in the system.

                            // For this storage policy rule, we ignore this business object data
                            // if it was updated earlier than the threshold value of days ago.
                            if (businessObjectDataEntity.getUpdatedOn().compareTo(updatedOnThresholdTimestamp) <= 0)
                            {
                                // Try to convert business object data primary partition value to a timestamp.
                                // If it is not a date, the storage policy rule is not matching this business object data.
                                Date primaryPartitionValue = getDateFromString(businessObjectDataEntity.getPartitionValue());

                                // For this storage policy rule, we ignore this business data if primary partition value is not a date.
                                if (primaryPartitionValue != null)
                                {
                                    // Compute the relative primary partition value threshold date
                                    // based on the current database timestamp and storage policy rule value.
                                    Date primaryPartitionValueThresholdDate =
                                        new Date(HerdDateUtils.addDays(currentTimestamp, -storagePolicyEntity.getStoragePolicyRuleValue()).getTime());

                                    // Select this business object data per this storage policy if it has
                                    // primary partition value before or equal to the threshold date.
                                    createStoragePolicySelection = (primaryPartitionValue.compareTo(primaryPartitionValueThresholdDate) <= 0);
                                }
                            }
                        }
                        // Fail on an un-supported storage policy rule type.
                        else
                        {
                            throw new IllegalStateException(
                                String.format("Storage policy type \"%s\" is not supported.", storagePolicyEntity.getStoragePolicyRuleType().getCode()));
                        }

                        // If this business object data got selected, create a storage policy selection and add it to the result list.
                        if (createStoragePolicySelection)
                        {
                            // Create and add a storage policy selection to the result list.
                            StoragePolicySelection storagePolicySelection = new StoragePolicySelection();
                            resultStoragePolicySelections.add(storagePolicySelection);
                            storagePolicySelection.setBusinessObjectDataKey(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity));
                            storagePolicySelection
                                .setStoragePolicyKey(new StoragePolicyKey(storagePolicyEntity.getNamespace().getCode(), storagePolicyEntity.getName()));
                            storagePolicySelection.setStoragePolicyVersion(storagePolicyEntity.getVersion());

                            // Log the storage policy selection.
                            LOGGER.info("Selected business object data for storage policy processing: " +
                                "businessObjectDataKey={} storagePolicyKey={} storagePolicyVersion={}",
                                jsonHelper.objectToJson(storagePolicySelection.getBusinessObjectDataKey()),
                                jsonHelper.objectToJson(storagePolicySelection.getStoragePolicyKey()), storagePolicySelection.getStoragePolicyVersion());

                            // Stop adding storage policy selections to the result list if we reached the max result limit.
                            if (resultStoragePolicySelections.size() >= maxResult)
                            {
                                break;
                            }
                        }
                    }
                }

                // Stop processing storage policies if we reached the max result limit or there are no more business object data to select.
                if (resultStoragePolicySelections.size() >= maxResult || map.isEmpty())
                {
                    break;
                }

                // Increase the start position for the next select.
                startPosition += maxResult;
            }

            // Stop processing storage policies if we reached the max result limit.
            if (resultStoragePolicySelections.size() >= maxResult)
            {
                break;
            }
        }

        // Send all storage policy selections to the specified SQS queue.
        sendStoragePolicySelectionToSqsQueue(sqsQueueName, resultStoragePolicySelections);

        return resultStoragePolicySelections;
    }

    /**
     * Gets a date in a date format from a string format or null if one wasn't specified. The format of the date should match
     * HerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.
     *
     * @param dateString the date as a string
     *
     * @return the date as a date or null if one wasn't specified or the conversion fails
     */
    private Date getDateFromString(String dateString)
    {
        Date resultDate = null;

        // For strict date parsing, process the date string only if it has the required length.
        if (dateString.length() == AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK.length())
        {
            // Try to convert the date string to a Date.
            try
            {
                // Use strict parsing to ensure our date is more definitive.
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK, Locale.US);
                simpleDateFormat.setLenient(false);
                resultDate = simpleDateFormat.parse(dateString);
            }
            catch (ParseException e)
            {
                // This assignment is here to pass PMD checks.
                resultDate = null;
            }
        }

        return resultDate;
    }

    /**
     * Sends storage policy selections to the specified AWS SQS queue.
     *
     * @param sqsQueueName the SQS queue name to send storage policy selections to
     * @param storagePolicySelections the list of storage policy selections
     */
    private void sendStoragePolicySelectionToSqsQueue(String sqsQueueName, List<StoragePolicySelection> storagePolicySelections)
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
                LOGGER.error("Failed to publish message to the JMS queue. jmsQueueName=\"{}\" jmsMessagePayload={}", sqsQueueName, messageText);

                // Throw the exception up.
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }
}
