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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.SqsDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.StoragePolicyPriorityLevel;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.service.StoragePolicySelectorService;
import org.finra.herd.service.helper.HerdDaoHelper;

/**
 * The file upload cleanup service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class StoragePolicySelectorServiceImpl implements StoragePolicySelectorService
{
    /**
     * List of business object data statuses that storage policies apply to.
     */
    public static final List<String> SUPPORTED_BUSINESS_OBJECT_DATA_STATUSES = Collections
        .unmodifiableList(Arrays.asList(BusinessObjectDataStatusEntity.VALID, BusinessObjectDataStatusEntity.INVALID, BusinessObjectDataStatusEntity.EXPIRED));

    /**
     * List of storage policy priority levels in order of priorities, highest priority listed first: <p><ul> <li>Storage policy filter has business object
     * definition, business object format usage, and business object format file type specified <li>Storage policy filter has only business object definition
     * specified <li>Storage policy filter has only business object format usage and business object format file type specified <li>Storage policy filter has no
     * fields specified </ul>
     */
    public static final List<StoragePolicyPriorityLevel> STORAGE_POLICY_PRIORITY_LEVELS = Collections.unmodifiableList(Arrays
        .asList(new StoragePolicyPriorityLevel(false, false, false), new StoragePolicyPriorityLevel(false, true, true),
            new StoragePolicyPriorityLevel(true, false, false), new StoragePolicyPriorityLevel(true, true, true)));

    private static final Logger LOGGER = Logger.getLogger(StoragePolicySelectorServiceImpl.class);

    @Autowired
    private HerdDao herdDao;

    @Autowired
    private HerdDaoHelper herdDaoHelper;

    @Autowired
    private SqsDao sqsDao;

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private JsonHelper jsonHelper;

    /**
     * {@inheritDoc}
     */
    @Override
    public List<StoragePolicySelection> execute(String sqsQueueName, int maxResult)
    {
        // Create a result list.
        List<StoragePolicySelection> resultStoragePolicySelections = new ArrayList<>();

        // Get the current timestamp from the database.
        Timestamp currentTimestamp = herdDao.getCurrentTimestamp();

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
                Map<BusinessObjectDataEntity, StoragePolicyEntity> map = herdDao
                    .getBusinessObjectDataEntitiesMatchingStoragePolicies(storagePolicyPriorityLevel, SUPPORTED_BUSINESS_OBJECT_DATA_STATUSES, startPosition,
                        maxResult);

                for (Map.Entry<BusinessObjectDataEntity, StoragePolicyEntity> entry : map.entrySet())
                {
                    BusinessObjectDataEntity businessObjectDataEntity = entry.getKey();

                    // Process this storage policy selection, only if this business object data has not been selected earlier.
                    if (!selectedBusinessObjectDataEntities.contains(businessObjectDataEntity))
                    {
                        // Remember that we got this business object data entity selected by a storage policy.
                        selectedBusinessObjectDataEntities.add(businessObjectDataEntity);

                        StoragePolicyEntity storagePolicyEntity = entry.getValue();

                        // For DAYS_SINCE_BDATA_REGISTERED storage policy rule type, select business object data based on it "created on" timestamp.
                        if (StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED.equals(storagePolicyEntity.getStoragePolicyRuleType().getCode()))
                        {
                            // Compute threshold timestamp based on the current database timestamp and storage policy rule value.
                            Timestamp thresholdTimestamp = HerdDateUtils.addDays(currentTimestamp, -storagePolicyEntity.getStoragePolicyRuleValue());

                            // Select this business object data if it has "created on" timestamp before the threshold timestamp.
                            if (businessObjectDataEntity.getCreatedOn().compareTo(thresholdTimestamp) < 0)
                            {
                                // Add this storage policy selection to the result list.
                                StoragePolicySelection storagePolicySelection = new StoragePolicySelection();
                                resultStoragePolicySelections.add(storagePolicySelection);
                                storagePolicySelection.setBusinessObjectDataKey(herdDaoHelper.getBusinessObjectDataKey(businessObjectDataEntity));
                                storagePolicySelection
                                    .setStoragePolicyKey(new StoragePolicyKey(storagePolicyEntity.getNamespace().getCode(), storagePolicyEntity.getName()));
                            }

                            // Stop adding storage policy selections to the result list if we reached the max result limit.
                            if (resultStoragePolicySelections.size() >= maxResult)
                            {
                                break;
                            }
                        }
                        // Fail on un-supported storage policy rule type.
                        else
                        {
                            throw new IllegalStateException(
                                String.format("Storage policy type \"%s\" is not supported.", storagePolicyEntity.getStoragePolicyRuleType().getCode()));
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
                sqsDao.sendSqsTextMessage(awsParamsDto, sqsQueueName, messageText);
            }
            catch (Exception e)
            {
                LOGGER.error(String.format("Failed to post message on \"%s\" SQS queue. Message: %s", sqsQueueName, messageText));

                // Throw the exception up.
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }
}
