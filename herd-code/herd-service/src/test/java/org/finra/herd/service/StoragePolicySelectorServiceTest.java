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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.Test;

import org.finra.herd.dao.impl.AbstractHerdDao;
import org.finra.herd.dao.impl.MockSqsOperationsImpl;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * This class tests functionality within the StoragePolicySelectorService.
 */
public class StoragePolicySelectorServiceTest extends AbstractServiceTest
{
    @Test
    public void testExecuteDifferentPriorityLevelsWhenStoragePoliciesAreWithNoTransitionLatestValidFlagDisabled()
    {
        // Create an list of unique storage policy keys.
        List<StoragePolicyKey> storagePolicyKeys = Arrays.asList(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD_2, STORAGE_POLICY_NAME_2),
            new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD_2, STORAGE_POLICY_NAME), new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME_2),
            new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME));

        // Storage a storage policy with a filter that has no fields specified.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(0), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, NO_BDEF_NAMESPACE,
                NO_BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID,
                StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, STORAGE_POLICY_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Get the business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = storageUnitEntity.getBusinessObjectData();

        // Get business object data key.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);

        // Execute the storage policy selection and validate the results. The business object data is not expected to be selected.
        assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());

        // Apply the offset in days to business object data "created on" value, so it would match to the storage policy.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(businessObjectDataEntity, BDATA_AGE_IN_DAYS + 1);

        // Execute the storage policy selection and validate the results. The business object data is expected to be selected.
        assertEquals(Collections.singletonList(new StoragePolicySelection(businessObjectDataKey, storagePolicyKeys.get(0), STORAGE_POLICY_VERSION)),
            storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));

        // Create a storage policy with a filter that has only usage and file type specified
        // and with the age restriction greater than the current business object data entity age.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(1), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS + 2,
                NO_BDEF_NAMESPACE, NO_BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID,
                StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, STORAGE_POLICY_VERSION, LATEST_VERSION_FLAG_SET);

        // Execute the storage policy selection and validate the results. The business object data is not expected to be selected.
        assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());

        // Apply the offset in days to business object data "created on" value, so it would match to the last added storage policy.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(businessObjectDataEntity, 2);

        // Execute the storage policy selection and validate the results. The business object data is expected to be selected.
        assertEquals(Collections.singletonList(new StoragePolicySelection(businessObjectDataKey, storagePolicyKeys.get(1), STORAGE_POLICY_VERSION)),
            storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));

        // Create a storage policy with a filter that has only business object definition
        // specified and with the age restriction greater than the current business object data entity age.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(2), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS + 4, BDEF_NAMESPACE,
                BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID,
                StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, STORAGE_POLICY_VERSION, LATEST_VERSION_FLAG_SET);

        // Execute the storage policy selection and validate the results. The business object data is not expected to be selected.
        assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());

        // Apply the offset in days to business object data "created on" value, so it would match to the last added storage policy.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(businessObjectDataEntity, 2);

        // Execute the storage policy selection and validate the results. The business object data is expected to be selected.
        assertEquals(Collections.singletonList(new StoragePolicySelection(businessObjectDataKey, storagePolicyKeys.get(2), STORAGE_POLICY_VERSION)),
            storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));

        // Create a storage policy with a filter that has business object definition, usage, and file type
        // specified and with the age restriction greater than the current business object data entity age.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(3), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS + 6, BDEF_NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER,
                StoragePolicyStatusEntity.ENABLED, STORAGE_POLICY_VERSION, LATEST_VERSION_FLAG_SET);

        // Execute the storage policy selection and validate the results. The business object data is not expected to be selected.
        assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());

        // Apply the offset in days to business object data "created on" value, so it would match to the last added storage policy.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(businessObjectDataEntity, 2);

        // Execute the storage policy selection and validate the results. The business object data is expected to be selected.
        assertEquals(Collections.singletonList(new StoragePolicySelection(businessObjectDataKey, storagePolicyKeys.get(3), STORAGE_POLICY_VERSION)),
            storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));
    }

    @Test
    public void testExecuteDifferentPriorityLevelsWhenStoragePoliciesAreWithNoTransitionLatestValidFlagEnabled()
    {
        // Create an list of unique storage policy keys.
        List<StoragePolicyKey> storagePolicyKeys = Arrays.asList(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD_2, STORAGE_POLICY_NAME_2),
            new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD_2, STORAGE_POLICY_NAME), new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME_2),
            new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME));

        // Storage a storage policy with a filter that has no fields specified.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(0), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, NO_BDEF_NAMESPACE,
                NO_BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, DO_NOT_TRANSITION_LATEST_VALID,
                StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, STORAGE_POLICY_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist three storage units in the storage policy filter storage.  One of them would be a latest valid version, thus it would not get
        // selected by any of the storage policies with the enabled NoTransitionLatestValid flag.
        List<StorageUnitEntity> storageUnitEntities = new ArrayList<>();
        for (Integer businessObjectDataVersion : Arrays.asList(INITIAL_DATA_VERSION, SECOND_DATA_VERSION, THIRD_DATA_VERSION))
        {
            storageUnitEntities.add(storageUnitDaoTestHelper
                .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, businessObjectDataVersion, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                    StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH));
        }

        // Get the relative business object data keys.
        List<BusinessObjectDataKey> businessObjectDataKeys = new ArrayList<>();
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            businessObjectDataKeys.add(businessObjectDataHelper.getBusinessObjectDataKey(storageUnitEntity.getBusinessObjectData()));
        }

        // Execute the storage policy selection and validate the results. No business object data is not expected to be selected.
        assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());

        // Apply the offset in days to business object data "created on" value, so it would match to the storage policy.
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 1);
        }

        // Execute the storage policy selection and validate the results. The business object data is expected to be selected.
        assertEquals(Arrays.asList(new StoragePolicySelection(businessObjectDataKeys.get(0), storagePolicyKeys.get(0), STORAGE_POLICY_VERSION),
            new StoragePolicySelection(businessObjectDataKeys.get(1), storagePolicyKeys.get(0), STORAGE_POLICY_VERSION)),
            storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));

        // Create a storage policy with a filter that has only usage and file type specified
        // and with the age restriction greater than the current business object data entity age.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(1), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS + 2,
                NO_BDEF_NAMESPACE, NO_BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, DO_NOT_TRANSITION_LATEST_VALID,
                StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, STORAGE_POLICY_VERSION, LATEST_VERSION_FLAG_SET);

        // Execute the storage policy selection and validate the results. No business object data is not expected to be selected.
        assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());

        // Apply the offset in days to business object data "created on" value, so it would match to the last added storage policy.
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity.getBusinessObjectData(), 2);
        }

        // Execute the storage policy selection and validate the results. The business object data is expected to be selected.
        assertEquals(Arrays.asList(new StoragePolicySelection(businessObjectDataKeys.get(0), storagePolicyKeys.get(1), STORAGE_POLICY_VERSION),
            new StoragePolicySelection(businessObjectDataKeys.get(1), storagePolicyKeys.get(1), STORAGE_POLICY_VERSION)),
            storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));

        // Create a storage policy with a filter that has only business object definition
        // specified and with the age restriction greater than the current business object data entity age.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(2), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS + 4, BDEF_NAMESPACE,
                BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, DO_NOT_TRANSITION_LATEST_VALID,
                StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, STORAGE_POLICY_VERSION, LATEST_VERSION_FLAG_SET);

        // Execute the storage policy selection and validate the results. No business object data is not expected to be selected.
        assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());

        // Apply the offset in days to business object data "created on" value, so it would match to the last added storage policy.
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity.getBusinessObjectData(), 2);
        }

        // Execute the storage policy selection and validate the results. The business object data is expected to be selected.
        assertEquals(Arrays.asList(new StoragePolicySelection(businessObjectDataKeys.get(0), storagePolicyKeys.get(2), STORAGE_POLICY_VERSION),
            new StoragePolicySelection(businessObjectDataKeys.get(1), storagePolicyKeys.get(2), STORAGE_POLICY_VERSION)),
            storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));

        // Create a storage policy with a filter that has business object definition, usage, and file type
        // specified and with the age restriction greater than the current business object data entity age.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(3), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS + 6, BDEF_NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER,
                StoragePolicyStatusEntity.ENABLED, STORAGE_POLICY_VERSION, LATEST_VERSION_FLAG_SET);

        // Execute the storage policy selection and validate the results. No business object data is not expected to be selected.
        assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());

        // Apply the offset in days to business object data "created on" value, so it would match to the last added storage policy.
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity.getBusinessObjectData(), 2);
        }

        // Execute the storage policy selection and validate the results. The business object data is expected to be selected.
        assertEquals(Arrays.asList(new StoragePolicySelection(businessObjectDataKeys.get(0), storagePolicyKeys.get(3), STORAGE_POLICY_VERSION),
            new StoragePolicySelection(businessObjectDataKeys.get(1), storagePolicyKeys.get(3), STORAGE_POLICY_VERSION)),
            storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));
    }

    @Test
    public void testExecuteInvalidSqsQueueName()
    {
        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
            LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Apply the offset in days to business object data "created on" value.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 1);

        // Try to execute the storage policy selection by passing an invalid SQS queue name.
        try
        {
            storagePolicySelectorService.execute(MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME, MAX_RESULT);
            fail("Should throw an IllegalStateException when invalid SQS queue name is specified.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("AWS SQS queue with \"%s\" name not found.", MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME), e.getMessage());
        }
    }

    @Test
    public void testExecuteInvalidStoragePolicyRuleType()
    {
        // Create and persist a storage policy entity with a non-supported storage policy type.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME), STORAGE_POLICY_RULE_TYPE, BDATA_AGE_IN_DAYS,
                BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID,
                StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Apply the offset in days to business object data "created on" value.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 1);

        // Try to retrieve the business object data as matching to the storage policy.
        try
        {
            storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT);
            fail("Should throw an IllegalStateException when a storage policy has an invalid storage policy rule type.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Storage policy type \"%s\" is not supported.", STORAGE_POLICY_RULE_TYPE), e.getMessage());
        }
    }

    @Test
    public void testExecuteMixingStoragePoliciesWithNoTransitionLatestValidFlagEnabledAndDisabled()
    {
        // Create an list of unique storage policy keys.
        List<StoragePolicyKey> storagePolicyKeys = Arrays.asList(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD_2, STORAGE_POLICY_NAME_2),
            new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD_2, STORAGE_POLICY_NAME), new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME_2),
            new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME));

        // Storage two identical storage policies with a filter that has no fields specified with and without set NoTransitionLatestValid flag.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(0), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, NO_BDEF_NAMESPACE,
                NO_BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, DO_NOT_TRANSITION_LATEST_VALID,
                StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, STORAGE_POLICY_VERSION, LATEST_VERSION_FLAG_SET);
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(1), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, NO_BDEF_NAMESPACE,
                NO_BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID,
                StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, STORAGE_POLICY_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist three storage units in the storage policy filter storage.  One of them would be a latest valid version, thus it would not get
        // selected by any of the storage policies with the enabled NoTransitionLatestValid flag.
        List<StorageUnitEntity> storageUnitEntities = new ArrayList<>();
        for (Integer businessObjectDataVersion : Arrays.asList(INITIAL_DATA_VERSION, SECOND_DATA_VERSION, THIRD_DATA_VERSION))
        {
            storageUnitEntities.add(storageUnitDaoTestHelper
                .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, businessObjectDataVersion, NO_LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                    StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH));
        }

        // Get the relative business object data keys.
        List<BusinessObjectDataKey> businessObjectDataKeys = new ArrayList<>();
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            businessObjectDataKeys.add(businessObjectDataHelper.getBusinessObjectDataKey(storageUnitEntity.getBusinessObjectData()));
        }

        // Execute the storage policy selection and validate the results. No business object data is not expected to be selected.
        assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());

        // Apply the offset in days to business object data "created on" value, so it would match to the storage policy.
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 1);
        }

        // Execute the storage policy selection and validate the results. All business object data
        // versions are expected to be selected by the relative storage policies.
        assertEquals(Arrays.asList(new StoragePolicySelection(businessObjectDataKeys.get(0), storagePolicyKeys.get(0), STORAGE_POLICY_VERSION),
            new StoragePolicySelection(businessObjectDataKeys.get(1), storagePolicyKeys.get(0), STORAGE_POLICY_VERSION),
            new StoragePolicySelection(businessObjectDataKeys.get(2), storagePolicyKeys.get(1), STORAGE_POLICY_VERSION)),
            storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));

        // Create two more storage policies with a filter that has business object definition, usage, and file type specified and with the age restriction
        // greater than the current business object data entity age - one with and another without set NoTransitionLatestValid flag.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(2), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS + 2, BDEF_NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER,
                StoragePolicyStatusEntity.ENABLED, STORAGE_POLICY_VERSION, LATEST_VERSION_FLAG_SET);
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(3), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS + 2, BDEF_NAMESPACE,
                BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER,
                StoragePolicyStatusEntity.ENABLED, STORAGE_POLICY_VERSION, LATEST_VERSION_FLAG_SET);

        // Execute the storage policy selection and validate the results. No business object data is not expected to be selected.
        assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());

        // Apply the offset in days to business object data "created on" value, so it would match to the last added storage policy.
        for (StorageUnitEntity storageUnitEntity : storageUnitEntities)
        {
            businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity.getBusinessObjectData(), 2);
        }

        // Execute the storage policy selection and validate the results. All business object data
        // versions are expected to be selected by the relative storage policies.
        assertEquals(Arrays.asList(new StoragePolicySelection(businessObjectDataKeys.get(0), storagePolicyKeys.get(2), STORAGE_POLICY_VERSION),
            new StoragePolicySelection(businessObjectDataKeys.get(1), storagePolicyKeys.get(2), STORAGE_POLICY_VERSION),
            new StoragePolicySelection(businessObjectDataKeys.get(2), storagePolicyKeys.get(3), STORAGE_POLICY_VERSION)),
            storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));
    }

    @Test
    public void testExecutePrimaryPartitionValueStoragePolicyRuleType() throws Exception
    {
        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(storagePolicyKey, StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE,
            BDATA_PARTITION_VALUE_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME,
            NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
            LATEST_VERSION_FLAG_SET);

        // Create a partition value that would satisfy the primary partition value age check as per the storage policy rule.
        String primaryPartitionValue = getTestPrimaryPartitionValue(BDATA_PARTITION_VALUE_AGE_IN_DAYS + 1);

        // Create and persist a storage unit in the storage policy filter storage.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Overwrite the "updated on" threshold for a newly created business object data to be selectable by the storage policy.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_UPDATED_ON_THRESHOLD_DAYS.getKey(), -1);
        modifyPropertySourceInEnvironment(overrideMap);
        try
        {
            // Execute the storage policy selection.
            List<StoragePolicySelection> resultStoragePolicySelections = storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT);

            // Validate the results.
            assertEquals(Collections.singletonList(new StoragePolicySelection(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
                    SUBPARTITION_VALUES, DATA_VERSION), storagePolicyKey, INITIAL_VERSION)), resultStoragePolicySelections);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testExecutePrimaryPartitionValueStoragePolicyRuleTypeBusinessObjectDataUpdatedTooRecently() throws Exception
    {
        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(storagePolicyKey, StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE,
            BDATA_PARTITION_VALUE_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME,
            NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
            LATEST_VERSION_FLAG_SET);

        // Create a partition value that would satisfy the primary partition value age check as per the storage policy rule.
        String primaryPartitionValue = getTestPrimaryPartitionValue(BDATA_PARTITION_VALUE_AGE_IN_DAYS + 1);

        // Create and persist a storage unit in the storage policy filter storage.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Execute the storage policy selection and validate the results. Since the default value for "updated on"
        // threshold is greater than 1 day, no newly created business object data instances can match the storage policy.
        assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());

        // Overwrite the "updated on" threshold for a newly created business object data to be selectable by the storage policy.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_UPDATED_ON_THRESHOLD_DAYS.getKey(), -1);
        modifyPropertySourceInEnvironment(overrideMap);
        try
        {
            // Execute the storage policy selection and validate the results. One business object data matching to storage policy should get selected.
            assertEquals(Collections.singletonList(new StoragePolicySelection(
                    new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
                        SUBPARTITION_VALUES, DATA_VERSION), storagePolicyKey, INITIAL_VERSION)),
                storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testExecutePrimaryPartitionValueStoragePolicyRuleTypePrimaryPartitionValueNotDate() throws Exception
    {
        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(storagePolicyKey, StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE,
            BDATA_PARTITION_VALUE_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME,
            NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
            LATEST_VERSION_FLAG_SET);

        // Create a partition value that would satisfy the primary partition value age check as per the storage policy rule.
        String primaryPartitionValue = getTestPrimaryPartitionValue(BDATA_PARTITION_VALUE_AGE_IN_DAYS + 1);

        // Create and persist a storage unit with ENABLED status in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Overwrite the "updated on" threshold for a newly created business object data to be selectable by the storage policy.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_UPDATED_ON_THRESHOLD_DAYS.getKey(), -1);
        modifyPropertySourceInEnvironment(overrideMap);
        try
        {
            // Execute the storage policy selection and validate the results. One business object data matching to storage policy should get selected.
            assertEquals(Collections.singletonList(new StoragePolicySelection(
                    new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
                        SUBPARTITION_VALUES, DATA_VERSION), storagePolicyKey, INITIAL_VERSION)),
                storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));

            // Update this business object data, so it's primary partition value has the length of the date pattern,
            // but cannot be converted to a date - we just use the single day date mask for such value.
            storageUnitEntity.getBusinessObjectData().setPartitionValue(AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK);
            businessObjectDataDao.saveAndRefresh(storageUnitEntity.getBusinessObjectData());

            // Execute the storage policy selection and validate the results. No business object data matching to storage policy should get selected.
            assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testExecutePrimaryPartitionValueStoragePolicyRuleTypePrimaryPartitionValueNotOldEnough() throws Exception
    {
        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(storagePolicyKey, StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE,
            BDATA_PARTITION_VALUE_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME,
            NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
            LATEST_VERSION_FLAG_SET);

        // Create a partition value that would satisfy the primary partition value age check as per the storage policy rule.
        String primaryPartitionValue = getTestPrimaryPartitionValue(BDATA_PARTITION_VALUE_AGE_IN_DAYS + 1);

        // Create and persist a storage unit with ENABLED status in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Overwrite the "updated on" threshold for a newly created business object data to be selectable by the storage policy.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_UPDATED_ON_THRESHOLD_DAYS.getKey(), -1);
        modifyPropertySourceInEnvironment(overrideMap);
        try
        {
            // Execute the storage policy selection and validate the results. One business object data matching to storage policy should get selected.
            assertEquals(Collections.singletonList(new StoragePolicySelection(
                    new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
                        SUBPARTITION_VALUES, DATA_VERSION), storagePolicyKey, INITIAL_VERSION)),
                storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));

            // Update this business object data, so it's primary partition value is now after the storage policy threshold date.
            storageUnitEntity.getBusinessObjectData().setPartitionValue(getTestPrimaryPartitionValue(BDATA_PARTITION_VALUE_AGE_IN_DAYS - 1));
            businessObjectDataDao.saveAndRefresh(storageUnitEntity.getBusinessObjectData());

            // Execute the storage policy selection and validate the results. No business object data matching to storage policy should get selected.
            assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testExecutePrimaryPartitionValueStoragePolicyRuleTypePrimaryPartitionValueSingleDayDateMaskSizeMismatch() throws Exception
    {
        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(storagePolicyKey, StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE,
            BDATA_PARTITION_VALUE_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME,
            NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
            LATEST_VERSION_FLAG_SET);

        // Create a partition value that would satisfy the primary partition value age check as per the storage policy rule.
        String primaryPartitionValue = getTestPrimaryPartitionValue(BDATA_PARTITION_VALUE_AGE_IN_DAYS + 1);

        // Create and persist a storage unit with ENABLED status in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Overwrite the "updated on" threshold for a newly created business object data to be selectable by the storage policy.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_UPDATED_ON_THRESHOLD_DAYS.getKey(), -1);
        modifyPropertySourceInEnvironment(overrideMap);
        try
        {
            // Execute the storage policy selection and validate the results. One business object data matching to storage policy should get selected.
            assertEquals(Collections.singletonList(new StoragePolicySelection(
                    new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, primaryPartitionValue,
                        SUBPARTITION_VALUES, DATA_VERSION), storagePolicyKey, INITIAL_VERSION)),
                storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));

            // Update this business object data, so it's primary partition value is now longer when the expected date pattern.
            storageUnitEntity.getBusinessObjectData().setPartitionValue(primaryPartitionValue + "_");
            businessObjectDataDao.saveAndRefresh(storageUnitEntity.getBusinessObjectData());

            // Execute the storage policy selection and validate the results. No business object data matching to storage policy should get selected.
            assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testExecuteRegistrationDateStoragePolicyRuleType()
    {
        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER,
                StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit with ENABLED status in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Apply the offset in days to business object data "created on" value.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 1);

        // Execute the storage policy selection.
        List<StoragePolicySelection> resultStoragePolicySelections = storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT);

        // Validate the results.
        assertEquals(Collections.singletonList(new StoragePolicySelection(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), storagePolicyKey, INITIAL_VERSION)), resultStoragePolicySelections);
    }

    @Test
    public void testExecuteRegistrationDateStoragePolicyRuleTypeBusinessObjectDataNotOldEnough()
    {
        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper.createStoragePolicyEntity(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
            LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit with ENABLED status in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Apply the offset in days to business object data "created on" value.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 1);

        // Execute the storage policy selection and validate the results. One business object data matching to storage policy should get selected.
        assertEquals(Collections.singletonList(new StoragePolicySelection(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), storagePolicyKey, INITIAL_VERSION)), storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT));

        // Apply another offset in days to business object data "created on" value to make it one day not old enough for the storage policy.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity.getBusinessObjectData(), -2);

        // Execute the storage policy selection and validate the results. No business object data matching to storage policy should get selected.
        assertEquals(0, storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, MAX_RESULT).size());
    }

    @Test
    public void testExecuteTestingMaxResult()
    {
        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, BDATA_AGE_IN_DAYS, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, NO_DO_NOT_TRANSITION_LATEST_VALID, StoragePolicyTransitionTypeEntity.GLACIER,
                StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Create and persist a storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity1 = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Apply the offset in days to business object data "created on" value.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity1.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 1);

        // Create and persist a second storage unit in the storage policy filter storage.
        StorageUnitEntity storageUnitEntity2 = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, StorageUnitStatusEntity.ENABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Also apply an offset to business object data "created on" value, but make this business object data older than the first.
        businessObjectDataDaoTestHelper.ageBusinessObjectData(storageUnitEntity2.getBusinessObjectData(), BDATA_AGE_IN_DAYS + 2);

        // Try to retrieve both business object data instances as matching to the storage policy, but with max result limit set to 1.
        List<StoragePolicySelection> resultStoragePolicySelections = storagePolicySelectorService.execute(AWS_SQS_QUEUE_NAME, 1);

        // Validate the results. Only the oldest business object data should get selected.
        assertEquals(Collections.singletonList(new StoragePolicySelection(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION), storagePolicyKey, INITIAL_VERSION)), resultStoragePolicySelections);
    }

    /**
     * Gets a primary partition value in "yyyy-MM-dd" format from the current date minus the specified number of days.
     *
     * @param offsetInDays the number of days to subtract from the current date to produce the primary partition value
     *
     * @return the primary partition value as a date in "yyyy-MM-dd" format
     */
    private String getTestPrimaryPartitionValue(long offsetInDays)
    {
        // Get the current timestamp from the database.
        Timestamp currentTimestamp = herdDao.getCurrentTimestamp();

        // Apply the offset in days to current timestamp.
        Timestamp updatedTimestamp = new Timestamp(currentTimestamp.getTime() - offsetInDays * 86400000L);    // 24L * 60L * 60L * 1000L

        // Return the primary partition value as a date in "yyyy-MM-dd" format.
        return new SimpleDateFormat(AbstractHerdDao.DEFAULT_SINGLE_DAY_DATE_MASK, Locale.US).format(updatedTimestamp);
    }
}
