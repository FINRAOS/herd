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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.finra.herd.dao.impl.MockSqsOperationsImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataRetryStoragePolicyTransitionRequest;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the business object data retry storage policy transition helper service.
 */
public class BusinessObjectDataRetryStoragePolicyTransitionHelperTest extends AbstractServiceTest
{
    @Test
    public void testRetryStoragePolicyTransition()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Retry a storage policy transition.
        BusinessObjectData businessObjectData = businessObjectDataRetryStoragePolicyTransitionHelper
            .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS, businessObjectData);
    }

    @Test
    public void testRetryStoragePolicyTransitionBusinessObjectDataNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to execute a before step for the retry storage policy transition for a non-existing business object data.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper
                .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectDataKey, NO_BDATA_STATUS),
                e.getMessage());
        }
    }

    @Test
    public void testRetryStoragePolicyTransitionInvalidParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Validate existence of business object data and storage policy.
        assertNotNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
        assertNotNull(storagePolicyDao.getStoragePolicyByAltKey(storagePolicyKey));

        // Try to execute a before step for the retry storage policy transition using an invalid namespace.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(I_DO_NOT_EXIST, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(I_DO_NOT_EXIST, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid business object definition name.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, I_DO_NOT_EXIST, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, I_DO_NOT_EXIST, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid format usage.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, I_DO_NOT_EXIST, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, I_DO_NOT_EXIST, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid format file type.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid business object format version.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid primary partition value.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, I_DO_NOT_EXIST,
                    SUBPARTITION_VALUES, DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    I_DO_NOT_EXIST, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid sub-partition value.
        for (int i = 0; i < SUBPARTITION_VALUES.size(); i++)
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.set(i, I_DO_NOT_EXIST);
            try
            {
                businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                    new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        testSubPartitionValues, DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
                fail();
            }
            catch (ObjectNotFoundException e)
            {
                assertEquals(businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                        PARTITION_VALUE, testSubPartitionValues, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
            }
        }

        // Try to execute a before step for the retry storage policy transition using an invalid business object data version.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, INVALID_DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, INVALID_DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid storage policy namespace.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(businessObjectDataKey,
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(new StoragePolicyKey(I_DO_NOT_EXIST, STORAGE_POLICY_NAME)));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(storagePolicyServiceTestHelper.getExpectedStoragePolicyNotFoundErrorMessage(new StoragePolicyKey(I_DO_NOT_EXIST, STORAGE_POLICY_NAME)),
                e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid storage policy name.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(businessObjectDataKey,
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, I_DO_NOT_EXIST)));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                storagePolicyServiceTestHelper.getExpectedStoragePolicyNotFoundErrorMessage(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, I_DO_NOT_EXIST)),
                e.getMessage());
        }
    }

    @Test
    public void testRetryStoragePolicyTransitionLowerCaseParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Retry a storage policy transition using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectData businessObjectData = businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
            new BusinessObjectDataKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION),
            new BusinessObjectDataRetryStoragePolicyTransitionRequest(
                new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toLowerCase(), STORAGE_POLICY_NAME.toLowerCase())));

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS, businessObjectData);
    }

    @Test
    public void testRetryStoragePolicyTransitionMissingOptionalParameters()
    {
        // Create a business object data key without sub-partition values.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Retry a storage policy transition.
        BusinessObjectData businessObjectData = businessObjectDataRetryStoragePolicyTransitionHelper
            .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS, businessObjectData);
    }

    @Test
    public void testRetryStoragePolicyTransitionMissingRequiredParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to execute a before step for the retry storage policy transition without specifying business object definition name.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying business object format usage.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying business object format file type.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying business object format version.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying primary partition value.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT, SUBPARTITION_VALUES,
                    DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying 1st subpartition value.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT, SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION),
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying 2nd subpartition value.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), BLANK_TEXT, SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION),
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying 3rd subpartition value.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), BLANK_TEXT, SUBPARTITION_VALUES.get(3)), DATA_VERSION),
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying 4th subpartition value.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), BLANK_TEXT), DATA_VERSION),
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying business object data version.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, NO_DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition
        // without specifying a business object data retry storage policy transition request.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(businessObjectDataKey, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data retry storage policy transition request must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying a storage policy key.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper
                .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(null));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy key must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying storage policy namespace.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(businessObjectDataKey,
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(new StoragePolicyKey(BLANK_TEXT, STORAGE_POLICY_NAME)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying storage policy name.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(businessObjectDataKey,
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, BLANK_TEXT)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testRetryStoragePolicyTransitionNoSqsQueueName() throws Exception
    {
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME.getKey(), BLANK_TEXT);
        modifyPropertySourceInEnvironment(overrideMap);
        try
        {
            // Create a business object data key.
            BusinessObjectDataKey businessObjectDataKey =
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION);

            // Create a storage policy key.
            StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

            // Create database entities required for testing.
            businessObjectDataServiceTestHelper
                .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME, S3_BUCKET_NAME,
                    StorageUnitStatusEntity.ARCHIVING);

            // Try to execute a before step for the retry storage policy transition when SQS queue name is not configured.
            try
            {
                businessObjectDataRetryStoragePolicyTransitionHelper
                    .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
                fail();
            }
            catch (IllegalStateException e)
            {
                assertEquals(String.format("SQS queue name not found. Ensure the \"%s\" configuration entry is configured.",
                    ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME.getKey()), e.getMessage());
            }
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testRetryStoragePolicyTransitionStorageHasNoBucketName()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing with a storage not having S3 bucket name configured.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME, NO_S3_BUCKET_NAME,
                StorageUnitStatusEntity.ARCHIVING);

        // Try to execute a before step for the retry storage policy transition when the storage does not have an S3 bucket name configured.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper
                .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testRetryStoragePolicyTransitionStoragePolicyFilterDoesNotMatchBusinessObjectData()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create four more storage policy keys.
        List<StoragePolicyKey> storagePolicyKeys = new ArrayList<>();
        for (int i = 0; i < 4; i++)
        {
            storagePolicyKeys.add(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME + "_" + i));
        }

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Create four more storage policies having on of the filter fields not matching the business object data.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(0), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, AbstractServiceTest.BDATA_AGE_IN_DAYS,
                BDEF_NAMESPACE_2, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME, StoragePolicyStatusEntity.ENABLED,
                AbstractServiceTest.INITIAL_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET);
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(1), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, AbstractServiceTest.BDATA_AGE_IN_DAYS,
                BDEF_NAMESPACE, BDEF_NAME_2, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME, StoragePolicyStatusEntity.ENABLED,
                AbstractServiceTest.INITIAL_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET);
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(2), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, AbstractServiceTest.BDATA_AGE_IN_DAYS,
                BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME, StoragePolicyStatusEntity.ENABLED,
                AbstractServiceTest.INITIAL_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET);
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(3), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, AbstractServiceTest.BDATA_AGE_IN_DAYS,
                BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME, STORAGE_NAME, StoragePolicyStatusEntity.ENABLED,
                AbstractServiceTest.INITIAL_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET);

        // Try to execute a before step for the retry storage policy transition when storage policy filter does not match business object data.
        for (int i = 0; i < 4; i++)
        {
            try
            {
                businessObjectDataRetryStoragePolicyTransitionHelper
                    .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKeys.get(i)));
                fail();
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("Business object data does not match storage policy filter. Storage policy: {%s}, business object data: {%s}",
                    storagePolicyServiceTestHelper.getExpectedStoragePolicyKeyAndVersionAsString(storagePolicyKeys.get(i), INITIAL_VERSION),
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
            }
        }
    }

    @Test
    public void testRetryStoragePolicyTransitionStoragePolicyFilterNoOptionalFields()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create two storage policy keys.
        List<StoragePolicyKey> storagePolicyKeys = Arrays.asList(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME_2));

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKeys.get(0));

        // Create another storage policy with storage policy filter with missing optional fields.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(1), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, AbstractServiceTest.BDATA_AGE_IN_DAYS,
                NO_BDEF_NAMESPACE, NO_BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME, StoragePolicyStatusEntity.ENABLED,
                AbstractServiceTest.INITIAL_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET);

        // Retry a storage policy transition with storage policy filter missing all optional fields.
        BusinessObjectData businessObjectData = businessObjectDataRetryStoragePolicyTransitionHelper
            .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKeys.get(1)));

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS, businessObjectData);
    }

    @Test
    public void testRetryStoragePolicyTransitionStoragePolicyNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create a business object data entity.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Try to execute a before step for the retry storage policy transition for a non-existing storage policy.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper
                .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(storagePolicyServiceTestHelper.getExpectedStoragePolicyNotFoundErrorMessage(storagePolicyKey), e.getMessage());
        }
    }

    @Test
    public void testRetryStoragePolicyTransitionStorageUnitHasNoStorageFiles()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME, S3_BUCKET_NAME,
                StorageUnitStatusEntity.ARCHIVING);

        // Get the storage unit entity.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME, businessObjectDataEntity);

        // Remove storage files from the storage unit.
        storageUnitEntity.getStorageFiles().clear();

        // Try to execute a before step for the retry storage policy transition when storage unit has no storage files.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper
                .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data has no storage files registered in \"%s\" storage. Business object data: {%s}", STORAGE_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testRetryStoragePolicyTransitionStorageUnitNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME, S3_BUCKET_NAME,
                NO_STORAGE_UNIT_STATUS);

        // Try to execute a before step for the retry storage policy transition when source storage has no storage unit.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper
                .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data has no storage unit in \"%s\" storage. Business object data: {%s}", STORAGE_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testRetryStoragePolicyTransitionStorageUnitNotInArchivingState()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing with a storage unit not in ARCHIVING state.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME, S3_BUCKET_NAME,
                STORAGE_UNIT_STATUS);

        // Try to execute a before step for the retry storage policy transition when storage unit is not in ARCHIVING state.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper
                .retryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data is not currently being archived. " +
                "Storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. Business object data: {%s}", STORAGE_NAME,
                StorageUnitStatusEntity.ARCHIVING, STORAGE_UNIT_STATUS,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testRetryStoragePolicyTransitionTrimParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Retry a storage policy transition using input parameters with leading and trailing empty spaces.
        BusinessObjectData businessObjectData = businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
            new BusinessObjectDataKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION),
            new BusinessObjectDataRetryStoragePolicyTransitionRequest(
                new StoragePolicyKey(addWhitespace(STORAGE_POLICY_NAMESPACE_CD), addWhitespace(STORAGE_POLICY_NAME))));

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS, businessObjectData);
    }

    @Test
    public void testRetryStoragePolicyTransitionUpperCaseParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Retry a storage policy transition using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectData businessObjectData = businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(
            new BusinessObjectDataKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION),
            new BusinessObjectDataRetryStoragePolicyTransitionRequest(
                new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toUpperCase(), STORAGE_POLICY_NAME.toUpperCase())));

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS, businessObjectData);
    }

    @Test
    public void testSendStoragePolicySelectionSqsMessageSqsOperationFails()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to execute AWS steps when AWS service exception is expected.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelper.sendStoragePolicySelectionSqsMessage(MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME,
                new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("AWS SQS queue with \"%s\" name not found.", MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME), e.getMessage());
        }
    }
}
