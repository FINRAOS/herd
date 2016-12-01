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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.dao.impl.MockS3OperationsImpl;
import org.finra.herd.dao.impl.MockSqsOperationsImpl;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataRetryStoragePolicyTransitionRequest;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.BusinessObjectDataRetryStoragePolicyTransitionDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyRuleTypeEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * This class tests functionality within the business object data retry storage policy transition helper service.
 */
public class BusinessObjectDataRetryStoragePolicyTransitionHelperServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "businessObjectDataRetryStoragePolicyTransitionHelperServiceImpl")
    private BusinessObjectDataRetryStoragePolicyTransitionHelperService businessObjectDataRetryStoragePolicyTransitionHelperServiceImpl;

    /**
     * This method is to get coverage for the business object data retry storage policy transition helper service methods that have an explicit annotation for
     * transaction propagation.
     */
    @Test
    public void testBusinessObjectDataRetryStoragePolicyTransitionHelperServiceMethodsNewTransactionPropagation()
    {
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperServiceImpl.prepareToRetryStoragePolicyTransition(new BusinessObjectDataKey(),
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(new StoragePolicyKey()));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperServiceImpl.executeAwsSpecificSteps(null);
            fail();
        }
        catch (NullPointerException e)
        {
            assertNull(e.getMessage());
        }

        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperServiceImpl.executeRetryStoragePolicyTransitionAfterStep(null);
            fail();
        }
        catch (NullPointerException e)
        {
            assertNull(e.getMessage());
        }
    }

    @Test
    public void testExecuteAwsSpecificStepsS3StepFails()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create a business object data retry storage policy transition DTO with
        // the Glacier S3 bucket name set to the mocked value that causes an AmazonServiceException.
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto =
            new BusinessObjectDataRetryStoragePolicyTransitionDto(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION, STORAGE_NAME_GLACIER,
                MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_INTERNAL_ERROR, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX,
                STORAGE_POLICY_SELECTOR_SQS_QUEUE_NAME);

        // Try to execute AWS steps when an AWS service exception is expected.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService.executeAwsSpecificSteps(businessObjectDataRetryStoragePolicyTransitionDto);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Failed to list keys/key versions with prefix \"%s/\" from bucket \"%s\". " +
                "Reason: InternalError (Service: null; Status Code: 0; Error Code: null; Request ID: null)", S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX,
                MockS3OperationsImpl.MOCK_S3_BUCKET_NAME_INTERNAL_ERROR), e.getMessage());
        }
    }

    @Test
    public void testExecuteAwsSpecificStepsSqsStepFails()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create a business object data retry storage policy transition DTO with
        // the SQS queue name set to the mocked value that causes an exception.
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto =
            new BusinessObjectDataRetryStoragePolicyTransitionDto(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION, STORAGE_NAME_GLACIER,
                S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME);

        // Try to execute AWS steps when an AWS service exception is expected.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService.executeAwsSpecificSteps(businessObjectDataRetryStoragePolicyTransitionDto);
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("AWS SQS queue with \"%s\" name not found.", MockSqsOperationsImpl.MOCK_SQS_QUEUE_NOT_FOUND_NAME), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransition()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Execute a before step for the retry storage policy transition.
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto =
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataRetryStoragePolicyTransitionDto(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION, STORAGE_NAME_GLACIER,
            S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, STORAGE_POLICY_SELECTOR_SQS_QUEUE_NAME),
            businessObjectDataRetryStoragePolicyTransitionDto);
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionBusinessObjectDataNoExists()
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
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectDataKey, NO_BDATA_STATUS),
                e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionGlacierStorageNoBucketName()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing with Glacier storage not having S3 bucket name configured.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.ENABLED, STORAGE_NAME_GLACIER, NO_S3_BUCKET_NAME, StorageUnitStatusEntity.ENABLED,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Try to execute a before step for the retry storage policy transition
        // when Glacier storage does not have an S3 bucket name configured.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), STORAGE_NAME_GLACIER), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionGlacierStorageUnitDirectoryPathDoesNotStartWithOriginBucketName()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing with the Glacier storage unit directory path not starting with the origin S3 bucket name.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.ENABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ARCHIVING, TEST_S3_KEY_PREFIX);

        // Try to execute a before step for the retry storage policy transition
        // when Glacier storage unit directory path does not start with the origin S3 bucket name.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                "Storage directory path \"%s\" for business object data in \"%s\" %s storage does not start with the origin S3 bucket name. " +
                    "Origin S3 bucket name: {%s}, origin storage: {%s}, business object data: {%s}", TEST_S3_KEY_PREFIX, STORAGE_NAME_GLACIER,
                StoragePlatformEntity.GLACIER, S3_BUCKET_NAME_ORIGIN, STORAGE_NAME_ORIGIN,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionGlacierStorageUnitDirectoryPathHasInvalidOriginS3keyPrefix()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing with the Glacier storage unit directory path having an invalid origin S3 key prefix.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.ENABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ARCHIVING,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX_2);

        // Try to execute a before step for the retry storage policy transition
        // when Glacier storage unit directory path contains an invalid origin S3 key prefix.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Number of storage files (6) registered for the business object data in \"%s\" storage is not equal " +
                "to the number of registered storage files (0) matching \"%s\" S3 key prefix in the same storage. " +
                "Business object data: {%s}", STORAGE_NAME_ORIGIN, TEST_S3_KEY_PREFIX_2 + "/",
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionGlacierStorageUnitNoDirectoryPath()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing with the Glacier storage unit without a directory path.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.ENABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ARCHIVING, BLANK_TEXT);

        // Try to execute a before step for the retry storage policy transition when Glacier storage unit is not in ARCHIVING state.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Business object data has no storage directory path specified in \"%s\" %s storage. Business object data: {%s}", STORAGE_NAME_GLACIER,
                    StoragePlatformEntity.GLACIER, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)),
                e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionGlacierStorageUnitNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing except for the Glacier storage unit.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.ENABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, NO_STORAGE_UNIT_STATUS,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Try to execute a before step for the retry storage policy transition when Glacier storage unit does not exist.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data has no storage unit in \"%s\" storage policy destination storage. Business object data: {%s}",
                STORAGE_NAME_GLACIER, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionGlacierStorageUnitNotArchiving()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing with the Glacier storage unit not in ARCHIVING state.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.ENABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, STORAGE_UNIT_STATUS,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Try to execute a before step for the retry storage policy transition when Glacier storage unit is not in ARCHIVING state.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Business object data is not currently being archived to \"%s\" storage policy destination storage. Business object data: {%s}",
                    STORAGE_NAME_GLACIER, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionInvalidParameters()
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
                new BusinessObjectDataKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid business object definition name.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid format usage.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid format file type.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid business object format version.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST",
                    SUBPARTITION_VALUES, DATA_VERSION), new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    "I_DO_NOT_EXIST", SUBPARTITION_VALUES, DATA_VERSION, NO_BDATA_STATUS), e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid sub-partition value.
        for (int i = 0; i < SUBPARTITION_VALUES.size(); i++)
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.set(i, "I_DO_NOT_EXIST");
            try
            {
                businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(businessObjectDataKey,
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(new StoragePolicyKey("I_DO_NOT_EXIST", STORAGE_POLICY_NAME)));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                storagePolicyServiceTestHelper.getExpectedStoragePolicyNotFoundErrorMessage(new StoragePolicyKey("I_DO_NOT_EXIST", STORAGE_POLICY_NAME)),
                e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition using an invalid storage policy name.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(businessObjectDataKey,
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, "I_DO_NOT_EXIST")));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(storagePolicyServiceTestHelper
                .getExpectedStoragePolicyNotFoundErrorMessage(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, "I_DO_NOT_EXIST")), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionLowerCaseParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Execute a before step for the retry storage policy transition
        // using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto =
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                    FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION),
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataRetryStoragePolicyTransitionDto(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION, STORAGE_NAME_GLACIER,
            S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, STORAGE_POLICY_SELECTOR_SQS_QUEUE_NAME),
            businessObjectDataRetryStoragePolicyTransitionDto);
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionMissingOptionalParameters()
    {
        // Create a business object data key without sub-partition values.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Execute a before step for the retry storage policy transition.
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto =
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataRetryStoragePolicyTransitionDto(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION, STORAGE_NAME_GLACIER,
            S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, STORAGE_POLICY_SELECTOR_SQS_QUEUE_NAME),
            businessObjectDataRetryStoragePolicyTransitionDto);
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionMissingRequiredParameters()
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(businessObjectDataKey, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data retry storage policy transition request must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying a storage policy key.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(null));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy key must be specified.", e.getMessage());
        }

        // Try to execute a before step for the retry storage policy transition without specifying storage policy namespace.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(businessObjectDataKey,
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
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(businessObjectDataKey,
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, BLANK_TEXT)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionNoSqsQueueName() throws Exception
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

            // Create database entities required for testing with the Glacier storage unit directory path having an invalid origin S3 key prefix.
            businessObjectDataServiceTestHelper
                .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME_ORIGIN,
                    S3_BUCKET_NAME_ORIGIN, StorageUnitStatusEntity.ENABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ARCHIVING,
                    S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

            // Try to execute a before step for the retry storage policy transition when SQS queue name is not configured.
            try
            {
                businessObjectDataRetryStoragePolicyTransitionHelperService
                    .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
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
    public void testPrepareToRetryStoragePolicyTransitionOriginStorageNoBucketName()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing with the origin storage not having S3 bucket name configured.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME_ORIGIN, NO_S3_BUCKET_NAME,
                StorageUnitStatusEntity.ENABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ARCHIVING,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Try to execute a before step for the retry storage policy transition
        // when the origin storage does not have an S3 bucket name configured.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), STORAGE_NAME_ORIGIN), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionOriginStorageUnitNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.ENABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ARCHIVING,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Get the Glacier storage unit entity.
        StorageUnitEntity glacierStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_GLACIER, businessObjectDataEntity);

        // Remove a reference to the origin storage unit from the glacier storage unit.
        glacierStorageUnitEntity.setParentStorageUnit(null);

        // Try to execute a before step for the retry storage policy transition when glacier storage unit has no origin origin unit reference.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Glacier storage unit in \"%s\" storage has no origin S3 storage unit. Business object data: {%s}", STORAGE_NAME_GLACIER,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionOriginStorageUnitNoStorageFiles()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.ENABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ARCHIVING,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Get the origin storage unit entity.
        StorageUnitEntity originStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_ORIGIN, businessObjectDataEntity);

        // Remove storage files from the origin storage unit.
        originStorageUnitEntity.getStorageFiles().clear();

        // Try to execute a before step for the retry storage policy transition when origin storage unit has no storage files.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Business object data has no storage files registered in \"%s\" origin storage. Business object data: {%s}", STORAGE_NAME_ORIGIN,
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionOriginStorageUnitNotEnabled()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing with the origin storage unit not in ENABLED state.
        businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                STORAGE_UNIT_STATUS, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ARCHIVING,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Try to execute a before step for the retry storage policy transition when origin storage unit is not disabled.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Origin S3 storage unit in \"%s\" storage must have \"%s\" status, but it actually has \"%s\" status. Business object data: {%s}",
                    STORAGE_NAME_ORIGIN, StorageUnitStatusEntity.ENABLED, STORAGE_UNIT_STATUS,
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionOriginStorageUnitNotS3()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataServiceTestHelper
            .createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey, STORAGE_NAME_ORIGIN, S3_BUCKET_NAME_ORIGIN,
                StorageUnitStatusEntity.ENABLED, STORAGE_NAME_GLACIER, S3_BUCKET_NAME_GLACIER, StorageUnitStatusEntity.ARCHIVING,
                S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX);

        // Create a non-S3 storage unit for this business object data.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, TEST_S3_KEY_PREFIX);

        // Get the Glacier storage unit entity.
        StorageUnitEntity glacierStorageUnitEntity = storageUnitDaoHelper.getStorageUnitEntity(STORAGE_NAME_GLACIER, businessObjectDataEntity);

        // Set the origin storage unit to be a non-S3 storage unit.
        glacierStorageUnitEntity.setParentStorageUnit(storageUnitEntity);

        // Try to execute a before step for the retry storage policy transition when origin storage unit is not S3.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Glacier storage unit in \"%s\" storage has no origin S3 storage unit. Business object data: {%s}", STORAGE_NAME_GLACIER,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionStoragePolicyFilterDoesNotMatchBusinessObjectData()
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
                BDEF_NAMESPACE_2, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME_ORIGIN, STORAGE_NAME_GLACIER,
                StoragePolicyStatusEntity.ENABLED, AbstractServiceTest.INITIAL_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET);
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(1), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, AbstractServiceTest.BDATA_AGE_IN_DAYS,
                BDEF_NAMESPACE, BDEF_NAME_2, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME_ORIGIN, STORAGE_NAME_GLACIER,
                StoragePolicyStatusEntity.ENABLED, AbstractServiceTest.INITIAL_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET);
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(2), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, AbstractServiceTest.BDATA_AGE_IN_DAYS,
                BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, STORAGE_NAME_ORIGIN, STORAGE_NAME_GLACIER,
                StoragePolicyStatusEntity.ENABLED, AbstractServiceTest.INITIAL_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET);
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(3), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, AbstractServiceTest.BDATA_AGE_IN_DAYS,
                BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_ORIGIN, STORAGE_NAME_GLACIER,
                StoragePolicyStatusEntity.ENABLED, AbstractServiceTest.INITIAL_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET);

        // Try to execute a before step for the retry storage policy transition when storage policy filter does not match business object data.
        for (int i = 0; i < 4; i++)
        {
            try
            {
                businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(businessObjectDataKey,
                    new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKeys.get(i)));
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
    public void testPrepareToRetryStoragePolicyTransitionStoragePolicyFilterNoOptionalFields()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create two storage policy keys.
        List<StoragePolicyKey> storagePolicyKeys = Arrays.asList(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME_2));

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKeys.get(0));

        // Create another storage policy with storage policy filter with missing optional fields.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKeys.get(1), StoragePolicyRuleTypeEntity.DAYS_SINCE_BDATA_REGISTERED, AbstractServiceTest.BDATA_AGE_IN_DAYS,
                NO_BDEF_NAMESPACE, NO_BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, STORAGE_NAME_ORIGIN, STORAGE_NAME_GLACIER,
                StoragePolicyStatusEntity.ENABLED, AbstractServiceTest.INITIAL_VERSION, AbstractServiceTest.LATEST_VERSION_FLAG_SET);

        // Execute a before step for the retry storage policy transition with storage policy filter missing all optional fields.
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto =
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(businessObjectDataKey,
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKeys.get(1)));

        // Validate the returned object.
        assertEquals(
            new BusinessObjectDataRetryStoragePolicyTransitionDto(businessObjectDataKey, storagePolicyKeys.get(1), INITIAL_VERSION, STORAGE_NAME_GLACIER,
                S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, STORAGE_POLICY_SELECTOR_SQS_QUEUE_NAME),
            businessObjectDataRetryStoragePolicyTransitionDto);
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionStoragePolicyNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create a business object data entity without a Glacier storage unit.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Try to execute a before step for the retry storage policy transition for a non-existing storage policy.
        try
        {
            businessObjectDataRetryStoragePolicyTransitionHelperService
                .prepareToRetryStoragePolicyTransition(businessObjectDataKey, new BusinessObjectDataRetryStoragePolicyTransitionRequest(storagePolicyKey));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(storagePolicyServiceTestHelper.getExpectedStoragePolicyNotFoundErrorMessage(storagePolicyKey), e.getMessage());
        }
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionTrimParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Execute a before step for the retry storage policy transition using input parameters with leading and trailing empty spaces.
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto =
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
                new BusinessObjectDataKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                    addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION),
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(
                    new StoragePolicyKey(addWhitespace(STORAGE_POLICY_NAMESPACE_CD), addWhitespace(STORAGE_POLICY_NAME))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataRetryStoragePolicyTransitionDto(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION, STORAGE_NAME_GLACIER,
            S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, STORAGE_POLICY_SELECTOR_SQS_QUEUE_NAME),
            businessObjectDataRetryStoragePolicyTransitionDto);
    }

    @Test
    public void testPrepareToRetryStoragePolicyTransitionUpperCaseParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create database entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForRetryStoragePolicyTransitionTesting(businessObjectDataKey, storagePolicyKey);

        // Execute a before step for the retry storage policy transition
        // using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectDataRetryStoragePolicyTransitionDto businessObjectDataRetryStoragePolicyTransitionDto =
            businessObjectDataRetryStoragePolicyTransitionHelperService.prepareToRetryStoragePolicyTransition(
                new BusinessObjectDataKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                    FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION),
                new BusinessObjectDataRetryStoragePolicyTransitionRequest(
                    new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toUpperCase(), STORAGE_POLICY_NAME.toUpperCase())));

        // Validate the returned object.
        assertEquals(new BusinessObjectDataRetryStoragePolicyTransitionDto(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION, STORAGE_NAME_GLACIER,
            S3_BUCKET_NAME_GLACIER, S3_BUCKET_NAME_ORIGIN + "/" + TEST_S3_KEY_PREFIX, STORAGE_POLICY_SELECTOR_SQS_QUEUE_NAME),
            businessObjectDataRetryStoragePolicyTransitionDto);
    }
}
