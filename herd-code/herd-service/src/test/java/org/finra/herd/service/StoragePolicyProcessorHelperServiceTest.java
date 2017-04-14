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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.PutObjectRequest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.dto.StoragePolicyTransitionParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;
import org.finra.herd.model.jpa.StoragePolicyTransitionTypeEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.impl.StoragePolicyProcessorHelperServiceImpl;

public class StoragePolicyProcessorHelperServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "storagePolicyProcessorHelperServiceImpl")
    private StoragePolicyProcessorHelperService storagePolicyProcessorHelperServiceImpl;

    @Test
    public void testInitiateStoragePolicyTransition() throws Exception
    {
        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Get the expected S3 key prefix for the business object data key.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(BDEF_NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // Add a storage file to the storage unit.
        storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, expectedS3KeyPrefix + "/" + LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Override configuration to specify some settings required for testing.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN.getKey(), S3_OBJECT_TAGGER_ROLE_ARN);
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME.getKey(), S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        modifyPropertySourceInEnvironment(overrideMap);

        // Initiate a storage policy transition.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStorageUnitAlreadyInArchivingState() throws Exception
    {
        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit with ARCHIVING state.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ARCHIVING, NO_STORAGE_DIRECTORY_PATH);

        // Get the expected S3 key prefix for the business object data key.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(BDEF_NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // Add a storage file to the storage unit.
        storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, expectedS3KeyPrefix + "/" + LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Override configuration to specify some settings required for testing.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN.getKey(), S3_OBJECT_TAGGER_ROLE_ARN);
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME.getKey(), S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        modifyPropertySourceInEnvironment(overrideMap);

        // Initiate a storage policy transition.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionInvalidParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to initiate a storage policy transition when storage policy selection message is not specified.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(null);
            fail("Should throw an IllegalArgumentException when when storage policy selection message is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy selection must be specified.", e.getMessage());
        }

        // Try to initiate a storage policy transition when business object data key is not specified.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(null, storagePolicyKey, INITIAL_VERSION));
            fail("Should throw an IllegalArgumentException when when business object data key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data key must be specified.", e.getMessage());
        }

        // Try to initiate a storage policy transition when business object definition namespace is not specified.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(
                new BusinessObjectDataKey(NO_BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), storagePolicyKey, INITIAL_VERSION));
            fail("Should throw an IllegalArgumentException when when business object definition namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to initiate a storage policy transition when storage policy key is not specified.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, null, INITIAL_VERSION));
            fail("Should throw an IllegalArgumentException when when storage policy key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy key must be specified.", e.getMessage());
        }

        // Try to initiate a storage policy transition when storage policy namespace is not specified.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(
                new StoragePolicySelection(businessObjectDataKey, new StoragePolicyKey(null, STORAGE_POLICY_NAME), INITIAL_VERSION));
            fail("Should throw an IllegalArgumentException when when storage policy namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to initiate a storage policy transition when storage policy name is not specified.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(
                new StoragePolicySelection(businessObjectDataKey, new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, null), INITIAL_VERSION));
            fail("Should throw an IllegalArgumentException when when storage policy name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy name must be specified.", e.getMessage());
        }

        // Try to initiate a storage policy transition when storage policy version is not specified.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(
                new StoragePolicySelection(businessObjectDataKey, new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME), null));
            fail("Should throw an IllegalArgumentException when when storage policy version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy version must be specified.", e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionBusinessObjectDataNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to initiate a storage policy transition when business object data does not exist.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectDataKey, null), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionBusinessObjectDataStatusNotSupported()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity with a status that is not supported by the storage policy feature.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to initiate a storage policy transition when business object data status is not supported by the storage policy feature.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail("Should throw an IllegalArgumentException when business object data status is not supported by the storage policy feature.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Business object data status \"%s\" is not supported by the storage policy feature. Business object data: {%s}", BDATA_STATUS,
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStoragePolicyNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to initiate a storage policy transition when storage policy does not exist.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail("Should throw an ObjectNotFoundException when storage policy does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String
                .format("Storage policy with name \"%s\" and version \"%d\" does not exist for \"%s\" namespace.", storagePolicyKey.getStoragePolicyName(),
                    INITIAL_VERSION, storagePolicyKey.getNamespace()), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStoragePolicyFilterStorageInvalidStoragePlatform()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage entity with storage platform type not set to S3.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE);

        // Create and persist a storage policy entity with storage policy filter storage having a non-S3 storage platform type.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Try to initiate a storage policy transition when storage policy filter storage has a non-S3 storage platform type.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail("Should throw an IllegalArgumentException when using non-S3 storage platform for storage policy filter storage.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage platform for storage policy filter storage with name \"%s\" is not \"%s\". Storage policy: {%s}", STORAGE_NAME,
                StoragePlatformEntity.S3, storagePolicyServiceTestHelper.getExpectedStoragePolicyKeyAndVersionAsString(storagePolicyKey, INITIAL_VERSION)),
                e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStoragePolicyFilterStoragePathPrefixValidationNotEnabled()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist an S3 storage without an S3 path prefix validation option configured.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString()));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.FALSE.toString()));
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, attributes);

        // Create and persist a storage policy entity with the storage policy filter storage having no S3 path prefix validation enabled.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Try to initiate a storage policy transition when storage policy filter storage has no S3 path prefix validation enabled.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail("Should throw an IllegalStateException when storage policy filter storage has no S3 path prefix validation enabled.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Path prefix validation must be enabled on \"%s\" storage. Storage policy: {%s}", STORAGE_NAME,
                storagePolicyServiceTestHelper.getExpectedStoragePolicyKeyAndVersionAsString(storagePolicyKey, INITIAL_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStoragePolicyFilterStorageFileExistenceValidationNotEnabled()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create an S3 storage without the S3 file existence validation enabled.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.FALSE.toString()));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()));
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, attributes);

        // Create and persist a storage policy entity with storage policy filter storage has no S3 bucket name attribute configured.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Try to initiate a storage policy transition when storage policy filter storage has no S3 file existence validation enabled.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail("Should throw an IllegalStateException when storage policy filter storage has no S3 file existence validation enabled.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("File existence validation must be enabled on \"%s\" storage. Storage policy: {%s}", STORAGE_NAME,
                storagePolicyServiceTestHelper.getExpectedStoragePolicyKeyAndVersionAsString(storagePolicyKey, INITIAL_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStoragePolicyFilterStorageBucketNameNotConfigured()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist an S3 storage entity without S3 bucket name configured.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString()));
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, attributes);

        // Create and persist a storage policy entity with storage policy filter storage has no S3 bucket name attribute configured.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Try to initiate a storage policy transition when storage policy filter storage has no S3 bucket name attribute configured.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail("Should throw an IllegalStateException when storage policy filter storage has no S3 bucket name attribute configured.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStorageUnitNoExists() throws Exception
    {
        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity without any storage units.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Override configuration to specify some settings required for testing.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN.getKey(), S3_OBJECT_TAGGER_ROLE_ARN);
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME.getKey(), S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        modifyPropertySourceInEnvironment(overrideMap);

        // Try to initiate a storage policy transition when storage unit does not exist.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", STORAGE_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStorageUnitInInvalidState() throws Exception
    {
        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit with an invalid storage unit status.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, STORAGE_UNIT_STATUS,
                NO_STORAGE_DIRECTORY_PATH);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Override configuration to specify some settings required for testing.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN.getKey(), S3_OBJECT_TAGGER_ROLE_ARN);
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME.getKey(), S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        modifyPropertySourceInEnvironment(overrideMap);

        // Try to initiate a storage policy transition when storage unit has an invalid status.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage unit status is \"%s\", but must be \"%s\" or \"%s\" for storage policy transition to proceed. " +
                "Storage: {%s}, business object data: {%s}", STORAGE_UNIT_STATUS, StorageUnitStatusEntity.ENABLED, StorageUnitStatusEntity.ARCHIVING,
                STORAGE_NAME, businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionSubpartitionValuesWithoutFormatSchema() throws Exception
    {
        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key. Please note that business object data key contains
        // sub-partition value that would require the relative business object format to have schema.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit.  Please note that the business object format is not going to have a schema.
        storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
            StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Override configuration to specify some settings required for testing.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN.getKey(), S3_OBJECT_TAGGER_ROLE_ARN);
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME.getKey(), S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        modifyPropertySourceInEnvironment(overrideMap);

        // Try to initiate a storage policy transition when sub-partition values are used and format has no schema.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail("Should throw an IllegalArgumentException when sub-partition values are used and format has no schema.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Schema must be defined when using subpartition values for business object format {%s}.",
                businessObjectFormatServiceTestHelper
                    .getExpectedBusinessObjectFormatKeyAsString(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION)),
                e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStorageFilesNoExists() throws Exception
    {
        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit.  This storage unit has no storage files registered.
        storageUnitDaoTestHelper.createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
            StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Override configuration to specify some settings required for testing.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN.getKey(), S3_OBJECT_TAGGER_ROLE_ARN);
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME.getKey(), S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        modifyPropertySourceInEnvironment(overrideMap);

        // Try to initiate a storage policy transition when storage unit has no storage files registered.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data has no storage files registered in \"%s\" storage. Business object data: {%s}", STORAGE_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStorageFileDoesNotMatchS3KeyPrefix() throws Exception
    {
        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Get the expected S3 key prefix for the business object data key.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(BDEF_NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // Add to the storage unit a storage file that is not matching the expected S3 key prefix.
        StorageFileEntity storageFileEntity =
            storageFileDaoTestHelper.createStorageFileEntity(storageUnitEntity, STORAGE_DIRECTORY_PATH + "/" + LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Override configuration to specify some settings required for testing.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN.getKey(), S3_OBJECT_TAGGER_ROLE_ARN);
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME.getKey(), S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        modifyPropertySourceInEnvironment(overrideMap);

        // Try to initiate a storage policy transition when storage unit has a storage file that is not matching the expected S3 key prefix.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey, INITIAL_VERSION));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                "Storage file \"%s\" registered with business object data {%s} in \"%s\" storage " + "does not match the expected S3 key prefix \"%s\".",
                storageFileEntity.getPath(), businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey), STORAGE_NAME,
                expectedS3KeyPrefix), e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionOtherBusinessObjectDataHasStorageFilesMatchingS3KeyPrefix() throws Exception
    {
        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create two business object data keys.
        List<BusinessObjectDataKey> businessObjectDataKeys = Arrays.asList(
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION),
            new BusinessObjectDataKey(BDEF_NAMESPACE_2, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION));

        // For the business object data keys, create and persist two storage units.
        List<StorageUnitEntity> storageUnitEntities = Arrays.asList(storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, businessObjectDataKeys.get(0), LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH), storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, businessObjectDataKeys.get(1), LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH));

        // Get the expected S3 key prefix for the business object data key.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(BDEF_NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // To both storage unit add storage files having the same S3 key prefix.
        List<StorageFileEntity> storageFileEntities = Arrays.asList(storageFileDaoTestHelper
            .createStorageFileEntity(storageUnitEntities.get(0), expectedS3KeyPrefix + "/" + LOCAL_FILES.get(0), FILE_SIZE_1_KB, ROW_COUNT_1000),
            storageFileDaoTestHelper
                .createStorageFileEntity(storageUnitEntities.get(1), expectedS3KeyPrefix + "/" + LOCAL_FILES.get(1), FILE_SIZE_1_KB, ROW_COUNT_1000));

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, StoragePolicyTransitionTypeEntity.GLACIER, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Override configuration to specify some settings required for testing.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_ARN.getKey(), S3_OBJECT_TAGGER_ROLE_ARN);
        overrideMap.put(ConfigurationValue.S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME.getKey(), S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        modifyPropertySourceInEnvironment(overrideMap);

        // Try to initiate a storage policy transition when storage has other
        // business object data storage files matching the expected S3 key prefix.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKeys.get(0), storagePolicyKey, INITIAL_VERSION));
            fail();
        }
        catch (IllegalStateException e)
        {
            assertEquals(String
                .format("Found %d registered storage file(s) matching business object data S3 key prefix in the storage that is not equal to the number " +
                    "of storage files (%d) registered with the business object data in that storage. " +
                    "Storage: {%s}, s3KeyPrefix {%s}, business object data: {%s}", storageFileEntities.size(), 1, STORAGE_NAME, expectedS3KeyPrefix,
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKeys.get(0))), e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testExecuteStoragePolicyTransition()
    {
        runExecuteStoragePolicyTransitionTest();
    }

    @Test
    public void testExecuteStoragePolicyTransitionWithInfoLoggingEnabled()
    {
        // Get the logger and the current logger level.
        LogLevel origLogLevel = getLogLevel(StoragePolicyProcessorHelperServiceImpl.class);

        // Set logging level to INFO.
        setLogLevel(StoragePolicyProcessorHelperServiceImpl.class, LogLevel.INFO);

        // Run the test and reset the logging level back to the original value.
        try
        {
            runExecuteStoragePolicyTransitionTest();
        }
        finally
        {
            setLogLevel(StoragePolicyProcessorHelperServiceImpl.class, origLogLevel);
        }
    }

    private void runExecuteStoragePolicyTransitionTest()
    {
        // Create S3FileTransferRequestParamsDto to access the S3 bucket location.
        // Since test S3 key prefix represents a directory, we add a trailing '/' character to it.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto =
            S3FileTransferRequestParamsDto.builder().s3BucketName(S3_BUCKET_NAME).s3KeyPrefix(TEST_S3_KEY_PREFIX + "/").build();

        // Create a list of storage files.
        List<StorageFile> storageFiles = new ArrayList<>();
        for (String file : LOCAL_FILES)
        {
            storageFiles.add(new StorageFile(String.format(String.format("%s/%s", TEST_S3_KEY_PREFIX, file)), FILE_SIZE_1_KB, ROW_COUNT));
        }

        try
        {
            // Put relative S3 files into the S3 bucket.
            for (StorageFile storageFile : storageFiles)
            {
                s3Operations
                    .putObject(new PutObjectRequest(S3_BUCKET_NAME, storageFile.getFilePath(), new ByteArrayInputStream(new byte[(int) FILE_SIZE_1_KB]), null),
                        null);
            }

            // Execute a storage policy transition.
            storagePolicyProcessorHelperService.executeStoragePolicyTransition(new StoragePolicyTransitionParamsDto(
                new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, DATA_VERSION), STORAGE_NAME, NO_S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, storageFiles, S3_ARCHIVE_TO_GLACIER_TAG_KEY, S3_ARCHIVE_TO_GLACIER_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN,
                S3_OBJECT_TAGGER_ROLE_SESSION_NAME));

            // Validate that we still have our S3 files in the S3 bucket.
            assertEquals(storageFiles.size(), s3Dao.listDirectory(s3FileTransferRequestParamsDto).size());
        }
        finally
        {
            // Delete test files from S3 storage.
            if (!s3Dao.listDirectory(s3FileTransferRequestParamsDto).isEmpty())
            {
                s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
            }
            s3Operations.rollback();
        }
    }

    @Test
    public void testCompleteStoragePolicyTransition()
    {
        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit with ARCHIVING state.
        StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ARCHIVING, NO_STORAGE_DIRECTORY_PATH);

        // Complete a storage policy transition.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_FILES, S3_ARCHIVE_TO_GLACIER_TAG_KEY, S3_ARCHIVE_TO_GLACIER_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN,
                S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);

        // Validate the results.
        assertEquals(new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX,
            StorageUnitStatusEntity.ARCHIVED, StorageUnitStatusEntity.ARCHIVING, NO_STORAGE_FILES, S3_ARCHIVE_TO_GLACIER_TAG_KEY,
            S3_ARCHIVE_TO_GLACIER_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME), storagePolicyTransitionParamsDto);

        // Validate that storage unit status is updated to ARCHIVED.
        assertEquals(StorageUnitStatusEntity.ARCHIVED, storageUnitEntity.getStatus().getCode());
    }

    @Test
    public void testCompleteStoragePolicyTransitionBusinessObjectDataNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Try to complete a storage policy transition when business object data does not exist.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_FILES, S3_ARCHIVE_TO_GLACIER_TAG_KEY, S3_ARCHIVE_TO_GLACIER_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN,
                S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        try
        {
            storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectDataKey, null), e.getMessage());
        }
    }

    @Test
    public void testCompleteStoragePolicyTransitionBusinessObjectDataStatusNotSupported()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity with a status that is not supported by the storage policy feature.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Try to complete a storage policy transition when business object data status is not supported by the storage policy feature.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_FILES, S3_ARCHIVE_TO_GLACIER_TAG_KEY, S3_ARCHIVE_TO_GLACIER_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN,
                S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        try
        {
            storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);
            fail("Should throw an IllegalArgumentException when business object data status is not supported by the storage policy feature.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Business object data status \"%s\" is not supported by the storage policy feature. Business object data: {%s}", BDATA_STATUS,
                    businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testCompleteStoragePolicyTransitionStorageUnitNoExists()
    {
        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity without any storage units.
        businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Try to complete a storage policy transition when storage unit does not exist.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_FILES, S3_ARCHIVE_TO_GLACIER_TAG_KEY, S3_ARCHIVE_TO_GLACIER_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN,
                S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        try
        {
            storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", STORAGE_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testCompleteStoragePolicyTransitionStorageUnitNotInArchivingState()
    {
        // Create and persist the relative database entities.
        storagePolicyServiceTestHelper
            .createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BDEF_NAMESPACE, BDEF_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(StoragePolicyTransitionTypeEntity.GLACIER));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit having a non-ARCHIVING storage unit status.
        storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, STORAGE_UNIT_STATUS,
                NO_STORAGE_DIRECTORY_PATH);

        // Try to complete a storage policy transition when storage unit does not have ARCHIVING status.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_ENDPOINT, S3_BUCKET_NAME, TEST_S3_KEY_PREFIX, NO_STORAGE_UNIT_STATUS,
                NO_STORAGE_UNIT_STATUS, NO_STORAGE_FILES, S3_ARCHIVE_TO_GLACIER_TAG_KEY, S3_ARCHIVE_TO_GLACIER_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN,
                S3_OBJECT_TAGGER_ROLE_SESSION_NAME);
        try
        {
            storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                "Storage unit status is \"%s\", but must be \"%s\" for storage policy transition to proceed. " + "Storage: {%s}, business object data: {%s}",
                STORAGE_UNIT_STATUS, StorageUnitStatusEntity.ARCHIVING, STORAGE_NAME,
                businessObjectDataServiceTestHelper.getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    /**
     * This method is to get the coverage for the storage policy processor helper service methods that start new transactions.
     */
    @Test
    public void testStoragePolicyProcessorHelperServiceMethodsNewTx()
    {
        try
        {
            storagePolicyProcessorHelperServiceImpl.initiateStoragePolicyTransition(null);
            fail("Should throw an IllegalArgumentException.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy selection must be specified.", e.getMessage());
        }

        try
        {
            storagePolicyProcessorHelperServiceImpl.executeStoragePolicyTransition(null);
            fail("Should throw an NullPointerException.");
        }
        catch (NullPointerException e)
        {
            assertNull(e.getMessage());
        }

        try
        {
            storagePolicyProcessorHelperServiceImpl.completeStoragePolicyTransition(null);
            fail("Should throw an NullPointerException.");
        }
        catch (NullPointerException e)
        {
            assertNull(e.getMessage());
        }
    }
}
