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

import org.finra.herd.dao.impl.MockGlacierOperationsImpl;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.dto.StoragePolicyTransitionParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

public class StoragePolicyProcessorHelperServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "storagePolicyProcessorHelperServiceImpl")
    private StoragePolicyProcessorHelperService storagePolicyProcessorHelperServiceImpl;

    @Test
    public void testInitiateStoragePolicyTransition()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit in the source storage.
        StorageUnitEntity sourceStorageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Get the expected S3 key prefix for the business object data key.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(BOD_NAMESPACE, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // Add a storage file to the source storage unit.
        createStorageFileEntity(sourceStorageUnitEntity, expectedS3KeyPrefix + "/" + LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Initiate a storage policy transition.
        storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
    }

    @Test
    public void testInitiateStoragePolicyTransitionDestinationStorageUnitDisabled()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit in the source storage.
        StorageUnitEntity sourceStorageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Get the expected S3 key prefix for the business object data key.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(BOD_NAMESPACE, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // Add a storage file to the source storage unit.
        createStorageFileEntity(sourceStorageUnitEntity, expectedS3KeyPrefix + "/" + LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);

        // Create and persist a destination storage unit with DISABLED status.
        StorageUnitEntity destinationStorageUnitEntity =
            createStorageUnitEntity(herdDao.getStorageByName(STORAGE_NAME_2), sourceStorageUnitEntity.getBusinessObjectData(), StorageUnitStatusEntity.DISABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Initiate a storage policy transition.
        storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));

        // Validate the results.
        assertEquals(StorageUnitStatusEntity.ARCHIVING, destinationStorageUnitEntity.getStatus().getCode());
    }

    @Test
    public void testInitiateStoragePolicyTransitionInvalidParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
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
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(null, storagePolicyKey));
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
                new BusinessObjectDataKey(NO_BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), storagePolicyKey));
            fail("Should throw an IllegalArgumentException when when business object definition namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to initiate a storage policy transition when storage policy key is not specified.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, null));
            fail("Should throw an IllegalArgumentException when when storage policy key is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy key must be specified.", e.getMessage());
        }

        // Try to initiate a storage policy transition when storage policy namespace is not specified.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, new StoragePolicyKey(null, STORAGE_POLICY_NAME)));
            fail("Should throw an IllegalArgumentException when when storage policy namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to initiate a storage policy transition when storage policy name is not specified.
        try
        {
            storagePolicyProcessorHelperService
                .initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, null)));
            fail("Should throw an IllegalArgumentException when when storage policy name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionBusinessObjectDataNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to initiate a storage policy transition when business object data does not exist.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectDataKey, null), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionBusinessObjectDataStatusNotSupported()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity with a status that is not supported by the storage policy feature.
        createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to initiate a storage policy transition when business object data status is not supported by the storage policy feature.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an IllegalArgumentException when business object data status is not supported by the storage policy feature.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Business object data status \"%s\" is not supported by the storage policy feature. Business object data: {%s}", BDATA_STATUS,
                    getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStoragePolicyNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to initiate a storage policy transition when business object data does not exist.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an ObjectNotFoundException when storage policy does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage policy with name \"%s\" does not exist for \"%s\" namespace.", storagePolicyKey.getStoragePolicyName(),
                storagePolicyKey.getNamespace()), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStoragePolicyFilterStorageInvalidStoragePlatform()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage entity with storage platform type not set to S3.
        createStorageEntity(STORAGE_NAME, STORAGE_PLATFORM_CODE);

        // Create and persist a storage policy entity with storage policy filter storage having a non-S3 storage platform type.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when storage policy filter storage has a non-S3 storage platform type.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an IllegalArgumentException when using non-S3 storage platform for storage policy filter storage.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage platform for storage policy filter storage with name \"%s\" is not \"%s\". Storage policy: {%s}", STORAGE_NAME,
                StoragePlatformEntity.S3, getExpectedStoragePolicyKeyAsString(storagePolicyKey)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStoragePolicyFilterStoragePathPrefixValidationNotEnabled()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist an S3 storage without an S3 path prefix validation option configured.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString()));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.FALSE.toString()));
        createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, attributes);

        // Create and persist a storage policy entity with the storage policy filter storage having no S3 path prefix validation enabled.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when storage policy filter storage has no S3 path prefix validation enabled.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an IllegalStateException when storage policy filter storage has no S3 path prefix validation enabled.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Path prefix validation must be enabled on \"%s\" storage. Storage policy: {%s}", STORAGE_NAME,
                getExpectedStoragePolicyKeyAsString(storagePolicyKey)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStoragePolicyFilterStorageFileExistenceValidationNotEnabled()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create an S3 storage without the S3 file existence validation enabled.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.FALSE.toString()));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()));
        createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, attributes);

        // Create and persist a storage policy entity with storage policy filter storage has no S3 bucket name attribute configured.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when storage policy filter storage has no S3 file existence validation enabled.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an IllegalStateException when storage policy filter storage has no S3 file existence validation enabled.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("File existence validation must be enabled on \"%s\" storage. Storage policy: {%s}", STORAGE_NAME,
                getExpectedStoragePolicyKeyAsString(storagePolicyKey)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStoragePolicyFilterStorageBucketNameNotConfigured()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist an S3 storage entity without S3 bucket name configured.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE), Boolean.TRUE.toString()));
        createStorageEntity(STORAGE_NAME, StoragePlatformEntity.S3, attributes);

        // Create and persist a storage policy entity with storage policy filter storage has no S3 bucket name attribute configured.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when storage policy filter storage has no S3 bucket name attribute configured.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an IllegalStateException when storage policy filter storage has no S3 bucket name attribute configured.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), STORAGE_NAME), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionDestinationStorageInvalidStoragePlatform()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity with storage policy transition destination storage having a non-GLACIER storage platform type.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME);

        // Try to initiate a storage policy transition when storage policy transition destination storage has a non-GLACIER storage platform type.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an IllegalArgumentException when using non-GLACIER storage platform for storage policy transition destination storage.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Storage platform for storage policy transition destination storage with name \"%s\" is not \"%s\". Storage policy: {%s}", STORAGE_NAME,
                    StoragePlatformEntity.GLACIER, getExpectedStoragePolicyKeyAsString(storagePolicyKey)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionDestinationStorageVaultNameNotConfigured()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), new ArrayList<>());

        // Create a Glacier storage without any attributes.
        createStorageEntity(STORAGE_NAME_2, StoragePlatformEntity.GLACIER);

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity with storage policy transition destination storage having no Glacier vault name attribute configured.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when storage policy transition destination storage has no Glacier vault name attribute configured.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an IllegalStateException when storage policy transition destination storage has no Glacier vault name attribute configured.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.GLACIER_ATTRIBUTE_NAME_VAULT_NAME), STORAGE_NAME_2), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionSourceStorageUnitNoExists()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity without any storage units.
        createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when source storage unit does not exist.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an ObjectNotFoundException when source storage unit does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", STORAGE_NAME,
                getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionSourceStorageUnitNotEnabled()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit in the source storage having a non-ENABLED storage unit status.
        createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, STORAGE_UNIT_STATUS,
            NO_STORAGE_DIRECTORY_PATH);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when source storage unit does not have ENABLED status.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an IllegalArgumentException when source storage unit does not have ENABLED status.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Source storage unit status is \"%s\", but must be \"%s\" for storage policy transition to proceed. " +
                "Storage: {%s}, business object data: {%s}", STORAGE_UNIT_STATUS, StorageUnitStatusEntity.ENABLED, STORAGE_NAME,
                getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionDestinationStorageUnitNotDisabled()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit in the source storage having the ENABLED storage unit status.
        StorageUnitEntity sourceStorageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a destination storage unit with a non-DISABLED status.
        createStorageUnitEntity(herdDao.getStorageByName(STORAGE_NAME_2), sourceStorageUnitEntity.getBusinessObjectData(), STORAGE_UNIT_STATUS,
            NO_STORAGE_DIRECTORY_PATH);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when destination storage unit exists and does not have DISABLED status.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an AlreadyExistsException when destination storage unit exists and does not have DISABLED status.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String
                .format("Destination storage unit already exists and has \"%s\" status. Storage: {%s}, business object data: {%s}", STORAGE_UNIT_STATUS,
                    STORAGE_NAME_2, getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionMultipleDestinationStorageFilesExist()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit in the source storage having the ENABLED storage unit status.
        StorageUnitEntity sourceStorageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a destination storage unit.
        StorageUnitEntity destinationStorageUnitEntity =
            createStorageUnitEntity(herdDao.getStorageByName(STORAGE_NAME_2), sourceStorageUnitEntity.getBusinessObjectData(), StorageUnitStatusEntity.DISABLED,
                NO_STORAGE_DIRECTORY_PATH);

        // Add two storage files to the destination storage unit.
        List<String> destinationStorageFilePaths = Arrays.asList(FILE_NAME, FILE_NAME_2);
        for (String filePath : destinationStorageFilePaths)
        {
            createStorageFileEntity(destinationStorageUnitEntity, filePath, FILE_SIZE, ROW_COUNT);
        }

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when destination storage unit exists and does not have DISABLED status.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an IllegalStateException when destination storage unit contains multiple storage files.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format(
                "Destination storage unit has %d storage files, but must have none or just one storage file. " + "Storage: {%s}, business object data: {%s}",
                destinationStorageFilePaths.size(), STORAGE_NAME_2, getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionSubpartitionValuesWithoutFormatSchema()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key. Please note that business object data key contains
        // sub-partition value that would require the relative business object format to have schema.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit in the source storage.  Please note that the business object format is not going to have a schema.
        createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
            StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when sub-partition values are used and format has no schema.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an IllegalArgumentException when sub-partition values are used and format has no schema.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Schema must be defined when using subpartition values for business object format {%s}.",
                getExpectedBusinessObjectFormatKeyAsString(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStorageFilesNoExists()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit in the source storage.  This storage unit has no storage files registered.
        createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
            StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when source storage unit has no storage files registered.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an IllegalArgumentException when source storage unit has no storage files registered.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data has no storage files registered in \"%s\" storage. Business object data: {%s}", STORAGE_NAME,
                getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStorageFilesSizeGreaterThanThreshold() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_SIZE_THRESHOLD_GB.getKey(), 0);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Create and persist the relative database entities.
            createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
                Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

            // Create a business object data key.
            BusinessObjectDataKey businessObjectDataKey =
                new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, DATA_VERSION);

            // Create and persist a storage unit in the source storage.  This storage unit has no storage files registered.
            StorageUnitEntity sourceStorageUnitEntity =
                createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                    StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

            // Get the expected S3 key prefix for the business object data key.
            String expectedS3KeyPrefix =
                getExpectedS3KeyPrefix(BOD_NAMESPACE, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                    PARTITION_VALUE, null, null, DATA_VERSION);

            // Add a non-zero size storage file to the source storage unit.
            createStorageFileEntity(sourceStorageUnitEntity, expectedS3KeyPrefix + "/" + LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);

            // Create a storage policy key.
            StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

            // Create and persist a storage policy entity.
            createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

            // Try to initiate a storage policy transition when storage files size is greater than the threshold.
            try
            {
                storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
                fail("Should throw an IllegalArgumentException when source storage unit has no storage files registered.");
            }
            catch (IllegalArgumentException e)
            {
                assertEquals(String.format("Total size of storage files (%d bytes) for business object data in \"%s\" storage is greater than " +
                    "the configured threshold of 0 GB (0 bytes) as per \"%s\" configuration entry. Business object data: {%s}", FILE_SIZE_1_KB, STORAGE_NAME,
                    ConfigurationValue.STORAGE_POLICY_PROCESSOR_BDATA_SIZE_THRESHOLD_GB.getKey(),
                    getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
            }
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionStorageFileDoesNotMatchS3KeyPrefix()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit in the source storage.
        StorageUnitEntity sourceStorageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Get the expected S3 key prefix for the business object data key.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(BOD_NAMESPACE, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // Add a storage file which is not matching the expected S3 key prefix to the source storage unit.
        StorageFileEntity storageFileEntity =
            createStorageFileEntity(sourceStorageUnitEntity, STORAGE_DIRECTORY_PATH + "/" + LOCAL_FILE, FILE_SIZE_1_KB, ROW_COUNT_1000);

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when source storage unit has a storage file that is not matching the expected S3 key prefix.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKey, storagePolicyKey));
            fail("Should throw an IllegalArgumentException when source storage unit has a storage file that is not matching the expected S3 key prefix.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                "Storage file \"%s\" registered with business object data {%s} in \"%s\" storage " + "does not match the expected S3 key prefix \"%s\".",
                storageFileEntity.getPath(), getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey), STORAGE_NAME, expectedS3KeyPrefix),
                e.getMessage());
        }
    }

    @Test
    public void testInitiateStoragePolicyTransitionOtherBusinessObjectDataHasStorageFilesMatchingS3KeyPrefix()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create two business object data keys.
        List<BusinessObjectDataKey> businessObjectDataKeys = Arrays.asList(
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION),
            new BusinessObjectDataKey(BOD_NAMESPACE_2, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION));

        // For the business object data keys, create and persist two storage units in the source storage.
        List<StorageUnitEntity> sourceStorageUnitEntities = Arrays.asList(
            createStorageUnitEntity(STORAGE_NAME, businessObjectDataKeys.get(0), LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH),
            createStorageUnitEntity(STORAGE_NAME, businessObjectDataKeys.get(1), LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH));

        // Get the expected S3 key prefix for the business object data key.
        String expectedS3KeyPrefix =
            getExpectedS3KeyPrefix(BOD_NAMESPACE, DATA_PROVIDER_NAME, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_KEY,
                PARTITION_VALUE, null, null, DATA_VERSION);

        // To both storage unit add storage files having the same S3 key prefix.
        List<StorageFileEntity> storageFileEntities = Arrays
            .asList(createStorageFileEntity(sourceStorageUnitEntities.get(0), expectedS3KeyPrefix + "/" + LOCAL_FILES.get(0), FILE_SIZE_1_KB, ROW_COUNT_1000),
                createStorageFileEntity(sourceStorageUnitEntities.get(1), expectedS3KeyPrefix + "/" + LOCAL_FILES.get(1), FILE_SIZE_1_KB, ROW_COUNT_1000));

        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2);

        // Try to initiate a storage policy transition when source storage has other
        // business object data storage files matching the expected S3 key prefix.
        try
        {
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(new StoragePolicySelection(businessObjectDataKeys.get(0), storagePolicyKey));
            fail("Should throw an IllegalStateException when source storage has other business object data storage files matching the expected S3 key prefix.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String
                .format("Found %d registered storage file(s) matching business object data S3 key prefix in the storage that is not equal to the number " +
                    "of storage files (%d) registered with the business object data in that storage. " +
                    "Storage: {%s}, s3KeyPrefix {%s}, business object data: {%s}", storageFileEntities.size(), 1, STORAGE_NAME, expectedS3KeyPrefix,
                    getExpectedBusinessObjectDataKeyAsString(businessObjectDataKeys.get(0))), e.getMessage());
        }
    }

    @Test
    public void testCompleteStoragePolicyTransition()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit in the source storage.
        StorageUnitEntity sourceStorageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a storage unit in the destination storage.
        StorageUnitEntity destinationStorageUnitEntity =
            createStorageUnitEntity(herdDao.getStorageByName(STORAGE_NAME_2), sourceStorageUnitEntity.getBusinessObjectData(),
                StorageUnitStatusEntity.ARCHIVING, NO_STORAGE_DIRECTORY_PATH);

        // Complete a storage policy transition.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, STORAGE_UNIT_ID, TEST_S3_KEY_PREFIX, NO_STORAGE_FILES,
                FILE_SIZE, STORAGE_NAME_2, GLACIER_VAULT_NAME,
                new StorageFile(FILE_NAME, FILE_SIZE_2, NO_ROW_COUNT, MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID));
        storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);

        // Validate the results.
        assertEquals(StorageUnitStatusEntity.DISABLED, sourceStorageUnitEntity.getStatus().getCode());
        assertEquals(StorageUnitStatusEntity.ENABLED, destinationStorageUnitEntity.getStatus().getCode());
        assertEquals(1, destinationStorageUnitEntity.getStorageFiles().size());
        StorageFileEntity destinationStorageFileEntity = destinationStorageUnitEntity.getStorageFiles().iterator().next();
        assertNotNull(destinationStorageFileEntity);
        assertEquals(FILE_NAME, destinationStorageFileEntity.getPath());
        assertEquals(FILE_SIZE_2, destinationStorageFileEntity.getFileSizeBytes());
        assertNull(destinationStorageFileEntity.getRowCount());
        assertEquals(MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID, destinationStorageFileEntity.getArchiveId());
    }

    @Test
    public void testCompleteStoragePolicyTransitionDestinationStorageFileExists()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit in the source storage.
        StorageUnitEntity sourceStorageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a storage unit in the destination storage.
        StorageUnitEntity destinationStorageUnitEntity =
            createStorageUnitEntity(herdDao.getStorageByName(STORAGE_NAME_2), sourceStorageUnitEntity.getBusinessObjectData(),
                StorageUnitStatusEntity.ARCHIVING, NO_STORAGE_DIRECTORY_PATH);

        // Add a storage file to the destination storage unit.
        createStorageFileEntity(destinationStorageUnitEntity, FILE_NAME, FILE_SIZE, ROW_COUNT);

        // Complete a storage policy transition.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, STORAGE_UNIT_ID, TEST_S3_KEY_PREFIX, NO_STORAGE_FILES,
                FILE_SIZE_1_KB, STORAGE_NAME_2, GLACIER_VAULT_NAME,
                new StorageFile(FILE_NAME_2, FILE_SIZE_2, ROW_COUNT_2, MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID));
        storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);

        // Validate the results.
        assertEquals(StorageUnitStatusEntity.DISABLED, sourceStorageUnitEntity.getStatus().getCode());
        assertEquals(StorageUnitStatusEntity.ENABLED, destinationStorageUnitEntity.getStatus().getCode());
        assertEquals(1, destinationStorageUnitEntity.getStorageFiles().size());
        StorageFileEntity destinationStorageFileEntity = destinationStorageUnitEntity.getStorageFiles().iterator().next();
        assertNotNull(destinationStorageFileEntity);
        assertEquals(FILE_NAME_2, destinationStorageFileEntity.getPath());
        assertEquals(FILE_SIZE_2, destinationStorageFileEntity.getFileSizeBytes());
        assertNull(destinationStorageFileEntity.getRowCount());
        assertEquals(MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID, destinationStorageFileEntity.getArchiveId());
    }

    @Test
    public void testCompleteStoragePolicyTransitionBusinessObjectDataNoExists()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Try to complete a storage policy transition when business object data does not exist.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, STORAGE_UNIT_ID, TEST_S3_KEY_PREFIX, NO_STORAGE_FILES,
                FILE_SIZE_1_KB, STORAGE_NAME_2, GLACIER_VAULT_NAME,
                new StorageFile(FILE_NAME_2, FILE_SIZE_2, ROW_COUNT_2, MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID));
        try
        {
            storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(businessObjectDataKey, null), e.getMessage());
        }
    }

    @Test
    public void testCompleteStoragePolicyTransitionBusinessObjectDataStatusNotSupported()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity with a status that is not supported by the storage policy feature.
        createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BDATA_STATUS);

        // Try to complete a storage policy transition when business object data status is not supported by the storage policy feature.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, STORAGE_UNIT_ID, TEST_S3_KEY_PREFIX, NO_STORAGE_FILES,
                FILE_SIZE_1_KB, STORAGE_NAME_2, GLACIER_VAULT_NAME,
                new StorageFile(FILE_NAME_2, FILE_SIZE_2, ROW_COUNT_2, MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID));
        try
        {
            storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);
            fail("Should throw an IllegalArgumentException when business object data status is not supported by the storage policy feature.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Business object data status \"%s\" is not supported by the storage policy feature. Business object data: {%s}", BDATA_STATUS,
                    getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testCompleteStoragePolicyTransitionSourceStorageUnitNoExists()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a business object data entity without any storage units.
        createBusinessObjectDataEntity(businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Try to complete a storage policy transition when source storage unit does not exist.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, STORAGE_UNIT_ID, TEST_S3_KEY_PREFIX, NO_STORAGE_FILES,
                FILE_SIZE_1_KB, STORAGE_NAME_2, GLACIER_VAULT_NAME,
                new StorageFile(FILE_NAME_2, FILE_SIZE_2, ROW_COUNT_2, MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID));
        try
        {
            storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);
            fail("Should throw an ObjectNotFoundException when source storage unit does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", STORAGE_NAME,
                getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testCompleteStoragePolicyTransitionSourceStorageUnitNotEnabled()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit in the source storage having a non-ENABLED storage unit status.
        createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID, STORAGE_UNIT_STATUS,
            NO_STORAGE_DIRECTORY_PATH);

        // Try to complete a storage policy transition when source storage unit does not have ENABLED status.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, STORAGE_UNIT_ID, TEST_S3_KEY_PREFIX, NO_STORAGE_FILES,
                FILE_SIZE_1_KB, STORAGE_NAME_2, GLACIER_VAULT_NAME,
                new StorageFile(FILE_NAME_2, FILE_SIZE_2, ROW_COUNT_2, MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID));
        try
        {
            storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);
            fail("Should throw an IllegalArgumentException when source storage unit does not have ENABLED status.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Source storage unit status is \"%s\", but must be \"%s\" for storage policy transition to proceed. " +
                "Storage: {%s}, business object data: {%s}", STORAGE_UNIT_STATUS, StorageUnitStatusEntity.ENABLED, STORAGE_NAME,
                getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testCompleteStoragePolicyTransitionDestinationStorageUnitNoExists()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit in the source storage.
        createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
            StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Try to complete a storage policy transition when destination storage unit does not exist.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, STORAGE_UNIT_ID, TEST_S3_KEY_PREFIX, NO_STORAGE_FILES,
                FILE_SIZE_1_KB, STORAGE_NAME_2, GLACIER_VAULT_NAME,
                new StorageFile(FILE_NAME_2, FILE_SIZE_2, ROW_COUNT_2, MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID));
        try
        {
            storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);
            fail("Should throw an IllegalArgumentException when destination storage unit does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Could not find storage unit in \"%s\" storage for the business object data {%s}.", STORAGE_NAME_2,
                getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testCompleteStoragePolicyTransitionDestinationStorageUnitNotInArchivingState()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create and persist a storage unit in the source storage.
        StorageUnitEntity sourceStorageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a storage unit in the destination storage having a non-ARCHIVING storage unit status.
        createStorageUnitEntity(herdDao.getStorageByName(STORAGE_NAME_2), sourceStorageUnitEntity.getBusinessObjectData(), STORAGE_UNIT_STATUS,
            NO_STORAGE_DIRECTORY_PATH);

        // Try to complete a storage policy transition when destination storage unit does not have ARCHIVING status.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, STORAGE_UNIT_ID, TEST_S3_KEY_PREFIX, NO_STORAGE_FILES,
                FILE_SIZE_1_KB, STORAGE_NAME_2, GLACIER_VAULT_NAME,
                new StorageFile(FILE_NAME_2, FILE_SIZE_2, ROW_COUNT_2, MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID));
        try
        {
            storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);
            fail("Should throw an IllegalArgumentException when destination storage unit does not have ARCHIVING status.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Destination storage unit status is \"%s\", but must be \"%s\" for storage policy transition to proceed. " +
                "Storage: {%s}, business object data: {%s}", STORAGE_UNIT_STATUS, StorageUnitStatusEntity.ARCHIVING, STORAGE_NAME_2,
                getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
        }
    }

    @Test
    public void testCompleteStoragePolicyTransitionMultipleDestinationStorageFilesExist()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting(STORAGE_POLICY_NAMESPACE_CD, Arrays.asList(STORAGE_POLICY_RULE_TYPE), BOD_NAMESPACE, BOD_NAME,
            Arrays.asList(FORMAT_FILE_TYPE_CODE), Arrays.asList(STORAGE_NAME), Arrays.asList(STORAGE_NAME_2));

        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BOD_NAMESPACE, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION);

        // Create and persist a storage unit in the source storage.
        StorageUnitEntity sourceStorageUnitEntity =
            createStorageUnitEntity(STORAGE_NAME, businessObjectDataKey, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.ENABLED, NO_STORAGE_DIRECTORY_PATH);

        // Create and persist a storage unit in the destination storage.
        StorageUnitEntity destinationStorageUnitEntity =
            createStorageUnitEntity(herdDao.getStorageByName(STORAGE_NAME_2), sourceStorageUnitEntity.getBusinessObjectData(),
                StorageUnitStatusEntity.ARCHIVING, NO_STORAGE_DIRECTORY_PATH);

        // Add two storage files to the destination storage unit.
        List<String> destinationStorageFilePaths = Arrays.asList(FILE_NAME, FILE_NAME_2);
        for (String filePath : destinationStorageFilePaths)
        {
            createStorageFileEntity(destinationStorageUnitEntity, filePath, FILE_SIZE, ROW_COUNT);
        }

        // Try to complete a storage policy transition when destination storage unit contains multiple storage files.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto =
            new StoragePolicyTransitionParamsDto(businessObjectDataKey, STORAGE_NAME, S3_BUCKET_NAME, STORAGE_UNIT_ID, TEST_S3_KEY_PREFIX, NO_STORAGE_FILES,
                FILE_SIZE_1_KB, STORAGE_NAME_2, GLACIER_VAULT_NAME,
                new StorageFile(FILE_NAME_3, FILE_SIZE_1_KB, NO_ROW_COUNT, MockGlacierOperationsImpl.MOCK_GLACIER_ARCHIVE_ID));
        try
        {
            storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);
            fail("Should throw an IllegalStateException when destination storage unit contains multiple storage files.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format(
                "Destination storage unit has %d storage files, but must have none or just one storage file. " + "Storage: {%s}, business object data: {%s}",
                destinationStorageFilePaths.size(), STORAGE_NAME_2, getExpectedBusinessObjectDataKeyAsString(businessObjectDataKey)), e.getMessage());
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

        try
        {
            storagePolicyProcessorHelperServiceImpl.executeStoragePolicyTransitionAfterStep(null);
            fail("Should throw an NullPointerException.");
        }
        catch (NullPointerException e)
        {
            assertNull(e.getMessage());
        }
    }
}
