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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.StoragePolicy;
import org.finra.herd.model.api.xml.StoragePolicyCreateRequest;
import org.finra.herd.model.api.xml.StoragePolicyFilter;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.api.xml.StoragePolicyRule;
import org.finra.herd.model.api.xml.StoragePolicyTransition;
import org.finra.herd.model.api.xml.StoragePolicyUpdateRequest;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StoragePolicyEntity;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;

/**
 * This class tests various functionality within the storage policy REST controller.
 */
public class StoragePolicyServiceTest extends AbstractServiceTest
{
    // Unit tests for createStoragePolicy().

    @Test
    public void testCreateStoragePolicy()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create a storage policy.
        StoragePolicy resultStoragePolicy = storagePolicyService.createStoragePolicy(
            createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));

        // Validate the returned object.
        assertEquals(
            new StoragePolicy(resultStoragePolicy.getId(), storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
                new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME),
                new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.ENABLED), resultStoragePolicy);
    }

    @Test
    public void testCreateStoragePolicyMissingRequiredParameters()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to create a storage policy instance when storage policy namespace is not specified.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(new StoragePolicyKey(BLANK_TEXT, STORAGE_POLICY_NAME), STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE,
                    BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage policy namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to create a storage policy instance when storage policy name is not specified.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, BLANK_TEXT), STORAGE_POLICY_RULE_TYPE,
                    STORAGE_POLICY_RULE_VALUE, STORAGE_POLICY_NAMESPACE_CD, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2,
                    StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage policy name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy name must be specified.", e.getMessage());
        }

        // Try to create a storage policy instance when storage policy rule type is not specified.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(storagePolicyKey, BLANK_TEXT, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                    FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage policy rule type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy rule type must be specified.", e.getMessage());
        }

        // Try to create a storage policy instance when storage policy rule value is not specified.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, null, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                    FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage policy rule value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy rule value must be specified.", e.getMessage());
        }

        // Try to create a storage policy instance when business object definition name is specified without business object definition namespace.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BLANK_TEXT, BDEF_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when business object definition name is specified without business object definition namespace.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition name and namespace must be specified together.", e.getMessage());
        }

        // Try to create a storage policy instance when business object definition namespace is specified without business object definition name.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BLANK_TEXT,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when business object definition namespace is specified without business object definition name.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition name and namespace must be specified together.", e.getMessage());
        }

        // Try to create a storage policy instance when business object format file type is specified without business object format usage.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT,
                    FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when business object format file type is specified without business object format usage.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format usage and file type must be specified together.", e.getMessage());
        }

        // Try to create a storage policy instance when business object format usage is specified without business object format file type.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                    FORMAT_USAGE_CODE, BLANK_TEXT, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when business object format usage is specified without business object format file type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format usage and file type must be specified together.", e.getMessage());
        }

        // Try to create a storage policy instance when storage name is not specified.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, BLANK_TEXT, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name must be specified.", e.getMessage());
        }

        // Try to create a storage policy instance when destination storage name is not specified.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, BLANK_TEXT, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when destination storage name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A destination storage name must be specified.", e.getMessage());
        }

        // Try to create a storage policy instance when storage policy status is not specified.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when storage policy status is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy status must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateStoragePolicyMissingOptionalParametersPassedAsWhitespace()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create a storage policy without specifying any of the optional parameters (passing them as whitespace characters).
        StoragePolicy resultStoragePolicy = storagePolicyService.createStoragePolicy(
            createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BLANK_TEXT, BLANK_TEXT, BLANK_TEXT,
                BLANK_TEXT, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));

        // Validate the returned object.
        assertEquals(
            new StoragePolicy(resultStoragePolicy.getId(), storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
                new StoragePolicyFilter(null, null, null, null, STORAGE_NAME), new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.ENABLED),
            resultStoragePolicy);
    }

    @Test
    public void testCreateStoragePolicyMissingOptionalParametersPassedAsNulls()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create a storage policy without specifying any of the optional parameters (passing them as null values).
        StoragePolicy resultStoragePolicy = storagePolicyService.createStoragePolicy(
            createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, null, null, null, null, STORAGE_NAME,
                STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));

        // Validate the returned object.
        assertEquals(
            new StoragePolicy(resultStoragePolicy.getId(), storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
                new StoragePolicyFilter(null, null, null, null, STORAGE_NAME), new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.ENABLED),
            resultStoragePolicy);
    }

    @Test
    public void testCreateStoragePolicyTrimParameters()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create a storage policy using input parameters with leading and trailing empty spaces.
        StoragePolicy resultStoragePolicy = storagePolicyService.createStoragePolicy(
            createStoragePolicyCreateRequest(new StoragePolicyKey(addWhitespace(STORAGE_POLICY_NAMESPACE_CD), addWhitespace(STORAGE_POLICY_NAME)),
                addWhitespace(STORAGE_POLICY_RULE_TYPE), STORAGE_POLICY_RULE_VALUE, addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME),
                addWhitespace(FORMAT_USAGE_CODE), addWhitespace(FORMAT_FILE_TYPE_CODE), addWhitespace(STORAGE_NAME), addWhitespace(STORAGE_NAME_2),
                addWhitespace(StoragePolicyStatusEntity.ENABLED)));

        // Validate the returned object.
        assertEquals(new StoragePolicy(resultStoragePolicy.getId(), new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
            new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME),
            new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.ENABLED), resultStoragePolicy);
    }

    @Test
    public void testCreateStoragePolicyUpperCaseParameters()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create a storage policy using upper case input parameters.
        StoragePolicy resultStoragePolicy = storagePolicyService.createStoragePolicy(
            createStoragePolicyCreateRequest(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toUpperCase(), STORAGE_POLICY_NAME.toUpperCase()),
                STORAGE_POLICY_RULE_TYPE.toUpperCase(), STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(),
                FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(), STORAGE_NAME.toUpperCase(), STORAGE_NAME_2.toUpperCase(),
                StoragePolicyStatusEntity.ENABLED.toUpperCase()));

        // Validate the returned object.
        assertEquals(new StoragePolicy(resultStoragePolicy.getId(), new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME.toUpperCase()),
            new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
            new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE, STORAGE_NAME),
            new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.ENABLED), resultStoragePolicy);
    }

    @Test
    public void testCreateStoragePolicyLowerCaseParameters()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create a storage policy using lower case input parameters.
        StoragePolicy resultStoragePolicy = storagePolicyService.createStoragePolicy(
            createStoragePolicyCreateRequest(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toLowerCase(), STORAGE_POLICY_NAME.toLowerCase()),
                STORAGE_POLICY_RULE_TYPE.toLowerCase(), STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(),
                FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(), STORAGE_NAME.toLowerCase(), STORAGE_NAME_2.toLowerCase(),
                StoragePolicyStatusEntity.ENABLED.toLowerCase()));

        // Validate the returned object.
        assertEquals(new StoragePolicy(resultStoragePolicy.getId(), new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME.toLowerCase()),
            new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
            new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE, STORAGE_NAME),
            new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.ENABLED), resultStoragePolicy);
    }

    @Test
    public void testCreateStoragePolicyInvalidParameters()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        StoragePolicyCreateRequest request;

        // Try to create a storage policy using non-existing storage policy namespace.
        request =
            createStoragePolicyCreateRequest(new StoragePolicyKey("I_DO_NOT_EXIST", STORAGE_POLICY_NAME), STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE,
                BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an ObjectNotFoundException when using non-existing storage policy namespace.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Namespace \"%s\" doesn't exist.", request.getStoragePolicyKey().getNamespace()), e.getMessage());
        }

        // Try to create a storage policy when storage policy namespace contains a forward slash character.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(new StoragePolicyKey(addSlash(STORAGE_POLICY_NAMESPACE_CD), STORAGE_POLICY_NAME), STORAGE_POLICY_RULE_TYPE,
                    STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2,
                    StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage policy namespace contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a storage policy when storage policy name contains a forward slash character.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, addSlash(STORAGE_POLICY_NAME)), STORAGE_POLICY_RULE_TYPE,
                    STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2,
                    StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage policy name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Storage policy name can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a storage policy using non-existing storage policy rule type.
        request = createStoragePolicyCreateRequest(storagePolicyKey, "I_DO_NOT_EXIST", STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an ObjectNotFoundException when using non-existing storage policy rule type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage policy rule type with code \"%s\" doesn't exist.", request.getStoragePolicyRule().getRuleType()),
                e.getMessage());
        }

        // Try to create a storage policy using a negative storage policy rule value.
        request = createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, -1, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an IllegalArgumentException when using a negative storage policy rule value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Storage policy rule value must be a positive integer or zero.", e.getMessage());
        }

        // Try to create a storage policy using non-existing business object definition namespace.
        request = createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, "I_DO_NOT_EXIST", BDEF_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object definition namespace.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".",
                request.getStoragePolicyFilter().getBusinessObjectDefinitionName(), request.getStoragePolicyFilter().getNamespace()), e.getMessage());
        }

        // Try to create a storage policy using non-existing business object definition name.
        request = createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, "I_DO_NOT_EXIST",
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object definition name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".",
                request.getStoragePolicyFilter().getBusinessObjectDefinitionName(), request.getStoragePolicyFilter().getNamespace()), e.getMessage());
        }

        // Try to create a storage policy using non-existing business object format file type.
        request = createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
            FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object format file type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("File type with code \"%s\" doesn't exist.", request.getStoragePolicyFilter().getBusinessObjectFormatFileType()),
                e.getMessage());
        }

        // Try to create a storage policy using non-existing storage name.
        request = createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, "I_DO_NOT_EXIST", STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an ObjectNotFoundException when using non-existing storage name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getStoragePolicyFilter().getStorageName()), e.getMessage());
        }

        // Try to create a storage policy using non-existing destination storage name.
        request = createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, "I_DO_NOT_EXIST", StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an ObjectNotFoundException when using non-existing destination storage name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getStoragePolicyTransition().getDestinationStorageName()),
                e.getMessage());
        }

        // Try to create a storage policy using non-existing storage policy status.
        request = createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
            FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, "I_DO_NOT_EXIST");
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an ObjectNotFoundException when using non-existing storage policy status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage policy status \"%s\" doesn't exist.", request.getStatus()), e.getMessage());
        }
    }

    @Test
    public void testCreateStoragePolicyStoragePolicyFilterStorageInvalidStoragePlatform()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to create a storage policy using storage of a non-S3 storage platform type.
        StoragePolicyCreateRequest request =
            createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME_2, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an IllegalArgumentException when using non-S3 storage platform for storage policy filter storage.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage platform for storage with name \"%s\" is not \"%s\".", request.getStoragePolicyFilter().getStorageName(),
                StoragePlatformEntity.S3), e.getMessage());
        }
    }

    @Test
    public void testCreateStoragePolicyStoragePolicyFilterStorageBucketNameNotConfigured()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create an S3 storage without any attributes.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_3, StoragePlatformEntity.S3);

        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to create a storage policy when storage policy filter storage has no S3 bucket name attribute configured.
        StoragePolicyCreateRequest request =
            createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME_3, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an IllegalStateException when storage policy filter storage has no S3 bucket name attribute configured.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), request.getStoragePolicyFilter().getStorageName()),
                e.getMessage());
        }
    }

    @Test
    public void testCreateStoragePolicyStoragePolicyFilterStoragePathPrefixValidationNotEnabled()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create an S3 storage with the bucket name configured, but without the S3 path prefix validation option configured.
        storageDaoTestHelper
            .createStorageEntity(STORAGE_NAME_3, StoragePlatformEntity.S3, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                S3_BUCKET_NAME);

        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to create a storage policy when storage policy filter storage has no S3 path prefix validation enabled.
        StoragePolicyCreateRequest request =
            createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME_3, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an IllegalStateException when storage policy filter storage has no S3 path prefix validation enabled.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Path prefix validation must be enabled on \"%s\" storage.", request.getStoragePolicyFilter().getStorageName()),
                e.getMessage());
        }
    }

    @Test
    public void testCreateStoragePolicyStoragePolicyFilterStorageFileExistenceValidationNotEnabled()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create an S3 storage with the bucket name configured, the S3 path prefix validation enabled, but without S3 file existence validation enabled.
        // Enable the S3 path prefix validation for this storage.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()));
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_3, StoragePlatformEntity.S3, attributes);

        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to create a storage policy when storage policy filter storage has no S3 file existence validation enabled.
        StoragePolicyCreateRequest request =
            createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME_3, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an IllegalStateException when storage policy filter storage has no S3 file existence validation enabled.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("File existence validation must be enabled on \"%s\" storage.", request.getStoragePolicyFilter().getStorageName()),
                e.getMessage());
        }
    }

    @Test
    public void testCreateStoragePolicyDestinationStorageInvalidStoragePlatform()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to create a storage policy using destination storage of a non-GLACIER storage platform type.
        StoragePolicyCreateRequest request =
            createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an IllegalArgumentException when using non-GLACIER storage platform for destination storage.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage platform for destination storage with name \"%s\" is not \"%s\".",
                request.getStoragePolicyTransition().getDestinationStorageName(), StoragePlatformEntity.GLACIER), e.getMessage());
        }
    }

    @Test
    public void testCreateStoragePolicyDestinationStorageBucketNameNotConfigured()
    {
        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create a Glacier storage without any attributes.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_3, StoragePlatformEntity.GLACIER);

        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to create a storage policy when destination storage has no Glacier vault name attribute configured.
        StoragePolicyCreateRequest request =
            createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_3, StoragePolicyStatusEntity.ENABLED);
        try
        {
            storagePolicyService.createStoragePolicy(request);
            fail("Should throw an IllegalStateException when destination storage has no Glacier vault name attribute configured.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                request.getStoragePolicyTransition().getDestinationStorageName()), e.getMessage());
        }
    }

    @Test
    public void testCreateStoragePolicyAlreadyExists()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Try to create a storage policy when it already exists.
        try
        {
            storagePolicyService.createStoragePolicy(
                createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                    FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an AlreadyExistsException when storage policy already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create storage policy with name \"%s\" because it already exists for namespace \"%s\".",
                storagePolicyKey.getStoragePolicyName(), storagePolicyKey.getNamespace()), e.getMessage());
        }
    }

    // Unit tests for updateStoragePolicy().

    @Test
    public void testUpdateStoragePolicy()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Update a storage policy.
        StoragePolicy resultStoragePolicy = storagePolicyService.updateStoragePolicy(storagePolicyKey,
            createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED));

        // Validate the returned object.
        assertEquals(
            new StoragePolicy(resultStoragePolicy.getId(), storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
                new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME),
                new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.DISABLED), resultStoragePolicy);
        assertTrue(resultStoragePolicy.getId() > storagePolicyEntity.getId());
    }

    @Test
    public void testUpdateStoragePolicyMissingRequiredParameters()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Try to update a storage policy when storage policy namespace is not specified.
        try
        {
            storagePolicyService.updateStoragePolicy(new StoragePolicyKey(BLANK_TEXT, STORAGE_POLICY_NAME),
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                    FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage policy namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to update a storage policy when storage policy name is not specified.
        try
        {
            storagePolicyService.updateStoragePolicy(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, BLANK_TEXT),
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, STORAGE_POLICY_NAMESPACE_CD, BDEF_NAME, FORMAT_USAGE_CODE,
                    FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage policy name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy name must be specified.", e.getMessage());
        }

        // Try to update a storage policy when storage policy rule type is not specified.
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey,
                createStoragePolicyUpdateRequest(BLANK_TEXT, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage policy rule type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy rule type must be specified.", e.getMessage());
        }

        // Try to update a storage policy when storage policy rule value is not specified.
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey,
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, null, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage policy rule value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy rule value must be specified.", e.getMessage());
        }

        // Try to update a storage policy when business object definition name is specified without business object definition namespace.
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey,
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BLANK_TEXT, BDEF_NAME, FORMAT_USAGE_CODE,
                    FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when business object definition name is specified without business object definition namespace.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition name and namespace must be specified together.", e.getMessage());
        }

        // Try to update a storage policy when business object definition namespace is specified without business object definition name.
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey,
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE,
                    FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when business object definition namespace is specified without business object definition name.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition name and namespace must be specified together.", e.getMessage());
        }

        // Try to update a storage policy when business object format file type is specified without business object format usage.
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey,
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT,
                    FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when business object format file type is specified without business object format usage.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format usage and file type must be specified together.", e.getMessage());
        }

        // Try to update a storage policy when business object format usage is specified without business object format file type.
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey,
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT,
                    STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when business object format usage is specified without business object format file type.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object format usage and file type must be specified together.", e.getMessage());
        }

        // Try to update a storage policy when storage name is not specified.
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey,
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                    FORMAT_FILE_TYPE_CODE, BLANK_TEXT, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when storage name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name must be specified.", e.getMessage());
        }

        // Try to update a storage policy when destination storage name is not specified.
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey,
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                    FORMAT_FILE_TYPE_CODE, STORAGE_NAME, BLANK_TEXT, StoragePolicyStatusEntity.ENABLED));
            fail("Should throw an IllegalArgumentException when destination storage name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A destination storage name must be specified.", e.getMessage());
        }

        // Try to update a storage policy when storage policy status is not specified.
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey,
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                    FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when storage policy status is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy status must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateStoragePolicyMissingOptionalParametersPassedAsWhitespace()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Update a storage policy without specifying any of the optional parameters (passing them as whitespace characters).
        StoragePolicy resultStoragePolicy = storagePolicyService.updateStoragePolicy(storagePolicyKey,
            createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BLANK_TEXT, BLANK_TEXT, BLANK_TEXT, BLANK_TEXT, STORAGE_NAME,
                STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED));

        // Validate the returned object.
        assertEquals(
            new StoragePolicy(resultStoragePolicy.getId(), storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
                new StoragePolicyFilter(null, null, null, null, STORAGE_NAME), new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.DISABLED),
            resultStoragePolicy);
        assertTrue(resultStoragePolicy.getId() > storagePolicyEntity.getId());
    }

    @Test
    public void testUpdateStoragePolicyMissingOptionalParametersPassedAsNulls()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Update a storage policy without specifying any of the optional parameters (passing them as null values).
        StoragePolicy resultStoragePolicy = storagePolicyService.updateStoragePolicy(storagePolicyKey,
            createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, null, null, null, null, STORAGE_NAME, STORAGE_NAME_2,
                StoragePolicyStatusEntity.DISABLED));

        // Validate the returned object.
        assertEquals(
            new StoragePolicy(resultStoragePolicy.getId(), storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
                new StoragePolicyFilter(null, null, null, null, STORAGE_NAME), new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.DISABLED),
            resultStoragePolicy);
        assertTrue(resultStoragePolicy.getId() > storagePolicyEntity.getId());
    }

    @Test
    public void testUpdateStoragePolicyTrimParameters()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Update a storage policy using input parameters with leading and trailing empty spaces.
        StoragePolicy resultStoragePolicy = storagePolicyService
            .updateStoragePolicy(new StoragePolicyKey(addWhitespace(STORAGE_POLICY_NAMESPACE_CD), addWhitespace(STORAGE_POLICY_NAME)),
                createStoragePolicyUpdateRequest(addWhitespace(STORAGE_POLICY_RULE_TYPE), STORAGE_POLICY_RULE_VALUE, addWhitespace(BDEF_NAMESPACE),
                    addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE), addWhitespace(FORMAT_FILE_TYPE_CODE), addWhitespace(STORAGE_NAME),
                    addWhitespace(STORAGE_NAME_2), addWhitespace(StoragePolicyStatusEntity.DISABLED)));

        // Validate the returned object.
        assertEquals(new StoragePolicy(resultStoragePolicy.getId(), new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
            new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME),
            new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.DISABLED), resultStoragePolicy);
        assertTrue(resultStoragePolicy.getId() > storagePolicyEntity.getId());
    }

    @Test
    public void testUpdateStoragePolicyUpperCaseParameters()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Update a storage policy using upper case input parameters.
        StoragePolicy resultStoragePolicy = storagePolicyService
            .updateStoragePolicy(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toUpperCase(), STORAGE_POLICY_NAME.toUpperCase()),
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE.toUpperCase(), STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE.toUpperCase(),
                    BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(), STORAGE_NAME.toUpperCase(),
                    STORAGE_NAME_2.toUpperCase(), StoragePolicyStatusEntity.DISABLED.toUpperCase()));

        // Validate the returned object.
        assertEquals(new StoragePolicy(resultStoragePolicy.getId(), new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
            new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE, STORAGE_NAME),
            new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.DISABLED), resultStoragePolicy);
        assertTrue(resultStoragePolicy.getId() > storagePolicyEntity.getId());
    }

    @Test
    public void testUpdateStoragePolicyLowerCaseParameters()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Update a storage policy using lower case input parameters.
        StoragePolicy resultStoragePolicy = storagePolicyService
            .updateStoragePolicy(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toLowerCase(), STORAGE_POLICY_NAME.toLowerCase()),
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE.toLowerCase(), STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE.toLowerCase(),
                    BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(), STORAGE_NAME.toLowerCase(),
                    STORAGE_NAME_2.toLowerCase(), StoragePolicyStatusEntity.DISABLED.toLowerCase()));

        // Validate the returned object.
        assertEquals(new StoragePolicy(resultStoragePolicy.getId(), new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME),
            new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
            new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE, STORAGE_NAME),
            new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.DISABLED), resultStoragePolicy);
        assertTrue(resultStoragePolicy.getId() > storagePolicyEntity.getId());
    }

    @Test
    public void testUpdateStoragePolicyInvalidParameters()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        StoragePolicyUpdateRequest request;

        // Try to update a storage policy using non-existing storage policy rule type.
        request =
            createStoragePolicyUpdateRequest("I_DO_NOT_EXIST", STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED);
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing storage policy rule type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage policy rule type with code \"%s\" doesn't exist.", request.getStoragePolicyRule().getRuleType()),
                e.getMessage());
        }

        // Try to update a storage policy using a negative storage policy rule value.
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey,
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, -1, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED));
            fail("Should throw an IllegalArgumentException when using a negative storage policy rule value.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Storage policy rule value must be a positive integer or zero.", e.getMessage());
        }

        // Try to update a storage policy using non-existing business object definition namespace.
        request = createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, "I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED);
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object definition namespace.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".",
                request.getStoragePolicyFilter().getBusinessObjectDefinitionName(), request.getStoragePolicyFilter().getNamespace()), e.getMessage());
        }

        // Try to update a storage policy using non-existing business object definition name.
        request = createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED);
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object definition name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".",
                request.getStoragePolicyFilter().getBusinessObjectDefinitionName(), request.getStoragePolicyFilter().getNamespace()), e.getMessage());
        }

        // Try to update a storage policy using non-existing business object format file type.
        request = createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
            "I_DO_NOT_EXIST", STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED);
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing business object format file type.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("File type with code \"%s\" doesn't exist.", request.getStoragePolicyFilter().getBusinessObjectFormatFileType()),
                e.getMessage());
        }

        // Try to update a storage policy using non-existing storage name.
        request = createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, "I_DO_NOT_EXIST", STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED);
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing storage name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getStoragePolicyFilter().getStorageName()), e.getMessage());
        }

        // Try to update a storage policy using non-existing destination storage name.
        request = createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, "I_DO_NOT_EXIST", StoragePolicyStatusEntity.DISABLED);
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing destination storage name.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", request.getStoragePolicyTransition().getDestinationStorageName()),
                e.getMessage());
        }

        // Try to update a storage policy using non-existing storage policy status.
        request = createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
            FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, "I_DO_NOT_EXIST");
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an ObjectNotFoundException when using non-existing storage policy status.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage policy status \"%s\" doesn't exist.", request.getStatus()), e.getMessage());
        }
    }

    @Test
    public void testUpdateStoragePolicyStoragePolicyFilterStorageInvalidStoragePlatform()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Try to update a storage policy using storage of a non-S3 storage platform type.
        StoragePolicyUpdateRequest request =
            createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME_2, STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED);
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an IllegalArgumentException when using non-S3 storage platform for storage policy filter storage.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage platform for storage with name \"%s\" is not \"%s\".", request.getStoragePolicyFilter().getStorageName(),
                StoragePlatformEntity.S3), e.getMessage());
        }
    }

    @Test
    public void testUpdateStoragePolicyStoragePolicyFilterStorageBucketNameNotConfigured()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Create an S3 storage without any attributes.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_5, StoragePlatformEntity.S3);

        // Try to update a storage policy when storage policy filter storage has no S3 bucket name attribute configured.
        StoragePolicyUpdateRequest request =
            createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME_5, STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED);
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an IllegalStateException when storage policy filter storage has no S3 bucket name attribute configured.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), request.getStoragePolicyFilter().getStorageName()),
                e.getMessage());
        }
    }

    @Test
    public void testUpdateStoragePolicyStoragePolicyFilterStoragePathPrefixValidationNotEnabled()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Create an S3 storage with the bucket name configured, but without the S3 path prefix validation option configured.
        storageDaoTestHelper
            .createStorageEntity(STORAGE_NAME_5, StoragePlatformEntity.S3, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                S3_BUCKET_NAME);

        // Try to update a storage policy when storage policy filter storage has no S3 path prefix validation enabled.
        StoragePolicyUpdateRequest request =
            createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME_5, STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED);
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an IllegalStateException when storage policy filter storage has no S3 path prefix validation enabled.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Path prefix validation must be enabled on \"%s\" storage.", request.getStoragePolicyFilter().getStorageName()),
                e.getMessage());
        }
    }

    @Test
    public void testUpdateStoragePolicyStoragePolicyFilterStorageFileExistenceValidationNotEnabled()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);


        // Create an S3 storage with the bucket name configured, the S3 path prefix validation enabled, but without S3 file existence validation enabled.
        // Enable the S3 path prefix validation for this storage.
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), S3_BUCKET_NAME));
        attributes.add(new Attribute(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX), Boolean.TRUE.toString()));
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_5, StoragePlatformEntity.S3, attributes);

        // Try to update a storage policy when storage policy filter storage has no S3 file existence validation enabled.
        StoragePolicyUpdateRequest request =
            createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME_5, STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED);
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an IllegalStateException when storage policy filter storage has no S3 file existence validation enabled.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("File existence validation must be enabled on \"%s\" storage.", request.getStoragePolicyFilter().getStorageName()),
                e.getMessage());
        }
    }

    @Test
    public void testUpdateStoragePolicyDestinationStorageInvalidStoragePlatform()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Try to update a storage policy using destination storage of a non-GLACIER storage platform type.
        StoragePolicyUpdateRequest request =
            createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME, StoragePolicyStatusEntity.DISABLED);
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an IllegalArgumentException when using non-GLACIER storage platform for destination storage.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Storage platform for destination storage with name \"%s\" is not \"%s\".",
                request.getStoragePolicyTransition().getDestinationStorageName(), StoragePlatformEntity.GLACIER), e.getMessage());
        }
    }

    @Test
    public void testUpdateStoragePolicyDestinationStorageVaultNameNotConfigured()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist the relative database entities.
        createDatabaseEntitiesForStoragePolicyTesting();

        // Create and persist a storage policy entity.
        storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE_2, STORAGE_POLICY_RULE_VALUE_2, BDEF_NAMESPACE_2, BDEF_NAME_2,
                FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, STORAGE_NAME_3, STORAGE_NAME_4, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION,
                LATEST_VERSION_FLAG_SET);

        // Create a Glacier storage without any attributes.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_5, StoragePlatformEntity.GLACIER);

        // Try to update a storage policy when destination storage has no Glacier vault name attribute configured.
        StoragePolicyUpdateRequest request =
            createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_5, StoragePolicyStatusEntity.DISABLED);
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey, request);
            fail("Should throw an IllegalStateException when destination storage has no Glacier vault name attribute configured.");
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Attribute \"%s\" for \"%s\" storage must be configured.",
                configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                request.getStoragePolicyTransition().getDestinationStorageName()), e.getMessage());
        }
    }

    @Test
    public void testUpdateStoragePolicyNoExists()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Try to update a storage policy when it already exists.
        try
        {
            storagePolicyService.updateStoragePolicy(storagePolicyKey,
                createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                    FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.DISABLED));
            fail("Should throw an ObjectNotFoundException when trying to retrieve a non-existing storage policy.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("Storage policy with name \"%s\" does not exist for \"%s\" namespace.", STORAGE_POLICY_NAME, STORAGE_POLICY_NAMESPACE_CD),
                e.getMessage());
        }
    }

    // Unit tests for getStoragePolicy().

    @Test
    public void testGetStoragePolicy()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Retrieve the storage policy.
        StoragePolicy resultStoragePolicy = storagePolicyService.getStoragePolicy(storagePolicyKey);

        // Validate the returned object.
        assertEquals(
            new StoragePolicy(storagePolicyEntity.getId(), storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
                new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME),
                new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.ENABLED), resultStoragePolicy);
    }

    @Test
    public void testGetStoragePolicyMissingRequiredParameters()
    {
        // Try to get a storage policy when namespace is not specified.
        try
        {
            storagePolicyService.getStoragePolicy(new StoragePolicyKey(BLANK_TEXT, STORAGE_POLICY_NAME));
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to get a storage policy when storage policy name is not specified.
        try
        {
            storagePolicyService.getStoragePolicy(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when storage policy name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage policy name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetStoragePolicyTrimParameters()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Retrieve the storage policy using input parameters with leading and trailing empty spaces.
        StoragePolicy resultStoragePolicy =
            storagePolicyService.getStoragePolicy(new StoragePolicyKey(addWhitespace(STORAGE_POLICY_NAMESPACE_CD), addWhitespace(STORAGE_POLICY_NAME)));

        // Validate the returned object.
        assertEquals(
            new StoragePolicy(storagePolicyEntity.getId(), storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
                new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME),
                new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.ENABLED), resultStoragePolicy);
    }

    @Test
    public void testGetStoragePolicyUpperCaseParameters()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Retrieve the storage policy using upper case input parameters.
        StoragePolicy resultStoragePolicy =
            storagePolicyService.getStoragePolicy(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toUpperCase(), STORAGE_POLICY_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(
            new StoragePolicy(storagePolicyEntity.getId(), storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
                new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME),
                new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.ENABLED), resultStoragePolicy);
    }

    @Test
    public void testGetStoragePolicyLowerCaseParameters()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        // Create and persist a storage policy entity.
        StoragePolicyEntity storagePolicyEntity = storagePolicyDaoTestHelper
            .createStoragePolicyEntity(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, STORAGE_NAME_2, StoragePolicyStatusEntity.ENABLED, INITIAL_VERSION, LATEST_VERSION_FLAG_SET);

        // Retrieve the storage policy using lower case input parameters.
        StoragePolicy resultStoragePolicy =
            storagePolicyService.getStoragePolicy(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD.toLowerCase(), STORAGE_POLICY_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(
            new StoragePolicy(storagePolicyEntity.getId(), storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
                new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME),
                new StoragePolicyTransition(STORAGE_NAME_2), StoragePolicyStatusEntity.ENABLED), resultStoragePolicy);
    }

    @Test
    public void testGetStoragePolicyNoExists()
    {
        // Try to retrieve a non-existing storage policy.
        try
        {
            storagePolicyService.getStoragePolicy(new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME));
            fail("Should throw an ObjectNotFoundException when trying to retrieve a non-existing storage policy.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("Storage policy with name \"%s\" does not exist for \"%s\" namespace.", STORAGE_POLICY_NAME, STORAGE_POLICY_NAMESPACE_CD),
                e.getMessage());
        }
    }
}
