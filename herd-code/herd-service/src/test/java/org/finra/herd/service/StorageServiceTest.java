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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import javax.persistence.PersistenceException;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.junit.Ignore;
import org.junit.Test;

import org.finra.herd.core.Command;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageAttributesUpdateRequest;
import org.finra.herd.model.api.xml.StorageCreateRequest;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.api.xml.StorageKeys;
import org.finra.herd.model.api.xml.StorageUpdateRequest;
import org.finra.herd.model.jpa.StorageAttributeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;

public class StorageServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateStorage()
    {
        // Create and persist a valid storage.
        StorageCreateRequest request = getNewStorageCreateRequest();
        Storage result = storageService.createStorage(request);

        // Validate the results.
        assertEquals(new Storage(request.getName(), request.getStoragePlatformName(), request.getAttributes()), result);
    }

    @Test
    public void testCreateStorageInvalidParameters()
    {
        // Try to create a storage instance when storage platform name contains a forward slash character.
        try
        {
            storageService.createStorage(new StorageCreateRequest(STORAGE_NAME, addSlash(STORAGE_PLATFORM_CODE), NO_ATTRIBUTES));
            fail("Should throw an IllegalArgumentException when storage platform name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Storage platform name can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a storage instance when storage name contains a forward slash character.
        try
        {
            storageService.createStorage(new StorageCreateRequest(addSlash(DATA_PROVIDER_NAME), STORAGE_PLATFORM_CODE, NO_ATTRIBUTES));
            fail("Should throw an IllegalArgumentException when storage name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Storage name can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateStorageMissingOptionalParameters()
    {
        // Create and persist a valid storage without specifying optional parameters.
        StorageCreateRequest request = getNewStorageCreateRequest();
        request.setAttributes(null);
        Storage result = storageService.createStorage(request);

        // Validate the results.
        assertEquals(new Storage(request.getName(), request.getStoragePlatformName(), NO_ATTRIBUTES), result);
    }

    @Test
    public void testCreateStorageMissingRequiredParameters()
    {
        // Try to create a storage without specifying a storage name.
        try
        {
            storageService.createStorage(new StorageCreateRequest(BLANK_TEXT, STORAGE_PLATFORM_CODE, NO_ATTRIBUTES));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateStorageStorageAlreadyExists()
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        storageService.createStorage(storageCreateRequest);

        // Try creating that storage again which is invalid since it already exists.
        try
        {
            storageService.createStorage(storageCreateRequest);
            fail();
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Storage with name \"%s\" already exists.", storageCreateRequest.getName()), e.getMessage());
        }

    }

    @Test
    public void testDeleteStorage()
    {
        // Create and persist a valid storage.
        Storage storage = storageService.createStorage(getNewStorageCreateRequest());

        // Delete the storage.
        Storage result = storageService.deleteStorage(new StorageKey(storage.getName()));
        assertEquals(storage, result);

        // Retrieve the storage by its name and verify that it doesn't exist.
        assertNull(storageDao.getStorageByName(storage.getName()));
    }

    /*
     * This test is ignored because the constraint validation is a DB dependent feature. This method had inconsistent behavior between Oracle and PostgreSQL.
     * Oracle was throwing the error after each statement, whereas PostgreSQL would not because it by default raises error only when transaction is committed.
     *
     * Besides, this test case is not valid as a use case as normal transactions wouldn't delete after insert within same transaction.
     */
    @Ignore
    @Test(expected = PersistenceException.class)
    public void testDeleteStorageConstraintViolation() throws Exception
    {
        // Create a storage unit entity that refers to a newly created storage.
        final StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
            .createStorageUnitEntity(STORAGE_NAME, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BDATA_STATUS, STORAGE_UNIT_STATUS, NO_STORAGE_DIRECTORY_PATH);

        executeWithoutLogging(SqlExceptionHelper.class, new Command()
        {
            @Override
            public void execute()
            {
                // Delete the storage which is invalid because there still exists a storage unit entity that references it.
                StorageKey alternateKey = new StorageKey(storageUnitEntity.getStorage().getName());
                storageService.deleteStorage(alternateKey);
            }
        });
    }

    @Test
    public void testDeleteStorageInvalidName()
    {
        // Try to delete a storage which doesn't exist.
        try
        {
            storageService.deleteStorage(new StorageKey(INVALID_VALUE));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", INVALID_VALUE), e.getMessage());
        }
    }

    @Test
    public void testGetAllStorage()
    {
        // Get a list of test storage keys.
        List<StorageKey> storageKeys = storageDaoTestHelper.getTestStorageKeys();

        // Create and persist test storage entities.
        for (StorageKey storageKey : storageKeys)
        {
            storageDaoTestHelper.createStorageEntity(storageKey.getStorageName());
        }

        // Retrieve a list of storage keys.
        StorageKeys result = storageService.getAllStorage();

        // Validate the results.
        assertNotNull(result);
        assertTrue(result.getStorageKeys().containsAll(storageKeys));
    }

    @Test
    public void testGetStorage()
    {
        // Create and persist a valid storage.
        Storage storage = storageService.createStorage(getNewStorageCreateRequest());

        // Retrieve the storage by its name.
        Storage result = storageService.getStorage(new StorageKey(storage.getName()));

        // Validate the results.
        assertEquals(storage, result);
    }

    @Test
    public void testGetStorageInvalidStorageName()
    {
        // Try getting a storage that doesn't exist.
        try
        {
            storageService.getStorage(new StorageKey(INVALID_VALUE));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", INVALID_VALUE), e.getMessage());
        }
    }

    @Test
    public void testUpdateStorage()
    {
        // Create a storage.
        Storage storage = storageService.createStorage(getNewStorageCreateRequest());

        // Update the storage.
        // TODO: Update various attributes of the storage update request in the future when there is something to update.
        Storage result = storageService.updateStorage(new StorageKey(storage.getName()), new StorageUpdateRequest());

        // Validate the results.
        // TODO: Add asserts to ensure fields that were update indeed got updated.
        assertEquals(new Storage(storage.getName(), storage.getStoragePlatformName(), storage.getAttributes()), result);
    }

    @Test
    public void testUpdateStorageAttributes()
    {

        // Create and persist a valid storage.
        StorageCreateRequest request = getNewStorageCreateRequest();
        Storage storage = storageService.createStorage(request);

        // Update attributes for the storage.
        Storage result = storageService.updateStorageAttributes(new StorageKey(storage.getName()),
            new StorageAttributesUpdateRequest(businessObjectDefinitionServiceTestHelper.getNewAttributes2()));

        // Validate the results.
        assertEquals(new Storage(storage.getName(), storage.getStoragePlatformName(), businessObjectDefinitionServiceTestHelper.getNewAttributes2()), result);
    }

    @Test
    public void testUpdateStorageAttributesRemoveAllAttributes()
    {
        // Create and persist a valid storage.
        StorageCreateRequest request = getNewStorageCreateRequest();
        Storage storage = storageService.createStorage(request);

        // Update attributes for the storage.
        Storage result = storageService.updateStorageAttributes(new StorageKey(storage.getName()), new StorageAttributesUpdateRequest(NO_ATTRIBUTES));

        // Validate the results.
        assertEquals(new Storage(storage.getName(), storage.getStoragePlatformName(), NO_ATTRIBUTES), result);
    }

    @Test
    public void testUpdateStorageAttributesStorageHasDuplicateAttributes()
    {
        // Create and persist a valid storage.
        StorageCreateRequest request = getNewStorageCreateRequest();
        Storage storage = storageService.createStorage(request);

        // Add a duplicate attribute to the storage.
        StorageEntity storageEntity = storageDao.getStorageByName(storage.getName());
        StorageAttributeEntity storageAttributeEntity = new StorageAttributeEntity();
        storageAttributeEntity.setStorage(storageEntity);
        storageAttributeEntity.setName(request.getAttributes().get(0).getName().toUpperCase());
        storageEntity.getAttributes().add(storageAttributeEntity);
        storageDao.saveAndRefresh(storageEntity);

        // Try to update attributes for the storage.
        try
        {
            storageService.updateStorageAttributes(new StorageKey(storage.getName()),
                new StorageAttributesUpdateRequest(businessObjectDefinitionServiceTestHelper.getNewAttributes2()));
        }
        catch (IllegalStateException e)
        {
            assertEquals(String.format("Found duplicate attribute with name \"%s\" for \"%s\" storage.", request.getAttributes().get(0).getName().toLowerCase(),
                storage.getName()), e.getMessage());
        }
    }

    @Test
    public void testUpdateStorageAttributesStorageNoExists()
    {
        // Try to update attributes for a storage that doesn't yet exist.
        try
        {
            storageService.updateStorageAttributes(new StorageKey(INVALID_VALUE),
                new StorageAttributesUpdateRequest(businessObjectDefinitionServiceTestHelper.getNewAttributes()));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", INVALID_VALUE), e.getMessage());
        }
    }

    @Test
    public void testUpdateStorageStorageNoExists()
    {
        // Try to update a storage that doesn't yet exist.
        try
        {
            storageService.updateStorage(new StorageKey(INVALID_VALUE), new StorageUpdateRequest());
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Storage with name \"%s\" doesn't exist.", INVALID_VALUE), e.getMessage());
        }
    }

    /**
     * Creates (but does not persist) a new valid storage create request.
     *
     * @return a new storage create request
     */
    private StorageCreateRequest getNewStorageCreateRequest()
    {
        return new StorageCreateRequest(STORAGE_NAME, StoragePlatformEntity.S3, businessObjectDefinitionServiceTestHelper.getNewAttributes());
    }
}
