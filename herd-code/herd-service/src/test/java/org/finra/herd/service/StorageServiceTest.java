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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.persistence.PersistenceException;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.junit.Ignore;
import org.junit.Test;

import org.finra.herd.core.Command;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageCreateRequest;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.api.xml.StorageKeys;
import org.finra.herd.model.api.xml.StorageUpdateRequest;
import org.finra.herd.model.dto.StorageAlternateKeyDto;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;

/**
 * This class tests various functionality within the storage REST controller.
 */
public class StorageServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateStorage() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();

        Storage storage = storageService.createStorage(storageCreateRequest);

        assertNotNull(storage);
        assertTrue(storage.getName().equals(storageCreateRequest.getName()));

        // Check if result list of attributes matches to the list from the create request.
        validateAttributes(storageCreateRequest.getAttributes(), storage.getAttributes());
    }

    @Test
    public void testCreateStorageMissingOptionalParameters() throws Exception
    {
        // Create and persist a valid storage without specifying optional parameters.
        StorageCreateRequest request = getNewStorageCreateRequest();
        request.setAttributes(null);
        Storage storage = storageService.createStorage(request);

        // Validate the returned object.
        assertNotNull(storage);
        assertTrue(storage.getName().equals(request.getName()));
        assertTrue(storage.getAttributes().isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateStorageNoName() throws Exception
    {
        // Leave the storage name blank which is invalid.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        storageCreateRequest.setName(null);
        storageService.createStorage(storageCreateRequest);
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

    @Test(expected = AlreadyExistsException.class)
    public void testCreateStorageAlreadyExists() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        storageService.createStorage(storageCreateRequest);

        // Try creating it again which is invalid since it already exists.
        storageService.createStorage(storageCreateRequest);
    }

    @Test
    public void testUpdateStorage() throws Exception
    {
        // Create a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        storageService.createStorage(storageCreateRequest);

        // Update the storage platform which is valid.
        StorageUpdateRequest storageUpdateRequest = new StorageUpdateRequest();

        // TODO: Update various attributes of the storage update request in the future when there is something to update.

        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storageCreateRequest.getName()).build();
        storageService.updateStorage(alternateKey, storageUpdateRequest);

        // TODO: Add asserts to ensure fields that were update indeed got updated.
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testUpdateStorageNoExists() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();

        // Try updating a storage that doesn't yet exist which is invalid.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storageCreateRequest.getName()).build();
        storageService.updateStorage(alternateKey, new StorageUpdateRequest());
    }

    @Test
    public void testGetStorage() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        String name = storageCreateRequest.getName();
        Storage storage = storageService.createStorage(storageCreateRequest);

        // Retrieve the storage by it's name which is valid.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storage.getName()).build();
        storage = storageService.getStorage(alternateKey);
        assertNotNull(storage);
        assertTrue(storage.getName().equals(name));
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testGetStorageInvalidName() throws Exception
    {
        // Try getting a storage that doesn't exist which is invalid.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName("invalid" + getRandomSuffix()).build();
        storageService.getStorage(alternateKey);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testDeleteStorage() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        String name = storageCreateRequest.getName();
        Storage storage = storageService.createStorage(storageCreateRequest);

        // Delete the storage by it's name which is valid.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storage.getName()).build();
        storage = storageService.deleteStorage(alternateKey);
        assertNotNull(storage);
        assertTrue(storage.getName().equals(name));

        // Retrieve the storage by it's name and verify that it doesn't exist.
        storageService.getStorage(alternateKey);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testDeleteStorageInvalidName() throws Exception
    {
        // Delete a storage which doesn't exist.
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(getNewStorageCreateRequest().getName()).build();
        storageService.deleteStorage(alternateKey);
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
                StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storageUnitEntity.getStorage().getName()).build();
                storageService.deleteStorage(alternateKey);
            }
        });
    }

    @Test
    public void testGetStorages() throws Exception
    {
        // Create and persist test storage entities.
        for (StorageKey key : storageDaoTestHelper.getTestStorageKeys())
        {
            storageDaoTestHelper.createStorageEntity(key.getStorageName());
        }

        // Retrieve a list of storage keys.
        StorageKeys resultStorageKeys = storageService.getStorages();

        // Validate the returned object.
        assertNotNull(resultStorageKeys);
        assertTrue(resultStorageKeys.getStorageKeys().containsAll(storageDaoTestHelper.getTestStorageKeys()));
    }

    /**
     * Creates (but does not persist) a new valid storage create request.
     *
     * @return a new storage.
     */
    private StorageCreateRequest getNewStorageCreateRequest()
    {
        String name = "StorageTest" + getRandomSuffix();
        StorageCreateRequest storageRequest = new StorageCreateRequest();
        storageRequest.setStoragePlatformName(StoragePlatformEntity.S3);
        storageRequest.setName(name);
        storageRequest.setAttributes(getNewAttributes());
        return storageRequest;
    }

}
